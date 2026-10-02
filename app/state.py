"""Local SQLite state: watermark, per-object results, failures, run and reconcile history.

Only the main thread touches this object.
"""
from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

SCHEMA = """
CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v TEXT NOT NULL);

-- One row per object that is confirmed to be in the backup bucket.
CREATE TABLE IF NOT EXISTS results (
    key          TEXT PRIMARY KEY,
    doc_url      TEXT,
    created      TEXT,               -- createdDate from MSSQL
    size         INTEGER NOT NULL,   -- object size in bytes
    action       TEXT NOT NULL,      -- copied | skipped (already in MinIO)
    source_etag  TEXT,
    backed_up_at TEXT NOT NULL,
    synced       INTEGER NOT NULL DEFAULT 0);  -- published to Postgres yet?
CREATE INDEX IF NOT EXISTS ix_results_at ON results(backed_up_at);
CREATE INDEX IF NOT EXISTS ix_results_created ON results(created);
CREATE INDEX IF NOT EXISTS ix_results_unsynced ON results(synced) WHERE synced = 0;

CREATE TABLE IF NOT EXISTS failures (
    doc_url TEXT PRIMARY KEY,
    created TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL,          -- pending | dead | source_missing | invalid_url
    last_error TEXT,
    updated_at TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS ix_fail_status ON failures(status);

CREATE TABLE IF NOT EXISTS runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at TEXT, finished_at TEXT, window_start TEXT, window_end TEXT,
    rows INTEGER, copied INTEGER, skipped INTEGER, failed INTEGER,
    source_missing INTEGER, invalid INTEGER, deferred INTEGER, bytes INTEGER);

CREATE TABLE IF NOT EXISTS reconciles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_at TEXT NOT NULL,
    day TEXT NOT NULL,             -- the createdDate day that was checked
    db_rows INTEGER, objects INTEGER, backed_up INTEGER, bytes INTEGER,
    requeued INTEGER,              -- had no backup and no failure record: queued now
    missing_in_dest INTEGER,       -- recorded as backed up but not found in MinIO: queued now
    size_mismatch INTEGER,         -- MinIO size differs from the recorded size
    pending INTEGER, dead INTEGER, source_missing INTEGER, invalid_url INTEGER,
    verified INTEGER);
CREATE INDEX IF NOT EXISTS ix_reconciles_day ON reconciles(day);
"""

RUNS_EXTRA_COLUMNS = {"backfill_objects": "INTEGER", "backfill_bytes": "INTEGER"}

PENDING, DEAD, SOURCE_MISSING, INVALID_URL = "pending", "dead", "source_missing", "invalid_url"


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _chunks(items: Sequence, n: int = 500) -> Iterable[Sequence]:
    for i in range(0, len(items), n):
        yield items[i:i + n]


class State:
    def __init__(self, path: str):
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        self.db = sqlite3.connect(path, isolation_level=None, timeout=30)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.executescript(SCHEMA)
        self._migrate()

    def _migrate(self) -> None:
        have = {r[1] for r in self.db.execute("PRAGMA table_info(runs)")}
        for col, typ in RUNS_EXTRA_COLUMNS.items():
            if col not in have:
                self.db.execute(f"ALTER TABLE runs ADD COLUMN {col} {typ}")
        # v1 kept a short-lived 'done' table; carry it over into results.
        if self.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='done'").fetchone():
            self.db.execute(
                "INSERT OR IGNORE INTO results(key,size,action,backed_up_at) "
                "SELECT key, COALESCE(size,0), 'copied', done_at FROM done")
            self.db.execute("DROP TABLE done")

    # --- key/value ---
    def kv_get(self, k: str) -> Optional[str]:
        row = self.db.execute("SELECT v FROM kv WHERE k=?", (k,)).fetchone()
        return row[0] if row else None

    def kv_set(self, k: str, v: str) -> None:
        self.db.execute("INSERT INTO kv(k,v) VALUES(?,?) "
                        "ON CONFLICT(k) DO UPDATE SET v=excluded.v", (k, v))

    def get_watermark(self) -> Optional[datetime]:
        v = self.kv_get("watermark")
        return datetime.fromisoformat(v) if v else None

    def set_watermark(self, wm: datetime) -> None:
        self.kv_set("watermark", wm.isoformat())

    # --- results ---
    def is_done(self, key: str) -> bool:
        return self.db.execute("SELECT 1 FROM results WHERE key=?", (key,)).fetchone() is not None

    def mark_done(self, key: str, size: int, doc_url: Optional[str] = None,
                  created: Optional[datetime] = None, action: str = "copied",
                  source_etag: Optional[str] = None) -> None:
        self.db.execute(
            "INSERT OR REPLACE INTO results(key,doc_url,created,size,action,source_etag,"
            "backed_up_at,synced) VALUES(?,?,?,?,?,?,?,0)",
            (key, doc_url, created.isoformat() if created else None, size, action,
             source_etag, _now()))
        if doc_url:
            self.db.execute("DELETE FROM failures WHERE doc_url=?", (doc_url,))

    def sizes_for(self, keys: Sequence[str]) -> Dict[str, int]:
        out: Dict[str, int] = {}
        for chunk in _chunks(list(keys)):
            q = ",".join("?" * len(chunk))
            out.update(self.db.execute(
                f"SELECT key, size FROM results WHERE key IN ({q})", tuple(chunk)).fetchall())
        return out

    def delete_result(self, key: str) -> None:
        self.db.execute("DELETE FROM results WHERE key=?", (key,))

    def unsynced(self, limit: int) -> List[dict]:
        cur = self.db.execute(
            "SELECT key, doc_url, created, size, action, source_etag, backed_up_at "
            "FROM results WHERE synced=0 LIMIT ?", (limit,))
        names = [d[0] for d in cur.description]
        return [dict(zip(names, row)) for row in cur.fetchall()]

    def mark_synced(self, keys: Sequence[str]) -> None:
        for chunk in _chunks(list(keys)):
            q = ",".join("?" * len(chunk))
            self.db.execute(f"UPDATE results SET synced=1 WHERE key IN ({q})", tuple(chunk))

    def prune_results(self, days: int, only_synced: bool) -> None:
        if days <= 0:
            return
        cutoff = (datetime.now() - timedelta(days=days)).isoformat(timespec="seconds")
        extra = " AND synced = 1" if only_synced else ""
        self.db.execute(f"DELETE FROM results WHERE backed_up_at < ?{extra}", (cutoff,))

    def result_totals(self) -> dict:
        n, b, u = self.db.execute(
            "SELECT COUNT(*), COALESCE(SUM(size),0), COALESCE(SUM(synced = 0),0) FROM results"
        ).fetchone()
        return {"objects": n, "bytes": b, "not_yet_published": u}

    # --- failures ---
    def record_failure(self, doc_url: str, created: Optional[datetime], status: str,
                       error: str, max_attempts: int, count_attempt: bool = True) -> None:
        row = self.db.execute("SELECT attempts FROM failures WHERE doc_url=?", (doc_url,)).fetchone()
        attempts = (row[0] if row else 0) + (1 if count_attempt else 0)
        if status == PENDING and attempts >= max_attempts:
            status = DEAD
        self.db.execute(
            "INSERT INTO failures(doc_url,created,attempts,status,last_error,updated_at) "
            "VALUES(?,?,?,?,?,?) ON CONFLICT(doc_url) DO UPDATE SET "
            "attempts=excluded.attempts, status=excluded.status, "
            "last_error=excluded.last_error, updated_at=excluded.updated_at",
            (doc_url, created.isoformat() if created else None, attempts, status, error[:500], _now()))

    def pending_failures(self, limit: int) -> List[Tuple[str, Optional[datetime]]]:
        rows = self.db.execute(
            "SELECT doc_url, created FROM failures WHERE status=? ORDER BY updated_at LIMIT ?",
            (PENDING, limit)).fetchall()
        return [(u, datetime.fromisoformat(c) if c else None) for u, c in rows]

    def failure_status_for(self, doc_urls: Sequence[str]) -> Dict[str, str]:
        out: Dict[str, str] = {}
        for chunk in _chunks(list(doc_urls)):
            q = ",".join("?" * len(chunk))
            out.update(self.db.execute(
                f"SELECT doc_url, status FROM failures WHERE doc_url IN ({q})", tuple(chunk)).fetchall())
        return out

    def requeue(self, statuses: Tuple[str, ...]) -> int:
        q = ",".join("?" * len(statuses))
        cur = self.db.execute(
            f"UPDATE failures SET status=?, attempts=0, updated_at=? WHERE status IN ({q})",
            (PENDING, _now(), *statuses))
        return cur.rowcount

    def all_failures(self) -> List[dict]:
        cur = self.db.execute(
            "SELECT doc_url, created, attempts, status, last_error, updated_at FROM failures")
        names = [d[0] for d in cur.description]
        return [dict(zip(names, row)) for row in cur.fetchall()]

    def failure_counts(self) -> dict:
        return dict(self.db.execute("SELECT status, COUNT(*) FROM failures GROUP BY status").fetchall())

    # --- runs / reconciles ---
    def _insert(self, table: str, r: dict) -> None:
        cols = ",".join(r)
        self.db.execute(f"INSERT INTO {table}({cols}) VALUES({','.join('?' * len(r))})",
                        tuple(r.values()))

    def _last(self, table: str, n: int) -> List[dict]:
        cur = self.db.execute(f"SELECT * FROM {table} ORDER BY id DESC LIMIT ?", (n,))
        names = [d[0] for d in cur.description]
        return [dict(zip(names, row)) for row in cur.fetchall()]

    def rows_after(self, table: str, last_id: int) -> List[dict]:
        """Rows of 'runs' or 'reconciles' with id > last_id, oldest first."""
        assert table in ("runs", "reconciles")
        cur = self.db.execute(f"SELECT * FROM {table} WHERE id > ? ORDER BY id", (last_id,))
        names = [d[0] for d in cur.description]
        return [dict(zip(names, row)) for row in cur.fetchall()]

    def record_run(self, **r) -> None:
        self._insert("runs", r)

    def last_runs(self, n: int = 10) -> List[dict]:
        return self._last("runs", n)

    def first_run_started(self) -> Optional[datetime]:
        row = self.db.execute("SELECT MIN(started_at) FROM runs").fetchone()
        return datetime.fromisoformat(row[0]) if row and row[0] else None

    def record_reconcile(self, **r) -> None:
        self._insert("reconciles", {"run_at": _now(), **r})

    def last_reconciles(self, n: int = 7) -> List[dict]:
        return self._last("reconciles", n)

    def reconcile_info(self, day: str) -> Tuple[int, int]:
        """(how many times this day was reconciled, open items in the latest report)."""
        n = self.db.execute("SELECT COUNT(*) FROM reconciles WHERE day=?", (day,)).fetchone()[0]
        row = self.db.execute(
            "SELECT requeued + pending FROM reconciles WHERE day=? ORDER BY id DESC LIMIT 1",
            (day,)).fetchone()
        return n, (row[0] if row else 0)

    def close(self) -> None:
        self.db.close()
