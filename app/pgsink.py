"""PostgreSQL result store: the service's own reporting database.

state.db stays the working copy. After each run the new records are pushed here:
  backup_result     one row per backed-up object, with its size
  backup_failure    current failures (replaced on every run)
  backup_reconcile  daily reconcile reports
  backup_run        one row per run
Records that could not be pushed stay queued in state.db and go with the next run.
"""
from __future__ import annotations

from datetime import date, datetime
from typing import List, Optional

from .config import Settings
from .state import State

BATCH = 5000
MAX_BATCHES_PER_RUN = 40          # 200k result records per run at most

TABLES = ("backup_result", "backup_failure", "backup_reconcile", "backup_run")


def ddl_statements(schema: str) -> List[str]:
    s = f'"{schema}"'
    return [
        f"""CREATE TABLE IF NOT EXISTS {s}.backup_result (
    object_key    text PRIMARY KEY,
    doc_url       text,
    created_date  timestamp,
    object_size   bigint NOT NULL,          -- bytes
    action        text NOT NULL,            -- copied | skipped (already in MinIO)
    source_etag   text,
    backed_up_at  timestamp NOT NULL
)""",
        f"CREATE INDEX IF NOT EXISTS backup_result_created_idx ON {s}.backup_result (created_date)",
        f"CREATE INDEX IF NOT EXISTS backup_result_backed_up_idx ON {s}.backup_result (backed_up_at)",
        f"""CREATE TABLE IF NOT EXISTS {s}.backup_failure (
    doc_url       text PRIMARY KEY,
    created_date  timestamp,
    attempts      integer NOT NULL,
    status        text NOT NULL,            -- pending | dead | source_missing | invalid_url
    last_error    text,
    updated_at    timestamp NOT NULL
)""",
        f"""CREATE TABLE IF NOT EXISTS {s}.backup_reconcile (
    id               bigserial PRIMARY KEY,
    run_at           timestamp NOT NULL,
    day              date NOT NULL,
    db_rows          integer, objects integer, backed_up integer, bytes bigint,
    requeued         integer, missing_in_dest integer, size_mismatch integer,
    pending          integer, dead integer, source_missing integer, invalid_url integer,
    verified         boolean
)""",
        f"CREATE INDEX IF NOT EXISTS backup_reconcile_day_idx ON {s}.backup_reconcile (day)",
        f"""CREATE TABLE IF NOT EXISTS {s}.backup_run (
    id               bigserial PRIMARY KEY,
    started_at       timestamp, finished_at timestamp,
    window_start     timestamp, window_end timestamp,
    db_rows          integer, copied integer, skipped integer, failed integer,
    source_missing   integer, invalid_url integer, deferred integer, bytes bigint,
    backfill_objects integer, backfill_bytes bigint
)""",
        f"CREATE INDEX IF NOT EXISTS backup_run_started_idx ON {s}.backup_run (started_at)",
    ]


def _dt(v: Optional[str]) -> Optional[datetime]:
    return datetime.fromisoformat(v) if v else None


def connect(s: Settings):
    import psycopg  # imported lazily so the service runs without it when Postgres is off

    return psycopg.connect(
        host=s.pg_host, port=s.pg_port, dbname=s.pg_database, user=s.pg_user,
        password=s.pg_password, sslmode=s.pg_sslmode, connect_timeout=15,
        application_name="odf-backup")


def _ensure_tables(conn, schema: str) -> None:
    """Create the tables only if they are missing (so a pre-created schema needs no CREATE right)."""
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM information_schema.tables "
                    "WHERE table_schema = %s AND table_name = ANY(%s)", (schema, list(TABLES)))
        if cur.fetchone()[0] == len(TABLES):
            return
        for stmt in ddl_statements(schema):
            cur.execute(stmt)
    conn.commit()


def publish(s: Settings, state: State) -> dict:
    """Push everything not yet published. Each batch is its own transaction."""
    sch = f'"{s.pg_schema}"'
    out = {"results": 0, "failures": 0, "reconciles": 0, "runs": 0}
    conn = connect(s)
    try:
        _ensure_tables(conn, s.pg_schema)

        # --- results (upsert by object key) ---
        upsert = (
            f"INSERT INTO {sch}.backup_result "
            "(object_key, doc_url, created_date, object_size, action, source_etag, backed_up_at) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s) "
            "ON CONFLICT (object_key) DO UPDATE SET doc_url=EXCLUDED.doc_url, "
            "created_date=EXCLUDED.created_date, object_size=EXCLUDED.object_size, "
            "action=EXCLUDED.action, source_etag=EXCLUDED.source_etag, "
            "backed_up_at=EXCLUDED.backed_up_at")
        for _ in range(MAX_BATCHES_PER_RUN):
            rows = state.unsynced(BATCH)
            if not rows:
                break
            with conn.cursor() as cur:
                cur.executemany(upsert, [
                    (r["key"], r["doc_url"], _dt(r["created"]), r["size"], r["action"],
                     r["source_etag"], _dt(r["backed_up_at"])) for r in rows])
            conn.commit()
            state.mark_synced([r["key"] for r in rows])
            out["results"] += len(rows)

        # --- failures (small table: replace with the current picture) ---
        failures = state.all_failures()
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {sch}.backup_failure")
            if failures:
                cur.executemany(
                    f"INSERT INTO {sch}.backup_failure "
                    "(doc_url, created_date, attempts, status, last_error, updated_at) "
                    "VALUES (%s,%s,%s,%s,%s,%s)",
                    [(f["doc_url"], _dt(f["created"]), f["attempts"], f["status"],
                      f["last_error"], _dt(f["updated_at"])) for f in failures])
        conn.commit()
        out["failures"] = len(failures)

        # --- reconcile reports (append) ---
        last = int(state.kv_get("pg_last_reconcile_id") or 0)
        recs = state.rows_after("reconciles", last)
        if recs:
            with conn.cursor() as cur:
                cur.executemany(
                    f"INSERT INTO {sch}.backup_reconcile (run_at, day, db_rows, objects, backed_up, "
                    "bytes, requeued, missing_in_dest, size_mismatch, pending, dead, "
                    "source_missing, invalid_url, verified) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    [(_dt(r["run_at"]), date.fromisoformat(r["day"]), r["db_rows"], r["objects"], r["backed_up"],
                      r["bytes"], r["requeued"], r["missing_in_dest"], r["size_mismatch"],
                      r["pending"], r["dead"], r["source_missing"], r["invalid_url"],
                      bool(r["verified"])) for r in recs])
            conn.commit()
            state.kv_set("pg_last_reconcile_id", str(recs[-1]["id"]))
            out["reconciles"] = len(recs)

        # --- runs (append) ---
        last = int(state.kv_get("pg_last_run_id") or 0)
        runs = state.rows_after("runs", last)
        if runs:
            with conn.cursor() as cur:
                cur.executemany(
                    f"INSERT INTO {sch}.backup_run (started_at, finished_at, window_start, "
                    "window_end, db_rows, copied, skipped, failed, source_missing, invalid_url, "
                    "deferred, bytes, backfill_objects, backfill_bytes) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    [(_dt(r["started_at"]), _dt(r["finished_at"]), _dt(r["window_start"]),
                      _dt(r["window_end"]), r["rows"], r["copied"], r["skipped"], r["failed"],
                      r["source_missing"], r["invalid"], r["deferred"], r["bytes"],
                      r["backfill_objects"], r["backfill_bytes"]) for r in runs])
            conn.commit()
            state.kv_set("pg_last_run_id", str(runs[-1]["id"]))
            out["runs"] = len(runs)
    finally:
        conn.close()
    return out
