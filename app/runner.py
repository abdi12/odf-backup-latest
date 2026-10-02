"""One run: reconcile (once a day), mirror the delta, retry failures, backfill, publish results.

MSSQL is only ever read. Results are kept in state.db and, if configured, in Postgres.
"""
from __future__ import annotations

import fcntl
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from . import copier, db, pgsink
from .config import Settings
from .s3 import make_client
from .state import DEAD, INVALID_URL, PENDING, SOURCE_MISSING, State
from .url_parser import InvalidDocUrl, parse_doc_url

log = logging.getLogger("odf-backup")

EXIT_OK, EXIT_FAILURES = 0, 1
MAX_AUTO_RECONCILES_PER_DAY = 3

Tasks = Dict[str, Tuple[str, Optional[datetime]]]   # key -> (docUrl, createdDate)


class RunLock:
    """File lock so only one process works on the state volume at a time."""

    def __init__(self, path: str):
        self.path, self.fh = path, None

    def acquire(self, wait_seconds: int = 0) -> bool:
        self.fh = open(self.path, "w")
        give_up = time.monotonic() + wait_seconds
        while True:
            try:
                fcntl.flock(self.fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
                self.fh.write(str(os.getpid()))
                self.fh.flush()
                return True
            except BlockingIOError:
                if time.monotonic() >= give_up:
                    self.fh.close()
                    self.fh = None
                    return False
                time.sleep(2)

    def release(self) -> None:
        if self.fh:
            fcntl.flock(self.fh, fcntl.LOCK_UN)
            self.fh.close()
            self.fh = None


def _clients(s: Settings, src, dst):
    pool = max(s.workers, s.backfill_workers) + 4
    src = src or make_client(s.src_endpoint, s.src_access_key, s.src_secret_key,
                             s.src_region, s.src_verify, pool)
    dst = dst or make_client(s.dst_endpoint, s.dst_access_key, s.dst_secret_key,
                             s.dst_region, s.dst_verify, pool)
    return src, dst


# --------------------------------------------------------------------------- copy helpers
def _build_tasks(s: Settings, state: State, rows) -> Tuple[Tasks, int, int]:
    """Rows -> unique keys that still need copying. Returns (tasks, invalid, already_done)."""
    tasks: Tasks = {}
    invalid = already = 0
    for doc_url, created in rows:
        try:
            key = parse_doc_url(doc_url, s.src_bucket, s.src_url_host)
        except InvalidDocUrl as e:
            invalid += 1
            state.record_failure(str(doc_url), created, INVALID_URL, str(e), s.max_attempts,
                                 count_attempt=False)
            continue
        if key in tasks:
            continue
        if state.is_done(key):
            already += 1
            continue
        tasks[key] = (doc_url, created)
    return tasks, invalid, already


def _execute(s: Settings, state: State, src, dst, tasks: Tasks, workers: int,
             deadline: float, queue_deferred: bool) -> Tuple[Dict[str, int], int]:
    """Copy tasks in parallel. Only this (main) thread writes state."""
    counts = {copier.COPIED: 0, copier.SKIPPED: 0, copier.FAILED: 0,
              copier.SOURCE_MISSING: 0, copier.DEFERRED: 0}
    total_bytes = 0
    if not tasks:
        return counts, total_bytes

    def work(key: str) -> copier.CopyResult:
        if time.monotonic() > deadline:
            return copier.CopyResult(key, copier.DEFERRED, error="run deadline reached")
        return copier.copy_object(src, dst, s.src_bucket, s.dst_bucket, key, s.dst_prefix)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(work, k) for k in tasks]
        for fut in as_completed(futures):
            r = fut.result()
            doc_url, created = tasks[r.key]
            counts[r.status] += 1
            if r.status in (copier.COPIED, copier.SKIPPED):
                total_bytes += r.size
                state.mark_done(r.key, r.size, doc_url, created, r.status, r.etag)
            elif r.status == copier.SOURCE_MISSING:
                state.record_failure(doc_url, created, SOURCE_MISSING, r.error or "", s.max_attempts)
                log.warning("source missing: %s", r.key)
            elif r.status == copier.DEFERRED:
                if queue_deferred:
                    state.record_failure(doc_url, created, PENDING, r.error or "",
                                         s.max_attempts, count_attempt=False)
            else:
                state.record_failure(doc_url, created, PENDING, r.error or "", s.max_attempts)
                log.error("copy failed: %s: %s", r.key, r.error)
    return counts, total_bytes


# --------------------------------------------------------------------------- reconcile
def _start_from(s: Settings, state: State, fallback: datetime) -> datetime:
    """Where incremental mirroring began. Stored once; used as the default backfill end."""
    v = state.kv_get("start_from")
    if v:
        return datetime.fromisoformat(v)
    first = state.first_run_started()          # state created by v1 has no start_from
    if s.start_from:
        start = datetime.fromisoformat(s.start_from)
    elif first:
        start = first.replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        start = fallback
    state.kv_set("start_from", start.isoformat())
    return start


def _day_is_covered(s: Settings, state: State, day: datetime) -> bool:
    start = state.kv_get("start_from")
    if start and day >= datetime.fromisoformat(start):
        return True
    if s.backfill_from and state.kv_get("backfill_done") == "1":
        return day >= datetime.fromisoformat(s.backfill_from)
    return False


def _reconcile_due(s: Settings, state: State, now: datetime) -> Optional[datetime]:
    """Yesterday, if it should be reconciled in this run."""
    if not s.reconcile_enabled or now.hour < s.reconcile_hour:
        return None
    day = (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    if not _day_is_covered(s, state, day):
        return None
    times, open_items = state.reconcile_info(day.date().isoformat())
    if times == 0 or (open_items > 0 and times < MAX_AUTO_RECONCILES_PER_DAY):
        return day
    return None


def reconcile_day(s: Settings, state: State, dst, day: datetime, rows, verify: bool) -> dict:
    """Compare one day of MSSQL rows with the backup and queue anything that is missing."""
    keys: Tasks = {}
    invalid = 0
    for doc_url, created in rows:
        try:
            key = parse_doc_url(doc_url, s.src_bucket, s.src_url_host)
        except InvalidDocUrl as e:
            invalid += 1
            state.record_failure(str(doc_url), created, INVALID_URL, str(e), s.max_attempts,
                                 count_attempt=False)
            continue
        keys.setdefault(key, (doc_url, created))

    recorded = state.sizes_for(list(keys))
    missing_in_dest = size_mismatch = 0
    if verify and recorded:
        def head(key: str):
            return key, copier.head_size(dst, s.dst_bucket, f"{s.dst_prefix}{key}")

        with ThreadPoolExecutor(max_workers=s.workers) as pool:
            for key, actual in pool.map(head, list(recorded)):
                if actual is None:
                    missing_in_dest += 1
                    state.delete_result(key)
                    del recorded[key]
                    log.error("reconcile: recorded as backed up but not in MinIO: %s", key)
                elif actual != recorded[key]:
                    size_mismatch += 1
                    log.error("reconcile: size mismatch for %s: recorded %d, MinIO has %d",
                              key, recorded[key], actual)

    failures = state.failure_status_for([u for u, _ in keys.values()])
    backed_up = total_bytes = requeued = pending = dead = source_missing = 0
    for key, (doc_url, created) in keys.items():
        if key in recorded:
            backed_up += 1
            total_bytes += recorded[key]
            continue
        status = failures.get(doc_url)
        if status == SOURCE_MISSING:
            source_missing += 1
        elif status == DEAD:
            dead += 1
        elif status == PENDING:
            pending += 1
        else:
            requeued += 1
            state.record_failure(doc_url, created, PENDING, "reconcile: not in backup",
                                 s.max_attempts, count_attempt=False)

    report = dict(day=day.date().isoformat(), db_rows=len(rows), objects=len(keys),
                  backed_up=backed_up, bytes=total_bytes, requeued=requeued,
                  missing_in_dest=missing_in_dest, size_mismatch=size_mismatch,
                  pending=pending, dead=dead, source_missing=source_missing,
                  invalid_url=invalid, verified=int(verify))
    state.record_reconcile(**report)
    clean = backed_up == len(keys) and size_mismatch == 0
    log.log(logging.INFO if clean else logging.WARNING,
            "reconcile %s: %s", report["day"], "complete" if clean else "gaps found",
            extra={"summary": {"type": "reconcile", "clean": clean, **report}})
    return report


# --------------------------------------------------------------------------- backfill
def _backfill(s: Settings, db_api, src, dst, state: State, deadline: float,
              start_from: datetime) -> dict:
    """Work through [BACKFILL_FROM, BACKFILL_TO) in chunks with whatever time is left."""
    if not s.backfill_from:
        return {}
    b_from = datetime.fromisoformat(s.backfill_from)
    b_to = datetime.fromisoformat(s.backfill_to) if s.backfill_to else start_from
    if b_from >= b_to:
        log.warning("backfill range is empty (%s >= %s); nothing to do", b_from, b_to)
        return {}
    ident = f"{b_from.isoformat()}|{b_to.isoformat()}"
    if state.kv_get("backfill_range") != ident:
        state.kv_set("backfill_range", ident)
        state.kv_set("backfill_cursor", b_from.isoformat())
        state.kv_set("backfill_done", "0")
        log.info("backfill started for %s -> %s", b_from.isoformat(), b_to.isoformat())
    if state.kv_get("backfill_done") == "1":
        return {"done": True}

    cursor = datetime.fromisoformat(state.kv_get("backfill_cursor"))
    step = timedelta(minutes=s.backfill_chunk_minutes)
    objects = total_bytes = failed = 0
    conn = db_api.connect(s)
    try:
        while cursor < b_to and time.monotonic() < deadline:
            chunk_end = min(cursor + step, b_to)
            rows = db_api.fetch_rows(conn, s, cursor, chunk_end)
            tasks, _, _ = _build_tasks(s, state, rows)
            counts, nbytes = _execute(s, state, src, dst, tasks, s.backfill_workers, deadline,
                                      queue_deferred=False)
            objects += counts[copier.COPIED] + counts[copier.SKIPPED]
            total_bytes += nbytes
            failed += counts[copier.FAILED]
            if counts[copier.DEFERRED]:
                break   # out of time: redo this chunk next run (finished objects are skipped)
            cursor = chunk_end
            state.kv_set("backfill_cursor", cursor.isoformat())
    finally:
        conn.close()

    done = cursor >= b_to
    if done:
        state.kv_set("backfill_done", "1")
        log.info("backfill complete for %s -> %s", b_from.isoformat(), b_to.isoformat())
    return {"from": b_from.isoformat(), "to": b_to.isoformat(), "cursor": cursor.isoformat(),
            "done": done, "objects": objects, "bytes": total_bytes, "failed": failed}


# --------------------------------------------------------------------------- result sink
def _publish(s: Settings, state: State, sink) -> Optional[dict]:
    """Push unpublished records to Postgres (if configured). Never fails the run."""
    if not s.pg_enabled:
        return None
    try:
        return sink.publish(s, state)
    except Exception:
        log.exception("could not publish to Postgres %s/%s; records stay queued for the next run",
                      s.pg_host, s.pg_database)
        return {"error": True}


# --------------------------------------------------------------------------- entry points
def run_once(s: Settings, db_api=db, src=None, dst=None, state: Optional[State] = None,
             sink=pgsink) -> int:
    os.makedirs(s.state_dir, exist_ok=True)
    lock = RunLock(os.path.join(s.state_dir, "run.lock"))
    if not lock.acquire():
        log.warning("previous run still in progress; skipping this tick")
        return EXIT_OK
    own_state = state is None
    state = state or State(os.path.join(s.state_dir, "state.db"))
    try:
        return _run(s, db_api, src, dst, state, sink)
    finally:
        if own_state:
            state.close()
        lock.release()


def reconcile_once(s: Settings, day: datetime, verify: bool, db_api=db, dst=None,
                   state: Optional[State] = None, sink=pgsink) -> Optional[dict]:
    """Manual reconcile of one day. Waits for a running job to finish first."""
    os.makedirs(s.state_dir, exist_ok=True)
    lock = RunLock(os.path.join(s.state_dir, "run.lock"))
    if not lock.acquire(wait_seconds=s.max_run_minutes * 60 + 60):
        log.error("another run is holding the lock; try again later")
        return None
    own_state = state is None
    state = state or State(os.path.join(s.state_dir, "state.db"))
    try:
        conn = db_api.connect(s)
        try:
            rows = db_api.fetch_rows(conn, s, day, day + timedelta(days=1))
        finally:
            conn.close()
        if verify and dst is None:
            dst = make_client(s.dst_endpoint, s.dst_access_key, s.dst_secret_key,
                              s.dst_region, s.dst_verify, s.workers + 4)
        report = reconcile_day(s, state, dst, day, rows, verify)
        _publish(s, state, sink)
        return report
    finally:
        if own_state:
            state.close()
        lock.release()


def _run(s: Settings, db_api, src, dst, state: State, sink) -> int:
    started = time.monotonic()
    started_at = datetime.now().isoformat(timespec="seconds")
    deadline = started + s.max_run_minutes * 60

    # ---- 1. Read the delta window [start, end) using the DB clock ----
    conn = db_api.connect(s)
    try:
        now = db_api.db_now(conn, s.created_date_is_utc)
        wm = state.get_watermark()
        if wm is None:
            wm = datetime.fromisoformat(s.start_from) if s.start_from else \
                db_api.db_day_start(conn, s.created_date_is_utc)
            log.info("no watermark yet; starting from %s", wm.isoformat())
        start_from = _start_from(s, state, wm)
        window_start = wm - timedelta(minutes=s.overlap_minutes)
        window_end = now - timedelta(minutes=s.safety_lag_minutes)
        rows = db_api.fetch_rows(conn, s, window_start, window_end) if window_end > window_start else []
        recon_day = _reconcile_due(s, state, now)
        recon_rows = db_api.fetch_rows(conn, s, recon_day, recon_day + timedelta(days=1)) \
            if recon_day else None
    finally:
        conn.close()

    src, dst = _clients(s, src, dst)

    # ---- 2. Daily reconcile of yesterday: gaps are queued and copied in step 3 ----
    recon = None
    if recon_day is not None:
        try:
            recon = reconcile_day(s, state, dst, recon_day, recon_rows, s.reconcile_verify_dest)
        except Exception:
            log.exception("reconcile failed; it will be tried again next run")

    # ---- 3. Delta rows + pending retries ----
    tasks, invalid, already_done = _build_tasks(s, state, rows)
    for doc_url, created in state.pending_failures(s.retry_batch):
        try:
            key = parse_doc_url(doc_url, s.src_bucket, s.src_url_host)
        except InvalidDocUrl as e:
            state.record_failure(doc_url, created, INVALID_URL, str(e), s.max_attempts)
            continue
        tasks.setdefault(key, (doc_url, created))

    log.info("window %s -> %s: %d rows, %d to copy (%d already done, %d invalid url)",
             window_start.isoformat(), window_end.isoformat(), len(rows), len(tasks),
             already_done, invalid)
    counts, total_bytes = _execute(s, state, src, dst, tasks, s.workers, deadline,
                                   queue_deferred=True)

    # Every delta row is now either recorded as backed up or in the failures table.
    if window_end > wm:
        state.set_watermark(window_end)

    # ---- 4. Backfill with the time that is left ----
    backfill = {}
    try:
        backfill = _backfill(s, db_api, src, dst, state, deadline, start_from)
    except Exception:
        log.exception("backfill step failed; it will continue next run")

    # ---- 5. Record the run, publish to Postgres, housekeeping ----
    summary = dict(
        started_at=started_at, finished_at=datetime.now().isoformat(timespec="seconds"),
        window_start=window_start.isoformat(), window_end=window_end.isoformat(),
        rows=len(rows), copied=counts[copier.COPIED], skipped=counts[copier.SKIPPED],
        failed=counts[copier.FAILED], source_missing=counts[copier.SOURCE_MISSING],
        invalid=invalid, deferred=counts[copier.DEFERRED], bytes=total_bytes,
        backfill_objects=backfill.get("objects", 0), backfill_bytes=backfill.get("bytes", 0))
    state.record_run(**summary)
    published = _publish(s, state, sink)
    state.prune_results(s.result_retention_days, only_synced=s.pg_enabled)
    log.info("run finished", extra={"summary": {
        "type": "run", **summary, "duration_s": round(time.monotonic() - started, 1),
        "backfill": backfill or None, "reconciled_day": recon["day"] if recon else None,
        "published": published, "failures_by_status": state.failure_counts()}})

    failed = counts[copier.FAILED] + backfill.get("failed", 0)
    return EXIT_FAILURES if (failed and s.strict_exit) else EXIT_OK
