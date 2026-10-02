"""CLI entrypoint: python -m app <command>"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timedelta


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        doc = {
            "@timestamp": datetime.now().astimezone().isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "service": "odf-backup",
        }
        if hasattr(record, "summary"):
            doc["summary"] = record.summary
        if record.exc_info:
            doc["exception"] = self.formatException(record.exc_info)
        return json.dumps(doc, default=str)


def _setup_logging(level: str) -> None:
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers[:] = [h]
    root.setLevel(level.upper())
    for noisy in ("botocore", "boto3", "urllib3", "s3transfer"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="odf-backup")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("run", help="one cycle: reconcile if due, mirror the delta, retries, backfill")
    rc = sub.add_parser("reconcile", help="compare one day of MSSQL rows with the backup")
    rc.add_argument("--date", help="createdDate day, YYYY-MM-DD (default: yesterday)")
    rc.add_argument("--no-verify", action="store_true",
                    help="trust the local result records; don't check each object in MinIO")
    sub.add_parser("status", help="watermark, backfill progress, totals, last runs and reconciles")
    wm = sub.add_parser("set-watermark", help="move the incremental watermark")
    wm.add_argument("value", help="ISO datetime in the createdDate clock, e.g. 2026-09-01T00:00:00")
    rq = sub.add_parser("requeue", help="put dead/source_missing items back to pending")
    rq.add_argument("--status", nargs="+", default=["dead"],
                    choices=["dead", "source_missing", "invalid_url"])
    sub.add_parser("pg-ddl", help="print the CREATE TABLE statements for the Postgres store")
    args = p.parse_args(argv)

    from .state import State

    if args.cmd in ("run", "reconcile"):
        from .config import load_settings
        from . import runner
        s = load_settings()
        _setup_logging(s.log_level)
        try:
            if args.cmd == "run":
                return runner.run_once(s)
            day = datetime.fromisoformat(args.date) if args.date else \
                (datetime.now() - timedelta(days=1))
            day = day.replace(hour=0, minute=0, second=0, microsecond=0)
            report = runner.reconcile_once(s, day, verify=not args.no_verify)
            return 0 if report is not None else 2
        except Exception:
            logging.getLogger("odf-backup").exception("%s aborted", args.cmd)
            return 2

    if args.cmd == "pg-ddl":
        from .pgsink import ddl_statements
        print(";\n\n".join(ddl_statements(os.getenv("PG_SCHEMA") or "public")) + ";")
        return 0

    state_dir = os.getenv("STATE_DIR", "/data")
    state = State(os.path.join(state_dir, "state.db"))
    try:
        if args.cmd == "status":
            wm_val = state.get_watermark()
            rng = state.kv_get("backfill_range")
            print(json.dumps({
                "watermark": wm_val.isoformat() if wm_val else None,
                "mirroring_since": state.kv_get("start_from"),
                "backfill": {
                    "range": rng.split("|") if rng else None,
                    "cursor": state.kv_get("backfill_cursor"),
                    "done": state.kv_get("backfill_done") == "1",
                },
                "backed_up_total": state.result_totals(),
                "failures_by_status": state.failure_counts(),
                "last_reconciles": state.last_reconciles(7),
                "last_runs": state.last_runs(10),
            }, indent=2, default=str))
        elif args.cmd == "set-watermark":
            state.set_watermark(datetime.fromisoformat(args.value))
            print(f"watermark set to {args.value}")
        elif args.cmd == "requeue":
            n = state.requeue(tuple(args.status))
            print(f"requeued {n} item(s)")
    finally:
        state.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
