# AGENTS.md

This repository contains a Python service that mirrors objects from an ODF S3 bucket into a MinIO backup bucket, using MSSQL as a read-only source and a local SQLite state database for progress and retry tracking.

## Project overview

- Primary entrypoint: `python -m app`
- Core logic lives under `app/`
- Deployment manifests live under `deploy/openshift/`
- Automated tests live under `tests/`
- Full operational details are in [README.md](README.md)

## Key workflow and safety rules

- Treat the MSSQL source as read-only. The service runs SELECT queries only and never creates tables, indexes, or writes to the source database.
- Configuration is environment-driven; values can also be supplied via `*_FILE` secrets. Fail early on invalid settings.
- The service tracks state in SQLite at `/data/state.db` by default, or `STATE_DIR/state.db` when overridden.
- Optional PostgreSQL reporting is enabled only when `PG_HOST` is set; otherwise keep all records in SQLite.
- The run loop is built around a lock file at `/data/run.lock` so overlapping runs exit cleanly.

## Typical commands

- Run one cycle: `python -m app run`
- Reconcile one day: `python -m app reconcile --date 2026-10-02`
- Manual status check: `python -m app status`
- Print the required PostgreSQL DDL: `python -m app pg-ddl`
- Test suite: `pytest -q`

## Repository layout

- `app/__main__.py`: CLI and subcommands.
- `app/config.py`: environment parsing and validation.
- `app/runner.py`: orchestration of incremental runs, backfill, and reconciliation.
- `app/copier.py`: S3 copy logic and result classification.
- `app/state.py`: persistence for watermarks, retries, and result totals.
- `app/db.py` and `app/pgsink.py`: database access for MSSQL and optional Postgres reporting.
- `tests/test_service.py`: behavior tests covering URL parsing, copying, retries, and state transitions.
- `deploy/openshift/`: OpenShift manifests and job templates.

## Development conventions

- Preserve the existing pattern of explicit environment validation and fail-fast configuration.
- Prefer small, behavior-focused changes and update tests when changing run logic, state transitions, or URL handling.
- Keep source-side queries read-only and maintain deterministic logging/output for automation.
- Use the existing CLI and SQLite state model rather than inventing a new runtime path or persistence layer.

## References

- [README.md](README.md) for architecture, deployment, and production behavior.
- [deploy/openshift](deploy/openshift/) for the OpenShift deployment model.
- [tests/test_service.py](tests/test_service.py) for the expected service behavior.
