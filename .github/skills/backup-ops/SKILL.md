---
name: backup-ops
description: 'Operational backup, reconciliation, and retry workflows for the ODF mirror service. Use for status checks, a normal run, manual reconcile, requeueing failed items, or Postgres DDL/reporting tasks.'
argument-hint: '[status|run|reconcile|requeue|pg-ddl]'
user-invocable: true
---

# Backup Operations

## When to use

- Check the current backup state, watermark, and recent runs.
- Run a normal incremental sync cycle.
- Reconcile a specific day against the backup bucket.
- Requeue failed objects after a transient issue is resolved.
- Inspect the PostgreSQL reporting schema or service status.

## Project context

This repository mirrors ODF S3 objects into MinIO using MSSQL rows as the source of truth. The service is intentionally conservative:

- MSSQL is treated as read-only; do not issue any write or schema-changing queries.
- Runtime state is stored in SQLite at `/data/state.db` by default, or `STATE_DIR/state.db`.
- The lock file at `/data/run.lock` prevents overlapping runs.
- Postgres reporting is optional and is only enabled when `PG_HOST` is set.

See [README.md](../../README.md) and [AGENTS.md](../../AGENTS.md) for the full operational model and deployment details.

## Operational workflow

### 1. Start with the right command

Choose the smallest command that matches the task:

- `python -m app status` — inspect watermark, backfill progress, totals, and recent run/reconcile history.
- `python -m app run` — execute one normal cycle: reconcile if due, then mirror the incremental window and retries.
- `python -m app reconcile --date 2026-10-02` — reconcile a specific day and compare MSSQL rows to the backup.
- `python -m app reconcile --date 2026-10-02 --no-verify` — trust local result records without checking every object in MinIO.
- `python -m app requeue --status dead` — return dead or source-missing items to `pending` for retry.
- `python -m app pg-ddl` — print the PostgreSQL schema DDL for the reporting tables.

### 2. Check the environment before acting

Before making changes or diagnosing a problem, confirm the service configuration is valid and consistent with the environment:

- required env vars such as `MSSQL_*`, `SRC_S3_*`, `DST_S3_*`, `STATE_DIR`
- optional `*_FILE` secrets are loaded correctly
- `PG_HOST` is only set when PostgreSQL reporting should be used
- any invalid datetime or identifier values fail early during settings load

This project prefers fail-fast configuration rather than silent fallback behavior.

### 3. Interpret the results correctly

The service reports statuses through the state database and run output. Use these meanings to guide the next action:

- Exit code `0`: success or lock skip.
- Exit code `1`: partial copy failures were queued because `STRICT_EXIT` is enabled.
- Exit code `2`: abort due to database or configuration issue; nothing advanced.

Common runtime states:

- `pending`: retry later
- `dead`: exhausted retries
- `source_missing`: object no longer exists in the source bucket
- `invalid_url`: URL could not be parsed

### 4. Handle retry and reconciliation cases deliberately

#### Reconciliation

- Use manual reconcile when a single day needs confirmation against the backup.
- Let the service perform daily automatic reconcile checks when due.
- If results are missing in MinIO, queued items are retried in the same run.
- Use `--no-verify` only when you deliberately want to skip the MinIO existence/size validation step.

#### Retry queue

- Requeue only after confirming the underlying issue is resolved.
- The service supports `dead`, `source_missing`, and `invalid_url` requeue paths.
- Keep the requeue scope narrow; avoid broad retry storms unless the issue pattern is clearly understood.

### 5. Preserve the design constraints

When working in this repo, do not:

- write to the source MSSQL database
- invent a second persistence model beyond the existing SQLite/Postgres flow
- bypass the CLI and state model for operational checks
- add runtime logic that ignores the lock file or watermark model

## Completion checks

A backup operation is complete when all of the following are true:

1. The chosen command exits with a valid status for the scenario.
2. The run or reconcile result matches the expected bucket/object state.
3. The state database reflects the expected watermark, failure counts, and totals.
4. Any queued failures were intentionally retried or documented as terminal failures.
5. No MSSQL writes or schema changes were introduced.

## References

- [README.md](../../README.md)
- [AGENTS.md](../../AGENTS.md)
- [tests/test_service.py](../../tests/test_service.py)
