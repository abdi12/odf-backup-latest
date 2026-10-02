# odf-backup

Mirrors document objects referenced in MSSQL (`docUrl`, `createdDate`) from the ODF S3 bucket on
OCP 4.15 to a MinIO backup bucket. One container image, run every 10 minutes by an OpenShift
CronJob (or by cron with Docker Compose).

**The source MSSQL database is only read.** The service runs two kinds of statement there: a
clock query and `SELECT docUrl, createdDate ... WHERE createdDate >= ? AND createdDate < ?`.
It creates no tables or indexes and writes no rows. Everything the service records goes to its
own state database, and optionally to its own Postgres database.

## What one run does

| Step | What happens |
|---|---|
| 1. Lock | Takes `/data/run.lock`. If a run is still going, exits 0. |
| 2. Reconcile | Once a day, after `RECONCILE_HOUR`: checks yesterday and queues anything missing. |
| 3. Incremental | Copies rows created since the last run, plus queued retries. |
| 4. Backfill | If `BACKFILL_FROM` is set: uses the time left in the run for older data. |
| 5. Results | Records each object with its size in state.db, and in Postgres if configured. |

`docUrl` maps to the same key in MinIO. For example
`https://storage.apps.btpnsyariah.com/images/DOMA/.../X_KLAUSUL_AKAD.jpg` is bucket `images`,
key `DOMA/.../X_KLAUSUL_AKAD.jpg`.

### Incremental

- Window: `[watermark - OVERLAP_MINUTES, db_now - SAFETY_LAG_MINUTES)`, using the database clock.
- The first run starts from today 00:00, or `START_FROM`.
- Each object: skip if already recorded or already in MinIO, otherwise GET from ODF and PUT to
  MinIO with `Content-MD5`.
- Every row ends as a result record or a failure record before the watermark moves.

| Outcome | What happens |
|---|---|
| Transient error | `pending`, retried next run. `dead` after `MAX_ATTEMPTS`. |
| Object missing in ODF | `source_missing`, logged, not retried |
| Unparseable docUrl | `invalid_url` |
| Run hits `MAX_RUN_MINUTES` | Leftovers go to `pending` without counting an attempt |

### Backfill

- Set `BACKFILL_FROM` (for example `2026-01-01T00:00:00`). No separate job is needed.
- The range ends at `BACKFILL_TO`, or at the moment incremental mirroring started if empty.
- Each run works through the range in `BACKFILL_CHUNK_MINUTES` slices with `BACKFILL_WORKERS`
  threads, after the incremental step, until `MAX_RUN_MINUTES`.
- Progress (a cursor) is stored in the state database, so it resumes on the next run.
- When the cursor reaches the end it logs `backfill complete` and stops querying that range.
- Changing `BACKFILL_FROM` or `BACKFILL_TO` restarts it for the new range. Objects already
  backed up are skipped.

### Reconcile

- **Automatic:** on the first run after `RECONCILE_HOUR`, yesterday's rows are read from MSSQL
  and compared with the backup.
- With `RECONCILE_VERIFY_DEST=true` every object is also checked in MinIO (existence and size).
- Anything missing is queued and copied in the same run. The day is then checked again on the
  next run (up to 3 times) so the last report shows the final state.
- Days before mirroring started are not reconciled, unless a completed backfill covers them.
- **Manual:** `reconcile --date YYYY-MM-DD`. Add `--no-verify` to skip the MinIO check.

Each reconcile writes a report (table `reconciles`, also logged as JSON):

| Field | Meaning |
|---|---|
| `db_rows` / `objects` | Rows in MSSQL for the day / unique objects |
| `backed_up` / `bytes` | Objects confirmed in the backup and their total size |
| `requeued` | Had no backup: queued now |
| `missing_in_dest` | Recorded as backed up but not in MinIO: queued now |
| `size_mismatch` | MinIO size differs from the recorded size. Not fixed automatically. |
| `pending` / `dead` / `source_missing` / `invalid_url` | Known failures for that day |

### Result records (with object size)

Every backed-up object gets one record in the state database (`/data/state.db`, table `results`):

| Column | Content |
|---|---|
| `key` | Object key in both buckets |
| `doc_url`, `created` | The MSSQL row it came from |
| `size` | Object size in bytes |
| `action` | `copied`, or `skipped` if it was already in MinIO |
| `source_etag` | ETag in ODF |
| `backed_up_at` | When it was confirmed |

State.db is always the working copy. Nothing else is needed for the service to run.

### Postgres result store

Set `PG_HOST` to also keep the records in Postgres, so they can be queried with SQL and survive
the loss of the volume. The OpenShift ConfigMap is preset to
`doma-db-backup.devops-prd.svc:5432`, database `doma-object`.

| Table | Content |
|---|---|
| `backup_result` | One row per backed-up object: `object_key`, `doc_url`, `created_date`, `object_size` (bytes), `action`, `source_etag`, `backed_up_at` |
| `backup_failure` | Current failures: `pending`, `dead`, `source_missing`, `invalid_url`. Replaced on every run. |
| `backup_reconcile` | One row per reconcile report, including the day's total `bytes` |
| `backup_run` | One row per run |

- **Publishing:** new records are sent at the end of each run. If Postgres is unreachable the
  run still succeeds, and the records stay queued in state.db for the next run.
- **Tables:** created automatically on the first run if the login may create tables in
  `PG_SCHEMA`. Otherwise print them with `python -m app pg-ddl`, have them created, and grant
  the login SELECT, INSERT, UPDATE, DELETE on the four tables and USAGE on their sequences.
- **Volume size:** with Postgres holding the long-term record you can set
  `RESULT_RETENTION_DAYS` (for example 90) so state.db stays small. Only published records are
  pruned.
- **Empty `PG_HOST`:** records are kept in state.db only.

Example queries:

```sql
-- Objects and total size backed up per day
SELECT created_date::date AS day, count(*) AS objects, pg_size_pretty(sum(object_size)) AS size
FROM backup_result GROUP BY 1 ORDER BY 1 DESC;

-- Was this document backed up, and how big is it?
SELECT object_size, backed_up_at FROM backup_result
WHERE doc_url = 'https://storage.apps.btpnsyariah.com/images/DOMA/.../X_KLAUSUL_AKAD.jpg';
```

### Exit codes

| Code | Meaning |
|---|---|
| 0 | OK, or skipped because another run holds the lock |
| 1 | Some copies failed and are queued. Only when `STRICT_EXIT=true`. |
| 2 | Run aborted (database or configuration problem). Nothing was advanced. |

## Prerequisites

**MSSQL**

- A login that only has SELECT on the document table. This is what guarantees the service
  cannot change the production database.
- Nothing has to be created or changed there.
- Query cost: each run reads one time window by `createdDate`. If that column has no index,
  every run scans the table. Check with the DBA before go-live; if there is no index, lower the
  load with a longer schedule or point `MSSQL_HOST` at a read replica.

**Postgres (optional)**

- Database `doma-object` and a login for the service. See "Postgres result store".

**ODF**

- An S3 user with read-only access to bucket `images`.

**MinIO**

```bash
mc mb --with-lock minio/images-backup
mc retention set --default GOVERNANCE 365d minio/images-backup   # match your retention policy
# Service account: s3:PutObject and s3:GetObject on images-backup/*, no delete
```

## Deploy on OpenShift

Files are in `deploy/openshift/`:

| File | Content |
|---|---|
| `configmap.yaml` | All non-sensitive settings |
| `secret.example.yaml` | Template for the Secret: MSSQL login, both S3 key pairs, Postgres login |
| `pvc.yaml` | 20Gi ReadWriteOnce volume for the state database |
| `build.yaml` | ImageStream and BuildConfig (uses `Dockerfile.ubi`) |
| `cronjob.yaml` | Runs every 10 minutes, `concurrencyPolicy: Forbid` |
| `job-ops.yaml` | One-off commands: status, manual reconcile, requeue |

```bash
oc new-project odf-backup

# 1. Secret (sensitive values). Keep secret.env out of git and delete it afterwards.
cat > secret.env <<'EOF'
MSSQL_USER=svc_odf_backup_ro
MSSQL_PASSWORD=...
SRC_S3_ACCESS_KEY=...
SRC_S3_SECRET_KEY=...
DST_S3_ACCESS_KEY=...
DST_S3_SECRET_KEY=...
PG_USER=...
PG_PASSWORD=...
EOF
oc create secret generic odf-backup-secret --from-env-file=secret.env && rm secret.env

# 2. CA bundle for the ODF route and MinIO (skip if both use public CAs and
#    remove SRC_S3_CA_BUNDLE / DST_S3_CA_BUNDLE from configmap.yaml)
cat ocp-ingress-ca.pem minio-ca.pem > ca-bundle.pem
oc create configmap odf-backup-ca --from-file=ca-bundle.pem

# 3. Build the image
oc apply -f deploy/openshift/build.yaml
oc start-build odf-backup --from-dir=. --follow

# 4. Edit deploy/openshift/configmap.yaml, then create ConfigMap, PVC and CronJob
oc apply -k deploy/openshift

# 5. First run by hand
oc create job --from=cronjob/odf-backup odf-backup-first
oc logs -f job/odf-backup-first
```

Notes:

- **Namespace:** the manifests assume `odf-backup`. For another one, change `namespace` in
  `kustomization.yaml` and the image path in `cronjob.yaml` and `job-ops.yaml`.
- **Build access:** the build pulls from `registry.access.redhat.com`, `packages.microsoft.com`
  and PyPI. If the cluster cannot reach them, build the image elsewhere, push it to your
  registry and change `image` in `cronjob.yaml`.
- **CA bundle:** when a bundle is set it replaces the system CAs for that endpoint, so it must
  contain the full chain.
- **Network:** the pod needs egress to MSSQL (1433), the ODF S3 endpoint, MinIO and Postgres.
  The Postgres service is in namespace `devops-prd`. If that namespace has a NetworkPolicy,
  allow ingress on 5432 from namespace `odf-backup`, or deploy this service into `devops-prd`.
- **Storage class:** use block storage (for example `ocs-storagecluster-ceph-rbd`). The state
  database is SQLite and should not sit on NFS or CephFS.
- **Security:** runs under the default `restricted-v2` SCC with a random UID. No extra
  permissions or service account are needed.

### Operate

```bash
# Change a setting: every run is a new pod, so it applies from the next tick
oc set data configmap/odf-backup-config BACKFILL_FROM=2026-01-01T00:00:00

# Status, manual reconcile, requeue: edit args in job-ops.yaml, then
oc create -f deploy/openshift/job-ops.yaml

# Pause / resume
oc patch cronjob odf-backup -p '{"spec":{"suspend":true}}'
oc patch cronjob odf-backup -p '{"spec":{"suspend":false}}'

# Logs of the latest run
oc logs -l app=odf-backup --tail=50
```

Logs are one JSON object per line. The last line of a run is `run finished` with a `summary`
field. Reconcile reports are logged as `reconcile <day>: complete` or `gaps found`.

Alerts worth setting:

- No `run finished` log in 30 minutes.
- A Job failed (exit 2). With `STRICT_EXIT=false` this only happens when a run aborts.
- `summary.failures_by_status.dead > 0`.
- A reconcile log with `summary.clean = false` on the third pass of a day.
- `summary.published.error = true` for several runs (Postgres unreachable).

## Run with Docker Compose instead

```bash
cp .env.example .env && vi .env
mkdir -p secrets certs && chmod 700 secrets
printf '%s' 'db-password'   > secrets/mssql_password
printf '%s' 'odf-secret'    > secrets/src_s3_secret_key
printf '%s' 'minio-secret'  > secrets/dst_s3_secret_key
docker compose build
docker compose run --rm odf-backup run
docker compose run --rm odf-backup status
sudo cp deploy/cron.d-odf-backup /etc/cron.d/odf-backup
```

## Upgrading from the first version

- The state database is migrated on first start. Nothing needs to be done by hand.
- `DONE_RETENTION_DAYS` is gone. Result records are kept (`RESULT_RETENTION_DAYS=0`).
- `RESULT_TABLE` (writing results to MSSQL) is removed. Use the Postgres store instead.
- Backfill is now `BACKFILL_FROM`. Moving the watermark back is no longer needed for it.

## Tests

```bash
pip install -r requirements.txt pytest "moto[s3]>=5"
pytest -q

# Also run the Postgres test against a scratch database
TEST_PG_HOST=localhost TEST_PG_PORT=5432 TEST_PG_DATABASE=test TEST_PG_USER=u TEST_PG_PASSWORD=p pytest -q
```

## Architecture diagram

`.archify/architecture-odf-backup-20261002-112233/odf-backup-architecture.html` is an interactive diagram built with
[Archify](https://github.com/tt-a1i/archify) (open it in a browser; it works offline).
Its source is `candidate.json` in the same folder. To change it in Claude Code:

```bash
npx skills add tt-a1i/archify -g
```

Then ask, for example: "Use Archify to update the architecture diagram from
.archify/architecture-odf-backup-20261002-112233/candidate.json: add the F5 in front of MinIO."
