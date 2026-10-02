import os
import sqlite3
from datetime import datetime, timedelta

import boto3
import pytest
from moto import mock_aws

from app import copier
from app.config import Settings
from app.runner import RunLock, reconcile_once, run_once
from app.state import State
from app.url_parser import InvalidDocUrl, parse_doc_url

SAMPLE = ("https://storage.apps.btpnsyariah.com/images/DOMA/8973/W076808973/"
          "W07680897314/W07680897314_KLAUSUL_AKAD.jpg")
HOST = "storage.apps.btpnsyariah.com"
NOW = datetime(2026, 10, 2, 17, 0, 0)


# ---------------- url parser ----------------
def test_parse_sample():
    assert parse_doc_url(SAMPLE, "images", HOST) == \
        "DOMA/8973/W076808973/W07680897314/W07680897314_KLAUSUL_AKAD.jpg"


def test_parse_decodes_percent_encoding():
    assert parse_doc_url(f"https://{HOST}/images/A/b%20c.jpg", "images") == "A/b c.jpg"


@pytest.mark.parametrize("url", [
    None, "", "ftp://x/images/a.jpg", f"https://{HOST}/other/a.jpg",
    f"https://{HOST}/images/", f"https://{HOST}/images/a/../b.jpg", "https://evil.com/images/a.jpg",
])
def test_parse_rejects(url):
    with pytest.raises(InvalidDocUrl):
        parse_doc_url(url, "images", HOST)


# ---------------- fixtures ----------------
@pytest.fixture
def s3():
    with mock_aws():
        src = boto3.client("s3", region_name="us-east-1")
        dst = boto3.client("s3", region_name="us-east-1")
        src.create_bucket(Bucket="images")
        dst.create_bucket(Bucket="images-backup")
        yield src, dst


def settings(tmp_path, **kw):
    base = dict(
        mssql_host="x", mssql_port=1433, mssql_database="d", mssql_user="u", mssql_password="p",
        mssql_encrypt=True, mssql_trust_server_cert=False, mssql_query_timeout=60,
        source_table="dbo.Documents", col_doc_url="docUrl", col_created="createdDate",
        created_date_is_utc=False,
        src_endpoint="x", src_access_key="x", src_secret_key="x", src_region="us-east-1",
        src_bucket="images", src_url_host=HOST, src_verify=True,
        dst_endpoint="x", dst_access_key="x", dst_secret_key="x", dst_region="us-east-1",
        dst_bucket="images-backup", dst_prefix="", dst_verify=True,
        state_dir=str(tmp_path), workers=4, overlap_minutes=15, safety_lag_minutes=2,
        start_from=None, max_attempts=3, retry_batch=100, max_run_minutes=9,
        strict_exit=True, log_level="INFO",
        result_retention_days=0,
        pg_host=None, pg_port=5432, pg_database=None, pg_user=None, pg_password=None,
        pg_schema="public", pg_sslmode="prefer",
        backfill_from=None, backfill_to=None, backfill_chunk_minutes=60, backfill_workers=4,
        reconcile_enabled=True, reconcile_hour=1, reconcile_verify_dest=True)
    base.update(kw)
    return Settings(**base)


class FakeDB:
    """Stands in for app.db: rows filtered by the window like the real query."""

    def __init__(self, rows, now):
        self.rows, self.now, self.windows = rows, now, []

    def connect(self, s, **kw):
        return self

    def close(self):
        pass

    def db_now(self, conn, utc):
        return self.now

    def db_day_start(self, conn, utc):
        return self.now.replace(hour=0, minute=0, second=0, microsecond=0)

    def fetch_rows(self, conn, s, start, end):
        self.windows.append((start, end))
        return [(u, c) for u, c in self.rows if start <= c < end]


def url(key):
    return f"https://{HOST}/images/{key}"


def put(src, key, size=1024):
    src.put_object(Bucket="images", Key=key, Body=os.urandom(size), ContentType="image/jpeg")


def dst_keys(dst):
    out, token = set(), None
    while True:
        kw = {"ContinuationToken": token} if token else {}
        page = dst.list_objects_v2(Bucket="images-backup", **kw)
        out.update(o["Key"] for o in page.get("Contents", []))
        token = page.get("NextContinuationToken")
        if not token:
            return out


def open_state(tmp_path):
    return State(os.path.join(str(tmp_path), "state.db"))


# ---------------- copier ----------------
def test_copy_then_skip_and_metadata(s3):
    src, dst = s3
    src.put_object(Bucket="images", Key="A/1.jpg", Body=b"x" * 300_000, ContentType="image/jpeg")
    r = copier.copy_object(src, dst, "images", "images-backup", "A/1.jpg")
    assert r.status == copier.COPIED and r.size == 300_000 and r.etag
    head = dst.head_object(Bucket="images-backup", Key="A/1.jpg")
    assert head["ContentType"] == "image/jpeg"
    assert head["Metadata"]["source-bucket"] == "images"
    again = copier.copy_object(src, dst, "images", "images-backup", "A/1.jpg")
    assert again.status == copier.SKIPPED and again.size == 300_000 and again.etag == r.etag


def test_copy_source_missing(s3):
    src, dst = s3
    assert copier.copy_object(src, dst, "images", "images-backup", "nope.jpg").status == \
        copier.SOURCE_MISSING


# ---------------- incremental ----------------
def test_run_first_then_incremental(s3, tmp_path):
    src, dst = s3
    rows = []
    for i in range(20):
        key = f"DOMA/{i}/doc_{i}.jpg"
        put(src, key)
        rows.append((url(key), NOW - timedelta(minutes=60 - i)))
    put(src, "old/x.jpg")
    rows.append((url("old/x.jpg"), NOW - timedelta(days=1)))   # before "today": ignored
    rows.append((url("missing/y.jpg"), NOW - timedelta(minutes=30)))
    rows.append(("not a url", NOW - timedelta(minutes=30)))
    rows.append((url("DOMA/new/late.jpg"), NOW - timedelta(minutes=1)))  # inside safety lag
    put(src, "DOMA/new/late.jpg")

    s = settings(tmp_path)
    fake = FakeDB(rows, NOW)
    assert run_once(s, db_api=fake, src=src, dst=dst) == 0

    keys = dst_keys(dst)
    assert len(keys) == 20 and "old/x.jpg" not in keys and "DOMA/new/late.jpg" not in keys

    st = open_state(tmp_path)
    assert st.get_watermark() == NOW - timedelta(minutes=2)
    assert st.kv_get("start_from") == "2026-10-02T00:00:00"
    assert st.failure_counts() == {"source_missing": 1, "invalid_url": 1}
    st.close()

    # Next tick, 10 minutes later: picks up the late row, doesn't re-copy anything.
    fake.now = NOW + timedelta(minutes=10)
    assert run_once(s, db_api=fake, src=src, dst=dst) == 0
    assert "DOMA/new/late.jpg" in dst_keys(dst) and len(dst_keys(dst)) == 21
    st = open_state(tmp_path)
    last = st.last_runs(1)[0]
    assert last["copied"] == 1 and last["failed"] == 0
    st.close()


def test_result_record_has_size(s3, tmp_path):
    src, dst = s3
    put(src, "A/big.jpg", 750_000)
    put(src, "A/small.jpg", 2_000)
    dst.put_object(Bucket="images-backup", Key="A/pre.jpg", Body=b"z" * 555)  # already mirrored
    put(src, "A/pre.jpg", 555)
    rows = [(url(k), NOW - timedelta(minutes=20)) for k in ("A/big.jpg", "A/small.jpg", "A/pre.jpg")]
    s = settings(tmp_path)
    assert run_once(s, db_api=FakeDB(rows, NOW), src=src, dst=dst) == 0

    db = sqlite3.connect(os.path.join(str(tmp_path), "state.db"))
    got = {k: (size, action, doc_url, created) for k, size, action, doc_url, created in
           db.execute("SELECT key, size, action, doc_url, created FROM results")}
    db.close()
    assert got["A/big.jpg"][:2] == (750_000, "copied")
    assert got["A/small.jpg"][:2] == (2_000, "copied")
    assert got["A/pre.jpg"][:2] == (555, "skipped")
    assert got["A/big.jpg"][2] == url("A/big.jpg")
    assert got["A/big.jpg"][3] == (NOW - timedelta(minutes=20)).isoformat()
    st = open_state(tmp_path)
    assert st.result_totals() == {"objects": 3, "bytes": 752_555, "not_yet_published": 3}
    assert st.last_runs(1)[0]["bytes"] == 752_555
    st.close()


def test_failures_retry_then_succeed(s3, tmp_path):
    src, dst = s3
    put(src, "A/flaky.jpg")
    rows = [(url("A/flaky.jpg"), NOW - timedelta(minutes=5))]
    s = settings(tmp_path)
    fake = FakeDB(rows, NOW)

    real_put, calls = dst.put_object, {"n": 0}

    def flaky_put(**kw):
        calls["n"] += 1
        if calls["n"] == 1:
            raise OSError("connection reset")
        return real_put(**kw)

    dst.put_object = flaky_put
    assert run_once(s, db_api=fake, src=src, dst=dst) == 1
    st = open_state(tmp_path)
    assert st.failure_counts() == {"pending": 1}
    st.close()

    fake.rows = []  # row is outside the next window; must come back via the retry queue
    fake.now = NOW + timedelta(hours=1)
    assert run_once(s, db_api=fake, src=src, dst=dst) == 0
    dst.head_object(Bucket="images-backup", Key="A/flaky.jpg")
    st = open_state(tmp_path)
    assert st.failure_counts() == {}
    st.close()


def test_strict_exit_off_returns_zero_on_copy_failure(s3, tmp_path):
    src, dst = s3
    put(src, "A/f.jpg")

    def broken_put(**kw):
        raise OSError("connection reset")

    dst.put_object = broken_put
    s = settings(tmp_path, strict_exit=False)
    fake = FakeDB([(url("A/f.jpg"), NOW - timedelta(minutes=5))], NOW)
    assert run_once(s, db_api=fake, src=src, dst=dst) == 0
    st = open_state(tmp_path)
    assert st.failure_counts() == {"pending": 1}
    st.close()


def test_overlapping_run_is_skipped(s3, tmp_path):
    src, dst = s3
    s = settings(tmp_path)
    held = RunLock(os.path.join(str(tmp_path), "run.lock"))
    assert held.acquire()
    fake = FakeDB([], NOW)
    assert run_once(s, db_api=fake, src=src, dst=dst) == 0
    assert fake.windows == []  # never touched the DB
    held.release()


# ---------------- backfill ----------------
def test_backfill_runs_to_completion_and_stops(s3, tmp_path):
    src, dst = s3
    rows = []
    for d in range(1, 4):                       # 3 earlier days, 5 objects each
        for i in range(5):
            key = f"hist/{d}/{i}.jpg"
            put(src, key)
            rows.append((url(key), NOW.replace(hour=9) - timedelta(days=d, minutes=i)))
    put(src, "today/a.jpg")
    rows.append((url("today/a.jpg"), NOW - timedelta(minutes=30)))
    put(src, "ancient/z.jpg")
    rows.append((url("ancient/z.jpg"), NOW - timedelta(days=30)))   # before BACKFILL_FROM

    s = settings(tmp_path, backfill_from="2026-09-29T00:00:00", backfill_chunk_minutes=360)
    fake = FakeDB(rows, NOW)
    assert run_once(s, db_api=fake, src=src, dst=dst) == 0

    keys = dst_keys(dst)
    assert len(keys) == 16 and "ancient/z.jpg" not in keys
    st = open_state(tmp_path)
    assert st.kv_get("backfill_done") == "1"
    assert st.kv_get("backfill_cursor") == "2026-10-02T00:00:00"   # stops where incremental began
    run = st.last_runs(1)[0]
    assert run["copied"] == 1 and run["backfill_objects"] == 15
    st.close()

    # A later run must not query the backfill range again.
    fake.windows.clear()
    fake.now = NOW + timedelta(minutes=10)
    run_once(s, db_api=fake, src=src, dst=dst)
    assert all(start >= datetime(2026, 10, 1) for start, _ in fake.windows)


def test_backfill_resumes_when_time_runs_out(s3, tmp_path):
    src, dst = s3
    rows = []
    for i in range(6):
        key = f"hist/{i}.jpg"
        put(src, key)
        rows.append((url(key), datetime(2026, 10, 1, 8 + i, 0)))
    s0 = settings(tmp_path, backfill_from="2026-10-01T00:00:00", max_run_minutes=0)
    fake = FakeDB(rows, NOW)
    run_once(s0, db_api=fake, src=src, dst=dst)                    # no time budget at all
    st = open_state(tmp_path)
    assert st.kv_get("backfill_done") == "0" and st.failure_counts() == {}
    st.close()
    assert dst_keys(dst) == set()

    s1 = settings(tmp_path, backfill_from="2026-10-01T00:00:00")
    fake.now = NOW + timedelta(minutes=10)
    run_once(s1, db_api=fake, src=src, dst=dst)
    assert len(dst_keys(dst)) == 6
    st = open_state(tmp_path)
    assert st.kv_get("backfill_done") == "1"
    st.close()


# ---------------- reconcile ----------------
def test_auto_reconcile_finds_and_repairs_gaps(s3, tmp_path):
    src, dst = s3
    day1 = datetime(2026, 10, 2, 12, 0)
    rows = []
    for i in range(10):
        key = f"d1/{i}.jpg"
        put(src, key, 1000 + i)
        rows.append((url(key), day1 - timedelta(minutes=i + 5)))
    s = settings(tmp_path)
    fake = FakeDB(rows, day1)
    run_once(s, db_api=fake, src=src, dst=dst)
    assert len(dst_keys(dst)) == 10

    # Damage: one object vanishes from MinIO; one row was never seen by the incremental run.
    dst.delete_object(Bucket="images-backup", Key="d1/3.jpg")
    put(src, "d1/late-commit.jpg", 4321)
    rows.append((url("d1/late-commit.jpg"), datetime(2026, 10, 2, 9, 0)))

    fake.now = datetime(2026, 10, 3, 0, 30)          # before RECONCILE_HOUR: nothing yet
    run_once(s, db_api=fake, src=src, dst=dst)
    st = open_state(tmp_path)
    assert st.last_reconciles() == []
    st.close()

    fake.now = datetime(2026, 10, 3, 1, 5)           # reconcile yesterday, then repair
    run_once(s, db_api=fake, src=src, dst=dst)
    st = open_state(tmp_path)
    rep = st.last_reconciles(1)[0]
    st.close()
    assert rep["day"] == "2026-10-02" and rep["db_rows"] == 11 and rep["objects"] == 11
    assert rep["backed_up"] == 9 and rep["missing_in_dest"] == 1 and rep["requeued"] == 2
    assert {"d1/3.jpg", "d1/late-commit.jpg"} <= dst_keys(dst)

    fake.now = datetime(2026, 10, 3, 1, 15)          # confirmation pass: clean
    run_once(s, db_api=fake, src=src, dst=dst)
    st = open_state(tmp_path)
    rep = st.last_reconciles(1)[0]
    assert rep["backed_up"] == 11 and rep["requeued"] == 0 and rep["missing_in_dest"] == 0
    assert rep["bytes"] == sum(1000 + i for i in range(10)) + 4321
    n_reports = len(st.last_reconciles(10))
    st.close()

    fake.now = datetime(2026, 10, 3, 1, 25)          # clean day is not reconciled again
    run_once(s, db_api=fake, src=src, dst=dst)
    st = open_state(tmp_path)
    assert len(st.last_reconciles(10)) == n_reports
    st.close()


def test_reconcile_skips_days_before_mirroring_started(s3, tmp_path):
    src, dst = s3
    put(src, "y/1.jpg")
    rows = [(url("y/1.jpg"), NOW - timedelta(days=1))]
    s = settings(tmp_path)
    run_once(s, db_api=FakeDB(rows, NOW), src=src, dst=dst)   # 17:00, started today
    st = open_state(tmp_path)
    assert st.last_reconciles() == [] and st.failure_counts() == {}
    st.close()
    assert dst_keys(dst) == set()


def test_manual_reconcile_reports_size_mismatch_and_source_missing(s3, tmp_path):
    src, dst = s3
    put(src, "m/ok.jpg", 100)
    put(src, "m/changed.jpg", 200)
    rows = [(url(k), NOW - timedelta(minutes=30)) for k in ("m/ok.jpg", "m/changed.jpg", "m/gone.jpg")]
    s = settings(tmp_path)
    fake = FakeDB(rows, NOW)
    run_once(s, db_api=fake, src=src, dst=dst)
    dst.put_object(Bucket="images-backup", Key="m/changed.jpg", Body=b"tampered")

    rep = reconcile_once(s, datetime(2026, 10, 2), verify=True, db_api=fake, dst=dst)
    assert rep["objects"] == 3 and rep["backed_up"] == 2 and rep["size_mismatch"] == 1
    assert rep["source_missing"] == 1 and rep["requeued"] == 0

    rep = reconcile_once(s, datetime(2026, 10, 2), verify=False, db_api=fake, dst=None)
    assert rep["size_mismatch"] == 0 and rep["verified"] == 0


# ---------------- MSSQL is read-only ----------------
def test_source_database_module_only_selects():
    import re
    from app import db
    src_code = open(db.__file__).read()
    assert not hasattr(db, "write_results")
    for word in ("INSERT", "UPDATE", "DELETE", "MERGE", "CREATE", "ALTER", "DROP", "TRUNCATE", "EXEC"):
        assert not re.search(rf'"[^"\n]*\b{word}\b', src_code), word
    assert "ApplicationIntent=ReadOnly" in src_code


def test_fake_db_is_never_asked_to_write(s3, tmp_path):
    """The runner may only call the read functions of the source DB API."""
    src, dst = s3
    put(src, "ro/1.jpg")

    class ReadOnlyDB(FakeDB):
        def __getattr__(self, name):
            raise AssertionError(f"unexpected source DB call: {name}")

    fake = ReadOnlyDB([(url("ro/1.jpg"), NOW - timedelta(minutes=20))], NOW)
    assert run_once(settings(tmp_path), db_api=fake, src=src, dst=dst) == 0


# ---------------- publishing to Postgres ----------------
class FakeSink:
    def __init__(self):
        self.fail, self.calls = False, 0

    def publish(self, s, state):
        self.calls += 1
        if self.fail:
            raise RuntimeError("postgres down")
        rows = state.unsynced(10_000)
        state.mark_synced([r["key"] for r in rows])
        return {"results": len(rows)}


def test_publish_failure_never_fails_the_run_and_is_retried(s3, tmp_path):
    src, dst = s3
    put(src, "p/1.jpg", 111)
    put(src, "p/2.jpg", 222)
    rows = [(url("p/1.jpg"), NOW - timedelta(minutes=20)), (url("p/2.jpg"), NOW - timedelta(minutes=19))]
    s = settings(tmp_path, pg_host="pg", pg_database="doma-object", pg_user="u", pg_password="p")
    fake, sink = FakeDB(rows, NOW), FakeSink()
    sink.fail = True
    assert run_once(s, db_api=fake, src=src, dst=dst, sink=sink) == 0
    st = open_state(tmp_path)
    assert st.result_totals() == {"objects": 2, "bytes": 333, "not_yet_published": 2}
    st.close()

    sink.fail = False
    fake.now = NOW + timedelta(minutes=10)
    run_once(s, db_api=fake, src=src, dst=dst, sink=sink)
    st = open_state(tmp_path)
    assert st.result_totals()["not_yet_published"] == 0
    st.close()


def test_postgres_off_means_sink_is_not_called(s3, tmp_path):
    src, dst = s3
    sink = FakeSink()
    run_once(settings(tmp_path), db_api=FakeDB([], NOW), src=src, dst=dst, sink=sink)
    assert sink.calls == 0


PG = {k: os.getenv(f"TEST_PG_{k}") for k in ("HOST", "PORT", "DATABASE", "USER", "PASSWORD")}


@pytest.mark.skipif(not PG["HOST"], reason="set TEST_PG_HOST/PORT/DATABASE/USER/PASSWORD to run")
def test_real_postgres_end_to_end(s3, tmp_path):
    import psycopg
    src, dst = s3
    schema = "it_" + os.urandom(4).hex()
    s = settings(tmp_path, pg_host=PG["HOST"], pg_port=int(PG["PORT"] or 5432),
                 pg_database=PG["DATABASE"], pg_user=PG["USER"], pg_password=PG["PASSWORD"],
                 pg_schema=schema)

    def query(sql):
        with psycopg.connect(host=s.pg_host, port=s.pg_port, dbname=s.pg_database,
                             user=s.pg_user, password=s.pg_password) as c:
            return c.execute(sql).fetchall()

    with psycopg.connect(host=s.pg_host, port=s.pg_port, dbname=s.pg_database,
                         user=s.pg_user, password=s.pg_password, autocommit=True) as c:
        c.execute(f'CREATE SCHEMA "{schema}"')
    try:
        day = datetime(2026, 10, 2, 12, 0)
        put(src, "pg/a.jpg", 750_000)
        put(src, "pg/b b.jpg", 2_000)
        rows = [(url("pg/a.jpg"), day - timedelta(minutes=30)),
                (url("pg/b%20b.jpg"), day - timedelta(minutes=29)),
                (url("pg/gone.jpg"), day - timedelta(minutes=28)),
                ("not a url", day - timedelta(minutes=27))]
        fake = FakeDB(rows, day)
        assert run_once(s, db_api=fake, src=src, dst=dst) == 0

        got = query(f'SELECT object_key, object_size, action, doc_url, created_date '
                    f'FROM "{schema}".backup_result ORDER BY object_key')
        assert [(r[0], r[1], r[2]) for r in got] == \
            [("pg/a.jpg", 750_000, "copied"), ("pg/b b.jpg", 2_000, "copied")]
        assert got[0][3] == url("pg/a.jpg") and got[0][4] == day - timedelta(minutes=30)
        assert sorted(query(f'SELECT status FROM "{schema}".backup_failure')) == \
            [("invalid_url",), ("source_missing",)]
        assert query(f'SELECT copied, bytes FROM "{schema}".backup_run') == [(2, 752_000)]

        # Second run: nothing new. No duplicates, one more run row, failures replaced not doubled.
        fake.now = day + timedelta(minutes=10)
        run_once(s, db_api=fake, src=src, dst=dst)
        assert query(f'SELECT count(*), sum(object_size) FROM "{schema}".backup_result') == [(2, 752_000)]
        assert query(f'SELECT count(*) FROM "{schema}".backup_run') == [(2,)]
        assert query(f'SELECT count(*) FROM "{schema}".backup_failure') == [(2,)]

        # Reconcile next day lands in backup_reconcile with the day's total size.
        fake.now = datetime(2026, 10, 3, 1, 5)
        run_once(s, db_api=fake, src=src, dst=dst)
        rec = query(f'SELECT day, db_rows, objects, backed_up, bytes, source_missing, invalid_url, '
                    f'verified FROM "{schema}".backup_reconcile')
        assert rec == [(datetime(2026, 10, 2).date(), 4, 3, 2, 752_000, 1, 1, True)]
    finally:
        with psycopg.connect(host=s.pg_host, port=s.pg_port, dbname=s.pg_database,
                             user=s.pg_user, password=s.pg_password, autocommit=True) as c:
            c.execute(f'DROP SCHEMA "{schema}" CASCADE')


# ---------------- upgrade from v1 state ----------------
def test_v1_state_is_migrated(tmp_path):
    path = os.path.join(str(tmp_path), "state.db")
    db = sqlite3.connect(path)
    db.executescript("""
        CREATE TABLE kv (k TEXT PRIMARY KEY, v TEXT NOT NULL);
        CREATE TABLE done (key TEXT PRIMARY KEY, size INTEGER, done_at TEXT NOT NULL);
        CREATE TABLE runs (id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TEXT, finished_at TEXT, window_start TEXT, window_end TEXT,
            rows INTEGER, copied INTEGER, skipped INTEGER, failed INTEGER,
            source_missing INTEGER, invalid INTEGER, deferred INTEGER, bytes INTEGER);
        INSERT INTO kv VALUES('watermark','2026-09-30T16:58:00');
        INSERT INTO done VALUES('A/1.jpg', 1234, '2026-09-30T16:59:00');
        INSERT INTO runs(started_at) VALUES('2026-09-30T16:50:00');
    """)
    db.commit()
    db.close()
    st = State(path)
    assert st.is_done("A/1.jpg") and st.sizes_for(["A/1.jpg"]) == {"A/1.jpg": 1234}
    assert st.get_watermark() == datetime(2026, 9, 30, 16, 58)
    st.record_run(started_at="x", backfill_objects=3, backfill_bytes=9)
    st.close()
