"""Environment-driven settings. Every value can be set via env var; secrets also via *_FILE."""
from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Union

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _env(name: str, default: Optional[str] = None, required: bool = False) -> Optional[str]:
    val = os.getenv(name)
    if val is None or val == "":
        file_path = os.getenv(f"{name}_FILE")
        if file_path:
            with open(file_path, "r", encoding="utf-8") as fh:
                val = fh.read().strip()
    if val is None or val == "":
        val = default
    if required and (val is None or val == ""):
        raise SystemExit(f"Missing required configuration: {name} (or {name}_FILE)")
    return val


def _bool(name: str, default: bool) -> bool:
    val = os.getenv(name)
    if val is None or val == "":
        return default
    return val.strip().lower() in ("1", "true", "yes", "on")


def _int(name: str, default: int) -> int:
    val = os.getenv(name)
    return int(val) if val not in (None, "") else default


def _dt(name: str) -> Optional[str]:
    """Optional ISO datetime; validated here so a typo fails before any work starts."""
    val = _env(name)
    if not val:
        return None
    try:
        datetime.fromisoformat(val)
    except ValueError:
        raise SystemExit(f"{name} must be an ISO datetime like 2026-01-01T00:00:00, got {val!r}")
    return val


def _verify(ca_name: str, verify_name: str) -> Union[bool, str]:
    """TLS verification for boto3: a CA bundle path, True (system CAs) or False."""
    ca = _env(ca_name)
    if ca:
        return ca
    return _bool(verify_name, True)


def quote_identifier(name: str) -> str:
    """Validate 'schema.table' / 'column' and return it bracket-quoted for T-SQL."""
    parts = name.split(".")
    if not 1 <= len(parts) <= 3 or not all(_IDENT.match(p) for p in parts):
        raise SystemExit(f"Invalid SQL identifier: {name!r}")
    return ".".join(f"[{p}]" for p in parts)


@dataclass(frozen=True)
class Settings:
    # --- MSSQL (source of docUrl rows) ---
    mssql_host: str
    mssql_port: int
    mssql_database: str
    mssql_user: str
    mssql_password: str
    mssql_encrypt: bool
    mssql_trust_server_cert: bool
    mssql_query_timeout: int
    source_table: str
    col_doc_url: str
    col_created: str
    created_date_is_utc: bool

    # --- Source (ODF S3) ---
    src_endpoint: str
    src_access_key: str
    src_secret_key: str
    src_region: str
    src_bucket: str
    src_url_host: Optional[str]
    src_verify: Union[bool, str]

    # --- Destination (MinIO) ---
    dst_endpoint: str
    dst_access_key: str
    dst_secret_key: str
    dst_region: str
    dst_bucket: str
    dst_prefix: str
    dst_verify: Union[bool, str]

    # --- Runtime ---
    state_dir: str
    workers: int
    overlap_minutes: int
    safety_lag_minutes: int
    start_from: Optional[str]
    max_attempts: int
    retry_batch: int
    max_run_minutes: int
    strict_exit: bool
    log_level: str

    # --- Result records ---
    result_retention_days: int            # 0 = keep forever in state.db

    # --- PostgreSQL result store (optional; enabled when pg_host is set) ---
    pg_host: Optional[str]
    pg_port: int
    pg_database: Optional[str]
    pg_user: Optional[str]
    pg_password: Optional[str]
    pg_schema: str
    pg_sslmode: str

    # --- Backfill ---
    backfill_from: Optional[str]
    backfill_to: Optional[str]
    backfill_chunk_minutes: int
    backfill_workers: int

    # --- Reconcile ---
    reconcile_enabled: bool
    reconcile_hour: int
    reconcile_verify_dest: bool

    @property
    def sql_table(self) -> str:
        return quote_identifier(self.source_table)

    @property
    def sql_col_url(self) -> str:
        return quote_identifier(self.col_doc_url)

    @property
    def sql_col_created(self) -> str:
        return quote_identifier(self.col_created)

    @property
    def pg_enabled(self) -> bool:
        return bool(self.pg_host)


def load_settings() -> Settings:
    s = Settings(
        mssql_host=_env("MSSQL_HOST", required=True),
        mssql_port=_int("MSSQL_PORT", 1433),
        mssql_database=_env("MSSQL_DATABASE", required=True),
        mssql_user=_env("MSSQL_USER", required=True),
        mssql_password=_env("MSSQL_PASSWORD", required=True),
        mssql_encrypt=_bool("MSSQL_ENCRYPT", True),
        mssql_trust_server_cert=_bool("MSSQL_TRUST_SERVER_CERT", False),
        mssql_query_timeout=_int("MSSQL_QUERY_TIMEOUT", 120),
        source_table=_env("SOURCE_TABLE", required=True),
        col_doc_url=_env("COL_DOC_URL", "docUrl"),
        col_created=_env("COL_CREATED", "createdDate"),
        created_date_is_utc=_bool("CREATED_DATE_IS_UTC", False),
        src_endpoint=_env("SRC_S3_ENDPOINT", required=True),
        src_access_key=_env("SRC_S3_ACCESS_KEY", required=True),
        src_secret_key=_env("SRC_S3_SECRET_KEY", required=True),
        src_region=_env("SRC_S3_REGION", "us-east-1"),
        src_bucket=_env("SRC_S3_BUCKET", "images"),
        src_url_host=_env("SRC_URL_HOST"),
        src_verify=_verify("SRC_S3_CA_BUNDLE", "SRC_S3_VERIFY_TLS"),
        dst_endpoint=_env("DST_S3_ENDPOINT", required=True),
        dst_access_key=_env("DST_S3_ACCESS_KEY", required=True),
        dst_secret_key=_env("DST_S3_SECRET_KEY", required=True),
        dst_region=_env("DST_S3_REGION", "us-east-1"),
        dst_bucket=_env("DST_S3_BUCKET", required=True),
        dst_prefix=(_env("DST_S3_PREFIX", "") or "").lstrip("/"),
        dst_verify=_verify("DST_S3_CA_BUNDLE", "DST_S3_VERIFY_TLS"),
        state_dir=_env("STATE_DIR", "/data"),
        workers=_int("WORKERS", 16),
        overlap_minutes=_int("OVERLAP_MINUTES", 15),
        safety_lag_minutes=_int("SAFETY_LAG_MINUTES", 2),
        start_from=_dt("START_FROM"),
        max_attempts=_int("MAX_ATTEMPTS", 10),
        retry_batch=_int("RETRY_BATCH", 5000),
        max_run_minutes=_int("MAX_RUN_MINUTES", 9),
        strict_exit=_bool("STRICT_EXIT", True),
        log_level=_env("LOG_LEVEL", "INFO"),
        result_retention_days=_int("RESULT_RETENTION_DAYS", 0),
        pg_host=_env("PG_HOST"),
        pg_port=_int("PG_PORT", 5432),
        pg_database=_env("PG_DATABASE"),
        pg_user=_env("PG_USER"),
        pg_password=_env("PG_PASSWORD"),
        pg_schema=_env("PG_SCHEMA", "public"),
        pg_sslmode=_env("PG_SSLMODE", "prefer"),
        backfill_from=_dt("BACKFILL_FROM"),
        backfill_to=_dt("BACKFILL_TO"),
        backfill_chunk_minutes=max(1, _int("BACKFILL_CHUNK_MINUTES", 60)),
        backfill_workers=max(1, _int("BACKFILL_WORKERS", 8)),
        reconcile_enabled=_bool("RECONCILE_ENABLED", True),
        reconcile_hour=_int("RECONCILE_HOUR", 1),
        reconcile_verify_dest=_bool("RECONCILE_VERIFY_DEST", True),
    )
    # Validate identifiers early so a bad config fails before touching anything.
    _ = (s.sql_table, s.sql_col_url, s.sql_col_created)
    if s.pg_enabled:
        missing = [n for n, v in (("PG_DATABASE", s.pg_database), ("PG_USER", s.pg_user),
                                  ("PG_PASSWORD", s.pg_password)) if not v]
        if missing:
            raise SystemExit(f"PG_HOST is set but {', '.join(missing)} is missing")
        if not _IDENT.match(s.pg_schema):
            raise SystemExit(f"Invalid PG_SCHEMA: {s.pg_schema!r}")
    return s
