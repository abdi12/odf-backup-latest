"""MSSQL access. Strictly read-only: this module contains SELECT statements only.

The service never creates, alters or writes anything in the source database.
Give it a login that only has SELECT on the document table.
"""
from __future__ import annotations

from datetime import datetime
from typing import List, Tuple

from .config import Settings


def connect(s: Settings):
    import pyodbc  # imported lazily so tests run without the ODBC driver

    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={s.mssql_host},{s.mssql_port};"
        f"DATABASE={s.mssql_database};"
        f"UID={s.mssql_user};PWD={{{s.mssql_password.replace('}', '}}')}}};"
        f"Encrypt={'yes' if s.mssql_encrypt else 'no'};"
        f"TrustServerCertificate={'yes' if s.mssql_trust_server_cert else 'no'};"
        "ApplicationIntent=ReadOnly;"
        "APP=odf-backup;"
    )
    conn = pyodbc.connect(conn_str, timeout=30, autocommit=True, readonly=True)
    conn.timeout = s.mssql_query_timeout
    return conn


def db_now(conn, utc: bool) -> datetime:
    """Use the DB clock so container clock skew can't open gaps in the window."""
    cur = conn.cursor()
    cur.execute("SELECT SYSUTCDATETIME()" if utc else "SELECT SYSDATETIME()")
    return cur.fetchone()[0]


def db_day_start(conn, utc: bool) -> datetime:
    fn = "SYSUTCDATETIME()" if utc else "SYSDATETIME()"
    cur = conn.cursor()
    cur.execute(f"SELECT CAST(CAST({fn} AS date) AS datetime2)")
    return cur.fetchone()[0]


def fetch_rows(conn, s: Settings, start: datetime, end: datetime) -> List[Tuple[str, datetime]]:
    """All (docUrl, createdDate) with start <= createdDate < end.

    Rows are loaded fully (a day is ~100k short rows) so the DB connection is
    released before the slow copy phase starts.
    """
    sql = (
        f"SELECT {s.sql_col_url}, {s.sql_col_created} FROM {s.sql_table} "
        f"WHERE {s.sql_col_created} >= ? AND {s.sql_col_created} < ? "
        f"AND {s.sql_col_url} IS NOT NULL "
        f"ORDER BY {s.sql_col_created}"
    )
    cur = conn.cursor()
    cur.execute(sql, start, end)
    rows: List[Tuple[str, datetime]] = []
    while True:
        batch = cur.fetchmany(5000)
        if not batch:
            break
        rows.extend((r[0], r[1]) for r in batch)
    return rows
