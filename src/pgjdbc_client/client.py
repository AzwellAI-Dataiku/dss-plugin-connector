# python-lib/pgjdbc_client/client.py
# PostgreSQL JDBC helper for Dataiku plugin (jaydebeapi)

from __future__ import annotations

from typing import Generator, List, Optional, Tuple

import jaydebeapi

PG_DRIVER_CLASS = "org.postgresql.Driver"


def build_jdbc_url(host: str, port: int, database: str, sslmode: str = "disable") -> str:
    """
    Build PostgreSQL JDBC URL.
    Example:
      jdbc:postgresql://127.0.0.1:5432/dataiku?sslmode=disable
    """
    host = host.strip()
    database = database.strip()
    sslmode = (sslmode or "disable").strip()
    return f"jdbc:postgresql://{host}:{int(port)}/{database}?sslmode={sslmode}"


def connect(jdbc_url: str, jars: List[str], username: str, password: Optional[str] = None):
    """
    Create a JDBC connection using jaydebeapi.
    - If password is None, uses empty string (works with trust/peer/pg_hba setups)
    """
    pw = "" if password is None else str(password)
    return jaydebeapi.connect(PG_DRIVER_CLASS, jdbc_url, [username, pw], jars)


def list_schemas(conn) -> List[str]:
    """
    List non-system schemas (exclude pg_* and information_schema).
    """
    sql = """
      SELECT schema_name
      FROM information_schema.schemata
      WHERE schema_name NOT LIKE 'pg_%'
        AND schema_name <> 'information_schema'
      ORDER BY schema_name
    """
    cur = conn.cursor()
    try:
        cur.execute(sql)
        rows = cur.fetchall()
        return [r[0] for r in rows]
    finally:
        cur.close()


def list_tables(conn, schema: str) -> List[str]:
    """
    List base tables in a given schema.
    jaydebeapi uses DB-API style placeholders: '?'
    """
    sql = """
      SELECT table_name
      FROM information_schema.tables
      WHERE table_schema = ?
        AND table_type = 'BASE TABLE'
      ORDER BY table_name
    """
    cur = conn.cursor()
    try:
        cur.execute(sql, [schema])
        rows = cur.fetchall()
        return [r[0] for r in rows]
    finally:
        cur.close()


def iter_rows(
    conn,
    schema: str,
    table: str,
    fetch_size: int = 5000,
    row_limit: Optional[int] = None,
) -> Generator[Tuple[List[str], List[Tuple]], None, None]:
    """
    Stream rows from schema.table in batches.

    Yields: (colnames, batch_rows)
      - colnames: list of column names
      - batch_rows: list of tuples
    """
    # identifiers: keep it simple but safe
    if not schema or not table:
        raise ValueError("schema/table is required")

    sql = f'SELECT * FROM "{schema}"."{table}"'
    if row_limit is not None and int(row_limit) > 0:
        sql += f" LIMIT {int(row_limit)}"

    cur = conn.cursor()
    try:
        cur.execute(sql)

        # column names
        desc = cur.description or []
        colnames = [d[0] for d in desc]

        # fetch in batches
        fs = max(1, int(fetch_size))
        while True:
            batch = cur.fetchmany(fs)
            if not batch:
                break
            yield colnames, batch
    finally:
        cur.close()
