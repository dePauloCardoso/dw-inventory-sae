# db.py
from __future__ import annotations
from typing import List, Dict, Iterable, Any
import logging

import psycopg
from psycopg.rows import dict_row
from psycopg import sql

from config import get_database_config

logger = logging.getLogger(__name__)


def get_connection() -> psycopg.Connection:
    cfg = get_database_config()
    conn = psycopg.connect(
        host=cfg["host"],
        port=cfg["port"],
        user=cfg["user"],
        password=cfg["password"],
        dbname=cfg["database"],
        autocommit=False,
        row_factory=dict_row,
    )
    return conn


def _make_upsert_query(table: str, cols: List[str], pk: str = "id") -> sql.SQL:
    """
    Build a safe SQL query for upsert using psycopg.sql
    Supports schema-qualified table names like 'public.raw_inventory'
    """
    if "." in table:
        schema, tbl = table.split(".", 1)
        table_ident = sql.SQL("{}.{}").format(
            sql.Identifier(schema), sql.Identifier(tbl)
        )
    else:
        table_ident = sql.Identifier(table)

    col_identifiers = [sql.Identifier(c) for c in cols]
    placeholders = [sql.Placeholder(c) for c in cols]

    updates = [
        sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c))
        for c in cols
        if c != pk
    ]

    query = sql.SQL(
        "INSERT INTO {table} ({fields}) VALUES ({values}) "
        "ON CONFLICT ({pk}) DO UPDATE SET {updates}"
    ).format(
        table=table_ident,
        fields=sql.SQL(", ").join(col_identifiers),
        values=sql.SQL(", ").join(placeholders),
        pk=sql.Identifier(pk),
        updates=sql.SQL(", ").join(updates),
    )
    return query


def upsert_table(
    conn: psycopg.Connection,
    table: str,
    rows: Iterable[Dict[str, Any]],
    chunk_size: int = 500,
    pk: str = "id",
) -> int:
    items = [r for r in rows]
    if not items:
        logger.debug("[db] upsert_table: no rows to upsert")
        return 0

    cols = list(items[0].keys())
    for r in items:
        # ensure all rows have the same keys
        for c in cols:
            r.setdefault(c, None)

    query = _make_upsert_query(table, cols, pk=pk)
    total = 0
    try:
        with conn.cursor() as cur:
            for chunk in (
                items[i : i + chunk_size] for i in range(0, len(items), chunk_size)
            ):
                cur.executemany(query.as_string(conn), chunk)
                total += len(chunk)
        conn.commit()
        logger.info(f"[db] upserted {total} rows into {table}")
    except Exception:
        conn.rollback()
        logger.exception("[db] upsert_table failed")
        raise
    return total


# convenience wrapper for inventory (keeps compatibility with previous code)
def upsert_inventory(
    conn: psycopg.Connection, rows: Iterable[Dict[str, Any]], chunk_size: int = 500
) -> int:
    return upsert_table(
        conn, "public.raw_inventory", rows, chunk_size=chunk_size, pk="id"
    )


def upsert_order_hdr(conn, rows, chunk_size: int = 500) -> int:
    return upsert_table(
        conn, "public.raw_order_hdr", rows, chunk_size=chunk_size, pk="id"
    )


def upsert_order_dtl(conn, rows, chunk_size: int = 500) -> int:
    return upsert_table(
        conn, "public.raw_order_dtl", rows, chunk_size=chunk_size, pk="id"
    )


def upsert_container(conn, rows, chunk_size: int = 500) -> int:
    return upsert_table(
        conn, "public.raw_container", rows, chunk_size=chunk_size, pk="id"
    )


def upsert_location(conn, rows, chunk_size: int = 500) -> int:
    return upsert_table(
        conn, "public.raw_location", rows, chunk_size=chunk_size, pk="id"
    )
