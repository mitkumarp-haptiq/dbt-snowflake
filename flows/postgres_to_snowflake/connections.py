"""Snowflake and Postgres connections built from environment variables.

Secrets and endpoints come from ``SNOWFLAKE_*`` / ``POSTGRES_*`` (see ``.env.example``).
Callers should load dotenv before opening connections.
"""

from __future__ import annotations

import os
from typing import Any

import psycopg2
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from snowflake.connector import connect as _snowflake_connector_connect

from flows.postgres_to_snowflake.logging_config import get_logger

log = get_logger(component="connections")

# Schemas that must not be used as load targets (misconfiguration).
FORBIDDEN_SNOWFLAKE_LOAD_SCHEMAS = frozenset({"INFORMATION_SCHEMA"})


def snowflake_private_key_der_from_env() -> bytes:
    """RSA private key as DER PKCS8 bytes (Snowflake key-pair auth)."""
    path = os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"]
    passphrase_env = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE", "")
    passphrase: bytes | None = (
        passphrase_env.encode() if passphrase_env else None
    )
    with open(path, "rb") as f:
        p_key = serialization.load_pem_private_key(
            f.read(),
            password=passphrase,
            backend=default_backend(),
        )
    return p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def snowflake_env_connect_params() -> dict[str, Any]:
    """Common Snowflake connector kwargs from env (includes session defaults)."""
    return {
        "user": os.environ["SNOWFLAKE_USER"],
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "private_key": snowflake_private_key_der_from_env(),
        "role": os.environ.get("SNOWFLAKE_ROLE"),
        "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE"),
        "database": os.environ.get("SNOWFLAKE_DATABASE"),
        "schema": os.environ.get("SNOWFLAKE_SCHEMA"),
    }


def snowflake_connect(*, database: str, schema: str):
    """Open a Snowflake connection with key-pair auth and explicit database/schema."""
    kwargs = snowflake_env_connect_params()
    log.info(
        "snowflake_connect",
        account=kwargs["account"],
        user=kwargs["user"],
        database=database,
        schema=schema,
        warehouse=kwargs.get("warehouse"),
        role=kwargs.get("role"),
    )
    conn = _snowflake_connector_connect(
        user=kwargs["user"],
        account=kwargs["account"],
        private_key=kwargs["private_key"],
        warehouse=kwargs.get("warehouse"),
        database=database,
        schema=schema,
        role=kwargs.get("role"),
    )
    log.info("snowflake_connected", database=database, schema=schema)
    return conn


def normalize_snowflake_database_schema(database: str, schema: str) -> tuple[str, str]:
    """Uppercase database and schema for Snowflake with quoted identifiers."""
    return database.strip().upper(), schema.strip().upper()


def snowflake_table_exists(
    conn,
    *,
    database: str,
    schema: str,
    table_upper: str,
) -> bool:
    """Return True if ``database.schema.table`` exists (case-insensitive match)."""
    sql = """
        SELECT 1
        FROM INFORMATION_SCHEMA.TABLES
        WHERE UPPER(TABLE_CATALOG) = UPPER(%s)
          AND UPPER(TABLE_SCHEMA) = UPPER(%s)
          AND UPPER(TABLE_NAME) = UPPER(%s)
        LIMIT 1
    """
    with conn.cursor() as cur:
        cur.execute(sql, (database, schema, table_upper))
        return cur.fetchone() is not None


def postgres_connect():
    """Postgres connection from ``POSTGRES_*`` env vars."""
    host = os.environ["POSTGRES_HOST"]
    port = int(os.environ.get("POSTGRES_PORT", "5432"))
    user = os.environ["POSTGRES_USER"]
    dbname = os.environ["POSTGRES_DB"]
    log.info("postgres_connect", host=host, port=port, user=user, dbname=dbname)
    conn = psycopg2.connect(
        host=host,
        port=port,
        user=user,
        password=os.environ.get("POSTGRES_PASSWORD", ""),
        dbname=dbname,
    )
    log.info("postgres_connected", host=host, dbname=dbname)
    return conn
