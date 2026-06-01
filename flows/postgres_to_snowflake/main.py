"""Read rows from a local Postgres database and load them into Snowflake.

Uses the same Snowflake key-pair env vars as dbt (``SNOWFLAKE_*``). See ``.env.example``.
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import pandas as pd
import structlog
from dotenv import load_dotenv
from prefect import flow, task
from snowflake.connector.errors import Error as SnowflakeDriverError
from snowflake.connector.errors import MissingDependencyError
from snowflake.connector.pandas_tools import write_pandas

from flows.postgres_to_snowflake.connections import (
    FORBIDDEN_SNOWFLAKE_LOAD_SCHEMAS,
    normalize_snowflake_database_schema,
    postgres_connect,
    snowflake_connect,
    snowflake_env_connect_params,
    snowflake_table_exists,
)
from flows.postgres_to_snowflake.logging_config import configure_structlog, get_logger


def _env_bool(name: str, default: bool = False) -> bool:
    raw = (os.environ.get(name) or "").strip().lower()
    if not raw:
        return default
    return raw in ("1", "true", "yes", "y", "on")


def _env_int(name: str, default: int) -> int:
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        return max(0, int(raw))
    except ValueError:
        return default


def _load_dotenv_layers() -> None:
    for path in (REPO_ROOT / ".env", REPO_ROOT.parent / ".env"):
        if path.is_file():
            load_dotenv(path, override=False)


def _log_postgres_extract(
    df: pd.DataFrame,
    sql: str,
    *,
    log: structlog.stdlib.BoundLogger,
    sample_rows: int,
) -> None:
    """Log shape, dtypes, SQL, and a row sample so Snowflake can be compared to Postgres."""
    n = len(df.index)
    log.info("postgres_extract_sql", sql_preview=sql[:4000])
    log.info(
        "postgres_extract_result",
        rows=n,
        columns=list(df.columns),
        column_count=len(df.columns),
    )
    if n == 0:
        return
    dtypes = {col: str(dtype) for col, dtype in df.dtypes.items()}
    log.info("postgres_extract_dtypes", dtypes=dtypes)
    if sample_rows > 0:
        preview_n = min(sample_rows, n)
        log.info(
            "postgres_extract_sample",
            preview_rows=preview_n,
            sample=df.head(preview_n).to_dict(orient="records"),
        )


@task(name="read-postgres", retries=1, retry_delay_seconds=15)
def read_postgres(sql: str) -> pd.DataFrame:
    log = get_logger(component="read_postgres", task="read-postgres")
    log.info("read_postgres_start", sql_preview=sql[:200])
    conn = postgres_connect()
    t0 = time.monotonic()
    try:
        df = pd.read_sql_query(sql, conn)
        elapsed = round(time.monotonic() - t0, 3)
        log.info(
            "read_postgres_done",
            rows=len(df),
            columns=len(df.columns),
            duration_s=elapsed,
        )
        return df
    except Exception:
        elapsed = round(time.monotonic() - t0, 3)
        log.exception("read_postgres_failed", duration_s=elapsed)
        raise
    finally:
        conn.close()
        log.debug("postgres_connection_closed")


@task(name="write-snowflake", retries=1, retry_delay_seconds=30)
def write_snowflake(
    df: pd.DataFrame,
    table_name: str,
    *,
    database: str | None,
    schema: str | None,
    auto_create_table: bool,
    overwrite: bool,
) -> int:
    log = get_logger(component="write_snowflake", task="write-snowflake")
    kwargs = snowflake_env_connect_params()
    raw_db = database or kwargs.get("database")
    raw_sch = schema or kwargs.get("schema")
    if not raw_db or not raw_sch:
        raise ValueError(
            "snowflake_database and snowflake_schema must be set "
            "(parameters or SNOWFLAKE_DATABASE / SNOWFLAKE_SCHEMA)."
        )
    db, sch = normalize_snowflake_database_schema(raw_db, raw_sch)
    if sch in FORBIDDEN_SNOWFLAKE_LOAD_SCHEMAS:
        raise ValueError(
            f"Target schema {sch!r} is not valid for loads. Set SNOWFLAKE_SCHEMA to your "
            "warehouse schema (e.g. PUBLIC or the same schema as dbt), not INFORMATION_SCHEMA."
        )
    tbl = table_name.upper()
    target = f"{db}.{sch}.{tbl}"
    rows = len(df.index)

    log = log.bind(target=target, rows_in=rows, auto_create=auto_create_table)

    if rows == 0:
        log.info("write_snowflake_skip", reason="empty dataframe")
        return 0

    log.info("write_snowflake_start", mode="replace" if overwrite else "append")
    conn = snowflake_connect(database=db, schema=sch)
    t0 = time.monotonic()
    try:
        if not auto_create_table and not snowflake_table_exists(
            conn, database=db, schema=sch, table_upper=tbl
        ):
            raise ValueError(
                f"Snowflake table {target} does not exist. "
                "Create it (e.g. dbt) or pass auto_create_table=True."
            )
        try:
            # quote_identifiers=True double-quotes table/column names. If False, Snowflake
            # uppercases unquoted identifiers (e.g. column "name" -> NAME), which hits
            # reserved-word / invalid identifier errors (SQL 000904) during COPY INTO.
            success, num_chunks, nrows_ingested, copy_output = write_pandas(
                conn,
                df,
                tbl,
                database=db,
                schema=sch,
                auto_create_table=auto_create_table,
                overwrite=overwrite,
                quote_identifiers=True,
                chunk_size=min(16_384, rows),
            )
        except MissingDependencyError as exc:
            log.error("write_snowflake_missing_dep", dependency="pyarrow")
            raise RuntimeError(
                "write_pandas needs PyArrow (Snowflake exports DataFrames as Parquet). "
                "Install it in the worker environment: pip install pyarrow "
                "(see requirements.txt)."
            ) from exc
        except SnowflakeDriverError as exc:
            msg = getattr(exc, "msg", None) or str(exc)
            log.error("write_snowflake_driver_error", error=msg)
            raise RuntimeError(
                f"Snowflake rejected the load into {target}: {msg}"
            ) from exc
        if not success:
            log.error(
                "write_snowflake_copy_failed",
                chunks=num_chunks,
                rows_reported=nrows_ingested,
                copy_output=repr(copy_output),
            )
            raise RuntimeError(
                f"Snowflake COPY did not complete successfully for {target} "
                f"(chunks={num_chunks}, rows_reported={nrows_ingested}, "
                f"copy_output={copy_output!r})"
            )
    except Exception:
        elapsed = round(time.monotonic() - t0, 3)
        log.exception("write_snowflake_failed", duration_s=elapsed)
        raise
    finally:
        conn.close()
        log.debug("snowflake_connection_closed")

    loaded = int(nrows_ingested)
    elapsed = round(time.monotonic() - t0, 3)
    mode = "replaced" if overwrite else "appended"
    log.info(
        "write_snowflake_done",
        rows_loaded=loaded,
        chunks=num_chunks,
        mode=mode,
        duration_s=elapsed,
    )
    return loaded


_DEFAULT_POSTGRES_SQL = "SELECT * FROM users;"
_DEFAULT_SNOWFLAKE_TABLE = "POSTGRES_SNAPSHOT"


@flow(name="postgres-to-snowflake")
def run_postgres_to_snowflake_flow(
    postgres_sql: str | None = None,
    snowflake_table: str | None = None,
    snowflake_database: str | None = None,
    snowflake_schema: str | None = None,
    auto_create_table: bool = True,
    replace_snowflake_table: bool | None = None,
    log_sample_rows: int | None = None,
) -> int:
    """Run ``postgres_sql`` on Postgres and load into ``snowflake_table``.

    If ``postgres_sql`` or ``snowflake_table`` are omitted, values come from
    ``POSTGRES_COPY_SQL`` / ``SNOWFLAKE_LOAD_TABLE`` after loading ``.env``,
    then built-in exploration defaults. The built-in default SQL is only a
    **one-row probe** (`SELECT 1 ...`); set ``POSTGRES_COPY_SQL`` to your real
    ``SELECT`` so Snowflake receives the same rows as your source query.

    **Keeping Snowflake aligned with Postgres**

    - **Append (default):** each run **adds** rows. The Snowflake table grows and
      will not match Postgres row-for-row unless you truncate elsewhere.
    - **Replace:** set ``replace_snowflake_table=True`` or env
      ``SNOWFLAKE_LOAD_REPLACE=true`` so this run **overwrites** the target table
      with the current extract only (mirror a snapshot).

    Logs include the SQL, row count, dtypes, and a sample of Postgres rows
    (see ``POSTGRES_LOG_SAMPLE_ROWS``).
    """
    _load_dotenv_layers()
    configure_structlog()
    log = get_logger(component="flow", pipeline="postgres-to-snowflake")

    sql = postgres_sql or os.environ.get("POSTGRES_COPY_SQL", _DEFAULT_POSTGRES_SQL)
    table = snowflake_table or os.environ.get(
        "SNOWFLAKE_LOAD_TABLE",
        _DEFAULT_SNOWFLAKE_TABLE,
    )
    sample = (
        log_sample_rows
        if log_sample_rows is not None
        else _env_int("POSTGRES_LOG_SAMPLE_ROWS", 15)
    )
    replace = (
        replace_snowflake_table
        if replace_snowflake_table is not None
        else _env_bool("SNOWFLAKE_LOAD_REPLACE", False)
    )

    log.info(
        "flow_start",
        snowflake_table=table,
        replace=replace,
        auto_create_table=auto_create_table,
        sample_rows=sample,
    )

    df = read_postgres(sql)
    _log_postgres_extract(df, sql, log=log, sample_rows=sample)

    rows_loaded = write_snowflake(
        df,
        table,
        database=snowflake_database,
        schema=snowflake_schema,
        auto_create_table=auto_create_table,
        overwrite=replace,
    )

    log.info("flow_done", rows_loaded=rows_loaded)
    return rows_loaded


if __name__ == "__main__":
    _load_dotenv_layers()
    configure_structlog()
    run_postgres_to_snowflake_flow()
