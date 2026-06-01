"""Drain CDC events from Redpanda and bulk-MERGE them into Snowflake.

Architecture:
    Postgres -> Debezium -> Redpanda -> Prefect-triggered THIS FLOW -> Snowflake

Cost model:
    One Snowflake warehouse spin-up per scheduled run, not per event. The flow
    stages a deduplicated batch into a temp table (single COPY INTO via
    `write_pandas`), then a single MERGE applies upserts and a single DELETE
    applies removals.

Safety:
    Kafka offsets are committed only after Snowflake confirms success, so any
    failure mid-flow is replayed on the next run.
"""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import pandas as pd
from confluent_kafka import Consumer, KafkaError, KafkaException
from dotenv import load_dotenv
from prefect import flow, task
from snowflake.connector.pandas_tools import write_pandas

from flows.postgres_to_snowflake.connections import (
    FORBIDDEN_SNOWFLAKE_LOAD_SCHEMAS,
    normalize_snowflake_database_schema,
    snowflake_connect,
    snowflake_env_connect_params,
)
from flows.postgres_to_snowflake.logging_config import configure_structlog, get_logger


def _load_dotenv_layers() -> None:
    for path in (REPO_ROOT / ".env", REPO_ROOT.parent / ".env"):
        if path.is_file():
            load_dotenv(path, override=False)


def _env_int(name: str, default: int) -> int:
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        return max(0, int(raw))
    except ValueError:
        return default


def _cdc_config() -> dict[str, Any]:
    return {
        "bootstrap_servers": os.environ.get("CDC_KAFKA_BOOTSTRAP", "localhost:9092"),
        # Must match Debezium topic.prefix + captured table (see docker-compose /
        # docker/debezium/application.properties — default capture is public.users).
        "topic": os.environ.get("CDC_KAFKA_TOPIC", "app.public.users"),
        "group_id": os.environ.get("CDC_KAFKA_GROUP_ID", "cdc-snowflake-loader"),
        "snowflake_table": os.environ.get("CDC_SNOWFLAKE_TABLE", "CDC_CUSTOMERS"),
        "pk_column": os.environ.get("CDC_PRIMARY_KEY_COLUMN", "id"),
        "drain_max_seconds": _env_int("CDC_DRAIN_MAX_SECONDS", 90),
        "drain_idle_seconds": _env_int("CDC_DRAIN_IDLE_SECONDS", 5),
        "drain_max_messages": _env_int("CDC_DRAIN_MAX_MESSAGES", 50_000),
    }


def _resolve_snowflake_target() -> tuple[str, str, str]:
    cfg = _cdc_config()
    params = snowflake_env_connect_params()
    raw_db = os.environ.get("SNOWFLAKE_DATABASE") or params.get("database", "")
    raw_sch = os.environ.get("SNOWFLAKE_SCHEMA") or params.get("schema", "")
    if not raw_db or not raw_sch:
        raise ValueError(
            "SNOWFLAKE_DATABASE and SNOWFLAKE_SCHEMA must be set for CDC target."
        )
    db, sch = normalize_snowflake_database_schema(raw_db, raw_sch)
    if sch in FORBIDDEN_SNOWFLAKE_LOAD_SCHEMAS:
        raise ValueError(f"Target schema {sch!r} is forbidden.")
    return db, sch, str(cfg["snowflake_table"]).upper()


def _drain_messages(
    consumer: Consumer,
    *,
    pk_column: str,
    max_seconds: int,
    idle_seconds: int,
    max_messages: int,
    log,
) -> list[dict[str, Any]]:
    """Pull all available messages until idle, time-cap, or count-cap reached."""
    messages: list[dict[str, Any]] = []
    start = time.monotonic()
    last_message_at = start

    while True:
        elapsed = time.monotonic() - start
        idle = time.monotonic() - last_message_at
        if elapsed >= max_seconds:
            log.info("drain_stop", reason="max_seconds", elapsed_s=round(elapsed, 2))
            break
        if idle >= idle_seconds and messages:
            log.info("drain_stop", reason="idle", idle_s=round(idle, 2))
            break
        if len(messages) >= max_messages:
            log.info("drain_stop", reason="max_messages", count=len(messages))
            break

        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            raise KafkaException(msg.error())

        try:
            payload = json.loads(msg.value())
        except (json.JSONDecodeError, TypeError):
            log.exception("drain_parse_error", offset=msg.offset())
            continue

        # Debezium commonly places primary-key fields in the Kafka message key.
        # If the PK is absent in the value payload, hydrate it from the key.
        if pk_column not in payload:
            raw_key = msg.key()
            if raw_key:
                try:
                    key_obj = json.loads(raw_key)
                except (json.JSONDecodeError, TypeError):
                    key_obj = None
                if isinstance(key_obj, dict) and pk_column in key_obj:
                    payload[pk_column] = key_obj[pk_column]

        messages.append(payload)
        last_message_at = time.monotonic()

    return messages


def _events_to_dataframes(
    events: list[dict[str, Any]], pk_column: str
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return (upserts_df, deletes_df), deduped to last-event-per-pk by ts_ms."""
    if not events:
        empty = pd.DataFrame()
        return empty, empty

    df = pd.DataFrame(events)
    if pk_column not in df.columns:
        raise ValueError(
            f"Primary key column {pk_column!r} not found; "
            f"event columns: {list(df.columns)}"
        )

    df["__ts_ms"] = (
        pd.to_numeric(df.get("__ts_ms"), errors="coerce").fillna(0).astype("int64")
    )
    df = df.sort_values("__ts_ms").drop_duplicates(subset=[pk_column], keep="last")

    op_col = df.get("__op", pd.Series(["c"] * len(df), index=df.index))
    deletes = df.loc[op_col == "d", [pk_column]].copy()
    upserts = df.loc[op_col != "d"].drop(
        columns=[c for c in df.columns if c.startswith("__")],
        errors="ignore",
    )
    return upserts, deletes


def _ensure_target_table(
    conn,
    sample_row: dict[str, Any],
    pk_column: str,
    database: str,
    schema: str,
    table: str,
    log,
) -> None:
    """Auto-create CDC target table on first run, columns inferred as VARIANT."""
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT 1 FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_CATALOG = UPPER(%s)
              AND TABLE_SCHEMA  = UPPER(%s)
              AND TABLE_NAME    = UPPER(%s)
            """,
            (database, schema, table),
        )
        if cur.fetchone() is not None:
            return
        columns = [k for k in sample_row if not k.startswith("__")]
        if pk_column not in columns:
            raise ValueError(f"PK {pk_column!r} not in event keys: {columns}")
        col_defs = ",\n    ".join(f'"{c}" VARIANT' for c in columns)
        ddl = f'CREATE TABLE "{table}" (\n    {col_defs}\n)'
        log.info("ensure_target_create", ddl=ddl)
        cur.execute(ddl)
    finally:
        cur.close()


def _apply_to_snowflake(
    upserts: pd.DataFrame,
    deletes: pd.DataFrame,
    pk_column: str,
    database: str,
    schema: str,
    table: str,
    sample_row: dict[str, Any] | None,
    log,
) -> dict[str, int]:
    """One warehouse session: stage upserts, MERGE, DELETE removed pks."""
    n_up = len(upserts.index)
    n_del = len(deletes.index)
    if n_up == 0 and n_del == 0:
        log.info("apply_skip", reason="empty")
        return {"upserts": 0, "deletes": 0}

    conn = snowflake_connect(database=database, schema=schema)
    t0 = time.monotonic()
    try:
        if sample_row is not None:
            _ensure_target_table(conn, sample_row, pk_column, database, schema, table, log)

        cur = conn.cursor()
        try:
            if n_up > 0:
                staging_table = f"{table}_STG_{int(time.time() * 1000)}"
                cols = list(upserts.columns)
                col_defs = ", ".join(f'"{c}" STRING' for c in cols)
                cur.execute(f'CREATE TEMP TABLE "{staging_table}" ({col_defs})')

                stringified = upserts.astype(object).where(pd.notna(upserts), None)
                stringified = stringified.applymap(
                    lambda v: json.dumps(v) if v is not None else None
                )
                success, _, nrows, _ = write_pandas(
                    conn,
                    stringified,
                    staging_table,
                    database=database,
                    schema=schema,
                    auto_create_table=False,
                    quote_identifiers=True,
                    chunk_size=min(16_384, n_up),
                )
                if not success:
                    raise RuntimeError("write_pandas failed for staging table")

                set_list = ", ".join(
                    f'target."{c}" = parse_json(source."{c}")'
                    for c in cols
                    if c != pk_column
                )
                col_list = ", ".join(f'"{c}"' for c in cols)
                insert_vals = ", ".join(
                    f'parse_json(source."{c}")' for c in cols
                )
                merge_sql = f"""
                    MERGE INTO "{table}" AS target
                    USING "{staging_table}" AS source
                       ON target."{pk_column}" = parse_json(source."{pk_column}")
                    WHEN MATCHED THEN UPDATE SET {set_list}
                    WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES ({insert_vals})
                """
                cur.execute(merge_sql)
                log.info("merge_done", upserts=int(nrows))

            if n_del > 0:
                pks = [json.dumps(v) for v in deletes[pk_column].tolist()]
                placeholders = ", ".join(["%s"] * len(pks))
                cur.execute(
                    f'DELETE FROM "{table}" '
                    f'WHERE TO_VARCHAR("{pk_column}") IN ({placeholders})',
                    [str(json.loads(p)) for p in pks],
                )
                log.info("delete_done", deletes=cur.rowcount)
        finally:
            cur.close()

        log.info(
            "apply_done",
            upserts=n_up,
            deletes=n_del,
            duration_s=round(time.monotonic() - t0, 3),
        )
        return {"upserts": n_up, "deletes": n_del}
    finally:
        conn.close()


@task(name="drain-and-load", retries=1, retry_delay_seconds=30)
def drain_and_load() -> dict[str, int]:
    """Atomic unit: open consumer -> drain -> apply to Snowflake -> commit offsets.

    Offsets are committed ONLY after Snowflake confirms success. On failure,
    the consumer closes without commit and the next run re-processes the same
    messages (at-least-once delivery; the MERGE makes processing idempotent).
    """
    log = get_logger(component="drain_and_load", task="drain-and-load")
    cfg = _cdc_config()
    db, sch, tbl = _resolve_snowflake_target()

    consumer = Consumer(
        {
            "bootstrap.servers": cfg["bootstrap_servers"],
            "group.id": cfg["group_id"],
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "session.timeout.ms": 30_000,
        }
    )
    consumer.subscribe([cfg["topic"]])
    log.info(
        "drain_start",
        topic=cfg["topic"],
        target=f"{db}.{sch}.{tbl}",
    )

    try:
        events = _drain_messages(
            consumer,
            pk_column=str(cfg["pk_column"]),
            max_seconds=int(cfg["drain_max_seconds"]),
            idle_seconds=int(cfg["drain_idle_seconds"]),
            max_messages=int(cfg["drain_max_messages"]),
            log=log,
        )
        log.info("drain_done", message_count=len(events))

        if not events:
            return {"upserts": 0, "deletes": 0}

        upserts, deletes = _events_to_dataframes(events, str(cfg["pk_column"]))
        result = _apply_to_snowflake(
            upserts=upserts,
            deletes=deletes,
            pk_column=str(cfg["pk_column"]),
            database=db,
            schema=sch,
            table=tbl,
            sample_row=events[0],
            log=log,
        )

        consumer.commit(asynchronous=False)
        log.info("offsets_committed", **result, raw_events=len(events))
        return result
    finally:
        consumer.close()


@flow(name="cdc-to-snowflake")
def cdc_to_snowflake_flow() -> dict[str, int]:
    """Drain Redpanda and bulk-MERGE the latest CDC state into Snowflake."""
    _load_dotenv_layers()
    configure_structlog()
    log = get_logger(component="flow", pipeline="cdc-to-snowflake")
    log.info("flow_start")
    result = drain_and_load()
    log.info("flow_done", **result)
    return result


if __name__ == "__main__":
    cdc_to_snowflake_flow()
