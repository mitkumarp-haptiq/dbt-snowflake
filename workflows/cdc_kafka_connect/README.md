# Multi-Table CDC Workflow (No Prefect Required)

This is a **separate workflow** for `<10` tables. It does not modify the existing
Prefect-based CDC path in this repo.

Pipeline:

1. Postgres WAL -> Debezium Server
2. Debezium -> Redpanda topics (`app.public.<table>`)
3. Kafka Connect Snowflake Sink -> Snowflake RAW tables (`RAW.CDC_*_RAW`)
4. Snowflake Stream + Task per table -> curated tables

## Why this workflow

- Event-driven ingest
- No Python consumer/orchestrator loop to maintain
- Simple for a small number of tables
- Robust buffering in Redpanda

## Configure captured tables

1. Edit `debezium/application.properties`:
   - `debezium.source.table.include.list=public.customers,public.orders,...`
2. Edit `connect/topic-table-map.env`:
   - `TOPICS=app.public.customers,app.public.orders,...`
   - `TOPIC_TABLE_MAP=app.public.customers:CDC_CUSTOMERS_RAW,app.public.orders:CDC_ORDERS_RAW,...`

Keep both files aligned.

## Start services

From repo root:

```bash
docker compose -f workflows/cdc_kafka_connect/docker-compose.yml up -d --build
```

## Register/Update Snowflake connector

From repo root, after `.env` is populated with `SNOWFLAKE_*` vars:

```bash
./workflows/cdc_kafka_connect/connect/register-connector.sh
```

Default connector endpoint is `http://localhost:8083`; override with `CONNECT_URL`.

## Configure Snowflake merge tasks

For each table:

1. Copy `sql/stream_task_template.sql`
2. Replace placeholders (`__RAW_TABLE__`, `__CURATED_TABLE__`, `__PK_COL__`, etc.)
3. Run the SQL in Snowflake

Example ready-made file:

- `sql/customers_stream_task.sql`

## Notes

- In this mode, Prefect can stay exactly as-is.
- Snowflake RAW tables are created by the connector on first events.
- The task runs every minute but executes only when stream data exists.
