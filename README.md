# ELT with Prefect and dbt (Snowflake)

Minimal layout:

- `**learn_dbt/**` — dbt project (models, tests, `profiles.yml` with env-based Snowflake credentials).
- `**flows/**` — Prefect flow entrypoints; `**flows/postgres_to_snowflake/main.py**` copies query results from Postgres into Snowflake.
- `**prefect.yaml**` — deployment entrypoints for Prefect workers.

## Prerequisites

- Python 3.10+ recommended
- Snowflake account and a role with usage on a warehouse, database, and schema
- [Prefect CLI](https://docs.prefect.io/) if you deploy or trigger runs remotely

## Local setup

```bash
cd /path/to/dbt-snowflake
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# Edit .env with real Snowflake values (repo root or one level above — see below)
```

Put `DBT_PROFILES_DIR=learn_dbt` in `.env` (as in `.env.example`) or run `export DBT_PROFILES_DIR=learn_dbt` in the shell before dbt.

**Where to put `.env`:** dbt does not read `.env` by itself; you must export variables into the shell (`set -a && source .env && set +a`), or run the Prefect flow (it loads dotenv for you).


| Location                         | From repo root (`dbt-snowflake/`) | From `learn_dbt/`                    |
| -------------------------------- | --------------------------------- | ------------------------------------ |
| Repo root (`dbt-snowflake/.env`) | `set -a && source .env && set +a` | `set -a && source ../.env && set +a` |


The Prefect Postgres -> Snowflake flow loads **both** `dbt-snowflake/.env` and **parent** `../.env`, without overriding keys already set (repo wins first).

**dbt from the CLI (working pattern):** use a fresh shell any time env vars are missing.

```bash
source .venv/bin/activate
set -a && source .env && set +a
dbt debug --project-dir learn_dbt --profiles-dir learn_dbt
```

Smoke check (same shell as above after sourcing `.env`):

```bash
dbt build --project-dir learn_dbt --profiles-dir learn_dbt --select smoke_test
```

Build the staged and core models that read from `POSTGRES_SNAPSHOT` in Snowflake:

```bash
dbt build --project-dir learn_dbt --profiles-dir learn_dbt --select users_data+
```

## Prefect

Run the Postgres -> Snowflake flow locally:

```bash
python flows/postgres_to_snowflake/main.py
```

### Azure Monitor logs (optional)

The flow can send **Prefect + pipeline logs** directly to a workspace-based
Application Insights / Azure Monitor resource with no separate collector.

1. Add `APPLICATIONINSIGHTS_CONNECTION_STRING` to `.env`.
2. Optional: set `AZURE_MONITOR_SERVICE_NAME` to control the service name shown in Azure.
3. Run the flow in a shell where `.env` has been loaded:

```bash
source .venv/bin/activate
set -a && source .env && set +a
python flows/postgres_to_snowflake/main.py
```

The resulting logs land in the linked Log Analytics workspace and are easy to
query from Grafana via the Azure Monitor data source.

### Postgres → Snowflake (Prefect)

You can orchestrate **extract from Postgres** and **load into Snowflake** with `flows/postgres_to_snowflake/main.py`. It uses `pandas` + `psycopg2` for the read and `snowflake-connector-python`’s `write_pandas` for the load (same key-pair env vars as dbt).

1. Add `POSTGRES_*` settings to `.env` (see `.env.example`).
2. Ensure your Snowflake role can use the warehouse and write to the target database/schema (defaults come from `SNOWFLAKE_DATABASE` / `SNOWFLAKE_SCHEMA`).
3. Run locally (optional: set `POSTGRES_COPY_SQL` and `SNOWFLAKE_LOAD_TABLE` for the `__main__` defaults):

```bash
source .venv/bin/activate
pip install -r requirements.txt
set -a && source .env && set +a
python flows/postgres_to_snowflake/main.py
```

Or invoke with explicit parameters:

```bash
python -c "
from flows.postgres_to_snowflake.main import run_postgres_to_snowflake_flow
run_postgres_to_snowflake_flow(
    postgres_sql='SELECT * FROM public.my_table LIMIT 10000',
    snowflake_table='MY_RAW_TABLE',
)
"
```

For large tables, prefer **batched** reads (chunked `read_sql_query`) or **unload to cloud storage** and `COPY INTO` in Snowflake instead of one huge DataFrame.

Apply deployments (after `prefect cloud login` or a self-hosted API URL):

```bash
prefect deploy --all
```

Ensure the worker environment has this repo, `pip install -r requirements.txt`, and the same Snowflake-related env vars as in `.env`.

### CDC trigger (webhook mode)

The `cdc-trigger` Docker service is a Kafka-to-Prefect webhook bridge:

1. It consumes `CDC_KAFKA_TOPIC` with observer group `CDC_TRIGGER_GROUP_ID`.
2. When CDC messages are observed, it emits one webhook call per cooldown window.
3. A Prefect Automation webhook receives this event and starts deployment `prefect_cdc_to_snowflake`.

Set these env vars for the bridge:

- `CDC_KAFKA_BOOTSTRAP`
- `CDC_KAFKA_TOPIC`
- `CDC_TRIGGER_GROUP_ID` (optional; default `cdc-prefect-webhook-bridge`)
- `CDC_TRIGGER_COOLDOWN_SECONDS`
- `PREFECT_CDC_WEBHOOK_URL` (required)

Then tail logs:

```bash
docker compose logs -f cdc-trigger
```

## ELT shape

1. **Extract / load** — Load raw tables into Snowflake (Prefect Python tasks, Snowpipe, Fivetran, etc.), typically into a **RAW** (or similar) schema.
2. **Transform** — This repo’s dbt project reads the Snowflake table `POSTGRES_SNAPSHOT` with `source()` and builds the `users_data` model directly from it.

If your source table name or schema changes, update the source definition in `models/core/core.yml` and the `users_data` model to match.