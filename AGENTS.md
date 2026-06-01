# AGENTS.md

Operating manual for AI coding agents working in this repository. Read this before making changes.

---

## Project in one paragraph

This repo is a small **ELT pipeline**: a Prefect Python flow extracts from **Postgres** and loads into **Snowflake** (`flows/postgres_to_snowflake/`), and a **dbt** project (`learn_dbt/`) transforms the landed `POSTGRES_SNAPSHOT` table into curated models (`users_data`, `adult_detection`). Snowflake auth is **RSA key-pair**; all credentials come from `.env` (never hardcoded). Logs flow through **structlog** and can optionally ship to **Azure Monitor / Application Insights**.

---

## Tech stack (pinned)

- **Python** 3.10+ (virtualenv at `./.venv`)
- **Prefect** 3.6.21 (orchestration, worker pool: `default-agent-pool`)
- **dbt-snowflake** >=1.7,<1.9
- **snowflake-connector-python** (via `dbt-snowflake`) — key-pair auth only
- **pandas** >=2.0,<3, **pyarrow** >=14,<21 (required by `write_pandas`)
- **psycopg2-binary** >=2.9,<3 (Postgres driver)
- **structlog** >=24.1,<26 (all pipeline logging)
- **azure-monitor-opentelemetry** >=1.8,<1.9 (optional Azure Monitor export)
- **python-dotenv** for `.env` loading (repo root + parent dir, repo wins)

See `requirements.txt` for the source of truth.

---

## Commands you will actually run

**Always activate the venv first** (see `.cursor/rules/os-venv.mdc`):

```bash
source .venv/bin/activate
```

### dbt (from repo root, always pass `--project-dir` and `--profiles-dir`)

```bash
set -a && source .env && set +a                                      # load env once per shell
dbt debug   --project-dir learn_dbt --profiles-dir learn_dbt         # verify Snowflake connectivity
dbt compile --project-dir learn_dbt --profiles-dir learn_dbt         # type/ref check, no warehouse writes
dbt build   --project-dir learn_dbt --profiles-dir learn_dbt --select users_data+   # build + test a subtree
dbt test    --project-dir learn_dbt --profiles-dir learn_dbt         # run schema + custom tests only
dbt run     --project-dir learn_dbt --profiles-dir learn_dbt --select tag:core      # run tagged models
```

`dbt` does **not** auto-load `.env` — you must `set -a && source .env && set +a` in the same shell.

### Prefect flow (Postgres → Snowflake)

```bash
source .venv/bin/activate
pip install -r requirements.txt
set -a && source .env && set +a        # optional; the flow also loads dotenv itself
python flows/postgres_to_snowflake/main.py                           # uses POSTGRES_COPY_SQL / SNOWFLAKE_LOAD_TABLE
prefect deploy --all                                                 # register deployments from prefect.yaml
prefect worker start -p default-agent-pool                           # start a worker that polls the pool
```

Invoke with explicit parameters (prefer for anything non-default):

```bash
python -c "
from flows.postgres_to_snowflake.main import run_postgres_to_snowflake_flow
run_postgres_to_snowflake_flow(
    postgres_sql='SELECT id, col FROM public.my_table LIMIT 10000',
    snowflake_table='MY_RAW_TABLE',
    replace_snowflake_table=True,
)
"
```

### Python environment

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install --upgrade pip                        # optional but often useful
```

---

## Project structure

```
dbt-snowflake/
├── flows/postgres_to_snowflake/     # Prefect extract/load flow (Python)
│   ├── main.py                      # @flow run_postgres_to_snowflake_flow
│   ├── connections.py               # Snowflake key-pair + Postgres psycopg2 connectors
│   └── logging_config.py            # structlog + optional Azure Monitor wiring
├── learn_dbt/                       # dbt project (profile: learn_dbt, adapter: snowflake)
│   ├── dbt_project.yml              # models.core (table, tag=core), models.marts (table, tag=marts)
│   ├── profiles.yml                 # reads SNOWFLAKE_* via {{ env_var(...) }}
│   ├── models/core/                 # curated models (users_data.sql, core.yml sources+models)
│   ├── macros/date_utils.sql        # parse_day_first_date, age_years, age_reference_date
│   ├── tests/                       # singular SQL tests (e.g., test_users_data_quality.sql)
│   ├── analyses/ seeds/ snapshots/  # placeholders with .gitkeep
│   └── packages.yml                 # empty today
├── prefect.yaml                     # deployment entrypoints
├── requirements.txt                 # pinned runtime deps
├── .env.example                     # copy to .env; never commit .env itself
├── README.md                        # human-facing overview
├── CLAUDE.md                        # behavioral guidelines for LLMs (read this too)
└── AGENTS.md                        # this file
```

Source tables live in Snowflake (`POSTGRES_SNAPSHOT`); dbt reads them via `source()` defined in `learn_dbt/models/core/core.yml`. If you rename the Snowflake source table, update **both** `core.yml` and any model referencing the source.

---

## Code style and conventions

### Python

- **Black-compatible** formatting. Keep imports grouped: stdlib → third-party → local, separated by blank lines.
- Use `from __future__ import annotations` in new modules (matches existing files).
- Type hints on all public functions.
- Use **structlog** for logging, never `print()`. Bind context with `.bind(...)` and emit event names (`"flow_start"`, `"write_snowflake_done"`) rather than free-form strings.
- Catch specific exceptions (`SnowflakeDriverError`, `MissingDependencyError`, `ValueError`). Never `except Exception: pass`.
- Use `try/finally` to close DB connections.
- Read config from env through small helpers (`_env_bool`, `_env_int`); never hardcode credentials, hostnames, roles, or table names.
- Prefect: each I/O step is a `@task` with retries; the `@flow` only orchestrates.

Example of the established style (Prefect task with structlog + specific exception handling):

```python
@task(name="read-postgres", retries=1, retry_delay_seconds=15)
def read_postgres(sql: str) -> pd.DataFrame:
    log = get_logger(component="read_postgres", task="read-postgres")
    log.info("read_postgres_start", sql_preview=sql[:200])
    conn = postgres_connect()
    t0 = time.monotonic()
    try:
        df = pd.read_sql_query(sql, conn)
        log.info("read_postgres_done", rows=len(df), duration_s=round(time.monotonic() - t0, 3))
        return df
    except Exception:
        log.exception("read_postgres_failed")
        raise
    finally:
        conn.close()
```

Anti-pattern (do **not** write this):

```python
# no structure, no typing, swallows errors, hardcoded creds
def get(sql):
    try:
        c = psycopg2.connect("host=localhost user=postgres password=hunter2 dbname=mydb")
        return pd.read_sql(sql, c).to_dict()
    except:
        pass
```

### SQL / dbt

- Models live in `learn_dbt/models/<layer>/`. Today only `core/` is used; `marts/` is reserved by `dbt_project.yml`.
- Use `source()` for warehouse-landed tables (never `{{ ref }}` on raw sources).
- CTE-per-step style, lowercase SQL keywords, quote mixed-case identifiers from the raw source:

```sql
with source as (
    select * from {{ source("snowflake", "postgres_snapshot") }}
),
cleaned as (
    select
        trim("name") as name,
        regexp_replace("phone_number", '[^0-9]', '') as phone_number,
        {{ parse_day_first_date('"date_of_birth"') }} as date_of_birth
    from source
)
select * from cleaned
```

- Shared logic goes in `macros/` (e.g., `age_years`, `parse_day_first_date`) — don't inline formulas across models.
- Every new model gets a matching block in the layer's `<layer>.yml` with `description` and column docs.
- Add data-quality checks in `learn_dbt/tests/` (singular SQL tests) or via schema tests on columns.

### YAML / config

- `dbt_project.yml` keys (`+materialized`, `+schema`, `+tags`) are authoritative for defaults; override per-model only when needed.
- `profiles.yml` must pull every secret via `{{ env_var('SNOWFLAKE_*') }}` with a safe `CHANGEME_*` fallback.

---

## Environment variables (required)

See `.env.example` for the full list. Minimum to run anything:

| Variable | Used by | Notes |
|---|---|---|
| `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER` | dbt, flow | Account locator + user |
| `SNOWFLAKE_PRIVATE_KEY_PATH` | dbt, flow | Path to PEM key (key-pair auth) |
| `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE` | dbt, flow | Only if PEM is encrypted |
| `SNOWFLAKE_ROLE`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_DATABASE`, `SNOWFLAKE_SCHEMA` | dbt, flow | `SCHEMA` must **not** be `INFORMATION_SCHEMA` |
| `DBT_PROFILES_DIR=learn_dbt` | dbt | Points dbt at `learn_dbt/profiles.yml` |
| `POSTGRES_HOST/PORT/USER/PASSWORD/DB` | flow | Source connection |
| `POSTGRES_COPY_SQL`, `SNOWFLAKE_LOAD_TABLE` | flow (optional) | Override the built-in exploratory defaults |
| `SNOWFLAKE_LOAD_REPLACE` | flow (optional) | `true` to overwrite instead of append |
| `APPLICATIONINSIGHTS_CONNECTION_STRING` | flow (optional) | Enables Azure Monitor logging |

`.env` is **gitignored**. Never commit real values.

---

## Testing

- **dbt tests** are the primary safety net. Add schema tests in `*.yml` and singular SQL tests in `learn_dbt/tests/`:

```bash
dbt test  --project-dir learn_dbt --profiles-dir learn_dbt
dbt build --project-dir learn_dbt --profiles-dir learn_dbt --select users_data+   # build + test together
```

- Example singular test (fails if any row violates expectations):

```sql
select *
from {{ ref("users_data") }}
where date_of_birth is null
   or age < 0
   or not regexp_like(phone_number, '^[0-9]+$')
```

- For Python changes, run a **smoke invocation** of the flow against a safe target:

```bash
python -c "
from flows.postgres_to_snowflake.main import run_postgres_to_snowflake_flow
run_postgres_to_snowflake_flow(
    postgres_sql='SELECT 1 AS probe',
    snowflake_table='AGENT_SMOKE_PROBE',
    replace_snowflake_table=True,
)
"
```

- There is no pytest suite today. If you add one, put it in `tests/` at repo root and wire it into CI before relying on it.

---

## Git workflow

- Branch from `main`; keep PRs narrow — one concern per PR (flow vs. dbt vs. CI).
- Never commit: `.env`, `.env.*` (except `.env.example`), `*.p8` / `*.pem`, `learn_dbt/target/`, `learn_dbt/dbt_packages/`, `learn_dbt/logs/`, `logs/`, `.venv/`, `graphify-out/cache/`. `.gitignore` covers the standard set — check it before staging new artefacts.
- Commit messages: imperative mood, scope prefix when helpful (`flow:`, `dbt:`, `ci:`). Match existing history style.
- Run `dbt compile` and the flow smoke probe locally before pushing anything that touches SQL or the loader.
- Do **not** force-push shared branches. Do **not** amend commits already pushed to a remote branch others may have pulled.

---

## Boundaries

- **Always do**
  - Read `CLAUDE.md` before coding (simplicity, surgical changes, goal-driven execution).
  - Activate `./.venv` before running Python or dbt.
  - Load `.env` into the shell (`set -a && source .env && set +a`) before invoking `dbt`.
  - Route all credentials through env vars + `.env`; use `snowflake_env_connect_params()` / `postgres_connect()` helpers.
  - Emit structured logs with `get_logger(component=...)` and bind relevant context (table, rows, duration).
  - Keep Prefect task boundaries (`read_postgres`, `write_snowflake`) — don't collapse them into the flow.
  - Update `learn_dbt/models/*/<layer>.yml` whenever you add or rename a model or column.

- **Ask first**
  - Changes to `SNOWFLAKE_SCHEMA`, target database/role, or `dbt_project.yml` materializations — these affect every downstream asset.
  - Adding or upgrading a dependency in `requirements.txt` (check that pins still satisfy `dbt-snowflake` and `snowflake-connector-python`).
  - Any change that would write to a new Snowflake schema/database.
  - Renaming `POSTGRES_SNAPSHOT` or altering `models/core/core.yml` sources.
  - Introducing a new ETL domain under `flows/` — the expected layout is `flows/<domain>/{extract,transform,load,main}.py` (see `.cursor/rules/etl-python-architecture.mdc`).

- **Never do**
  - Commit secrets, `.env`, private keys (`*.p8`, `*.pem`), passwords, or Application Insights connection strings.
  - Hardcode Snowflake accounts, users, roles, warehouses, databases, schemas, or Postgres credentials anywhere in code or dbt YAML.
  - Set `SNOWFLAKE_SCHEMA` to `INFORMATION_SCHEMA` — the flow rejects this on purpose (see `FORBIDDEN_SNOWFLAKE_LOAD_SCHEMAS`).
  - Pass `quote_identifiers=False` to `write_pandas` — Snowflake uppercases unquoted identifiers and reserved words will fail with SQL 000904.
  - Skip `pyarrow` in any environment that runs `write_pandas` — the Parquet path requires it.
  - Rewrite `test.py` without confirming ownership — it is a standalone API probe, not part of the ELT pipeline.
  - Delete or regenerate `graphify-out/` as part of unrelated work; treat it as generated artefact.
  - Swallow exceptions from Snowflake / Postgres drivers. Log with `log.exception(...)` and re-raise.
  - Modify `learn_dbt/target/`, `learn_dbt/dbt_packages/`, `.venv/`, or any path under `logs/` — these are generated.

---

## When in doubt

1. Check `README.md` for human-facing setup context.
2. Check `CLAUDE.md` for behavioral guardrails (think before coding, simplicity, surgical changes).
3. Check `.cursor/rules/` for domain-specific conventions (ETL architecture, Flake8, venv, GitHub workflows).
4. If a change is ambiguous or cuts across the six areas above, stop and ask — don't guess.
