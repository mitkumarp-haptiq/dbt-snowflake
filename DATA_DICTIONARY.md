# Snowflake Data Dictionary

> Auto-generated on 2026-04-14. Sensitive column values are not listed.

---

## Databases Overview

| Database | Kind | Owner | Description |
|---|---|---|---|
| SNOWFLAKE | APPLICATION | — | System database (account usage, metadata) |
| SNOWFLAKE_LEARNING_DB | STANDARD | ACCOUNTADMIN | Project working database |
| SNOWFLAKE_SAMPLE_DATA | IMPORTED | ACCOUNTADMIN | Preloaded TPC-H / TPC-DS benchmark data |

---

## 1. SNOWFLAKE_LEARNING_DB

### Schemas

| Schema | Owner |
|---|---|
| MY_NEW_SCHEMA | PUBLIC |
| MY_NEW_SCHEMA_CORE | PUBLIC |

---

### 1.1 MY_NEW_SCHEMA.POSTGRES_SNAPSHOT

Raw snapshot of user records ingested from PostgreSQL.

| # | Column | Data Type | Nullable | Sensitive |
|---|---|---|---|---|
| 1 | name | TEXT | YES | **PII** |
| 2 | phone_number | TEXT | YES | **PII** |
| 3 | date_of_birth | TEXT | YES | **PII** |

- **Row count:** 21
- **Storage:** 1.5 KB
- **Notes:** All columns are stored as TEXT (no type casting at ingestion). Contains personally identifiable information — do not expose raw values in reporting.

---

### 1.2 MY_NEW_SCHEMA_CORE.USERS_DATA

Transformed/curated user data produced by dbt (core layer).

| # | Column | Data Type | Nullable | Sensitive |
|---|---|---|---|---|
| 1 | NAME | TEXT | YES | **PII** |
| 2 | PHONE_NUMBER | TEXT | YES | **PII** |
| 3 | DATE_OF_BIRTH | DATE | YES | **PII** |
| 4 | AGE | NUMBER(10,0) | YES | No |

- **Row count:** 3
- **Storage:** 2 KB
- **Notes:** Derived from `POSTGRES_SNAPSHOT`. `DATE_OF_BIRTH` is properly cast to DATE and `AGE` is computed as a NUMBER.

---

## 2. SNOWFLAKE_SAMPLE_DATA (TPC Benchmarks)

Pre-loaded benchmark datasets at various scale factors. Schemas with the same prefix share identical table structures.

### Schemas

| Schema | Description |
|---|---|
| TPCH_SF1 | TPC-H at scale factor 1 (~1 GB) |
| TPCH_SF10 | TPC-H at scale factor 10 (~10 GB) |
| TPCH_SF100 | TPC-H at scale factor 100 (~100 GB) |
| TPCH_SF1000 | TPC-H at scale factor 1000 (~1 TB) |
| TPCDS_SF10TCL | TPC-DS at 10 TB (clustered) |
| TPCDS_SF100TCL | TPC-DS at 100 TB |

---

### 2.1 TPC-H Tables (all TPCH_SF* schemas share this structure)

#### CUSTOMER

| # | Column | Data Type | Nullable | Description |
|---|---|---|---|---|
| 1 | C_CUSTKEY | NUMBER | NO | Primary key |
| 2 | C_NAME | TEXT | NO | Customer name |
| 3 | C_ADDRESS | TEXT | NO | Street address |
| 4 | C_NATIONKEY | NUMBER | NO | FK → NATION |
| 5 | C_PHONE | TEXT | NO | Phone number |
| 6 | C_ACCTBAL | NUMBER | NO | Account balance |
| 7 | C_MKTSEGMENT | TEXT | YES | Market segment |
| 8 | C_COMMENT | TEXT | YES | Free-text comment |

#### ORDERS

| # | Column | Data Type | Nullable | Description |
|---|---|---|---|---|
| 1 | O_ORDERKEY | NUMBER | NO | Primary key |
| 2 | O_CUSTKEY | NUMBER | NO | FK → CUSTOMER |
| 3 | O_ORDERSTATUS | TEXT | NO | F, O, or P |
| 4 | O_TOTALPRICE | NUMBER | NO | Total order price |
| 5 | O_ORDERDATE | DATE | NO | Date order was placed |
| 6 | O_ORDERPRIORITY | TEXT | NO | Priority level |
| 7 | O_CLERK | TEXT | NO | Clerk identifier |
| 8 | O_SHIPPRIORITY | NUMBER | NO | Shipping priority |
| 9 | O_COMMENT | TEXT | NO | Free-text comment |

#### LINEITEM

| # | Column | Data Type | Nullable | Description |
|---|---|---|---|---|
| 1 | L_ORDERKEY | NUMBER | NO | FK → ORDERS |
| 2 | L_PARTKEY | NUMBER | NO | FK → PART |
| 3 | L_SUPPKEY | NUMBER | NO | FK → SUPPLIER |
| 4 | L_LINENUMBER | NUMBER | NO | Line item sequence |
| 5 | L_QUANTITY | NUMBER | NO | Quantity ordered |
| 6 | L_EXTENDEDPRICE | NUMBER | NO | Extended price |
| 7 | L_DISCOUNT | NUMBER | NO | Discount percentage |
| 8 | L_TAX | NUMBER | NO | Tax percentage |
| 9 | L_RETURNFLAG | TEXT | NO | Return flag (R/A/N) |
| 10 | L_LINESTATUS | TEXT | NO | Line status (O/F) |
| 11 | L_SHIPDATE | DATE | NO | Ship date |
| 12 | L_COMMITDATE | DATE | NO | Commit date |
| 13 | L_RECEIPTDATE | DATE | NO | Receipt date |
| 14 | L_SHIPINSTRUCT | TEXT | NO | Shipping instructions |
| 15 | L_SHIPMODE | TEXT | NO | Shipping mode |
| 16 | L_COMMENT | TEXT | NO | Free-text comment |

#### PART

| # | Column | Data Type | Nullable | Description |
|---|---|---|---|---|
| 1 | P_PARTKEY | NUMBER | NO | Primary key |
| 2 | P_NAME | TEXT | NO | Part name |
| 3 | P_MFGR | TEXT | NO | Manufacturer |
| 4 | P_BRAND | TEXT | NO | Brand |
| 5 | P_TYPE | TEXT | NO | Part type |
| 6 | P_SIZE | NUMBER | NO | Part size |
| 7 | P_CONTAINER | TEXT | NO | Container type |
| 8 | P_RETAILPRICE | NUMBER | NO | Retail price |
| 9 | P_COMMENT | TEXT | YES | Free-text comment |

#### PARTSUPP

| # | Column | Data Type | Nullable | Description |
|---|---|---|---|---|
| 1 | PS_PARTKEY | NUMBER | NO | FK → PART |
| 2 | PS_SUPPKEY | NUMBER | NO | FK → SUPPLIER |
| 3 | PS_AVAILQTY | NUMBER | NO | Available quantity |
| 4 | PS_SUPPLYCOST | NUMBER | NO | Supply cost |
| 5 | PS_COMMENT | TEXT | YES | Free-text comment |

#### SUPPLIER

| # | Column | Data Type | Nullable | Description |
|---|---|---|---|---|
| 1 | S_SUPPKEY | NUMBER | NO | Primary key |
| 2 | S_NAME | TEXT | NO | Supplier name |
| 3 | S_ADDRESS | TEXT | NO | Address |
| 4 | S_NATIONKEY | NUMBER | NO | FK → NATION |
| 5 | S_PHONE | TEXT | NO | Phone number |
| 6 | S_ACCTBAL | NUMBER | NO | Account balance |
| 7 | S_COMMENT | TEXT | YES | Free-text comment |

#### NATION

| # | Column | Data Type | Nullable | Description |
|---|---|---|---|---|
| 1 | N_NATIONKEY | NUMBER | NO | Primary key |
| 2 | N_NAME | TEXT | NO | Nation name |
| 3 | N_REGIONKEY | NUMBER | NO | FK → REGION |
| 4 | N_COMMENT | TEXT | YES | Free-text comment |

#### REGION

| # | Column | Data Type | Nullable | Description |
|---|---|---|---|---|
| 1 | R_REGIONKEY | NUMBER | NO | Primary key |
| 2 | R_NAME | TEXT | NO | Region name |
| 3 | R_COMMENT | TEXT | YES | Free-text comment |

---

## Sensitive Data Summary

The following columns contain PII and should **not** be exposed in dashboards or shared datasets without masking/redaction:

| Database | Schema | Table | Column | Classification |
|---|---|---|---|---|
| SNOWFLAKE_LEARNING_DB | MY_NEW_SCHEMA | POSTGRES_SNAPSHOT | name | PII — Full name |
| SNOWFLAKE_LEARNING_DB | MY_NEW_SCHEMA | POSTGRES_SNAPSHOT | phone_number | PII — Phone |
| SNOWFLAKE_LEARNING_DB | MY_NEW_SCHEMA | POSTGRES_SNAPSHOT | date_of_birth | PII — DOB |
| SNOWFLAKE_LEARNING_DB | MY_NEW_SCHEMA_CORE | USERS_DATA | NAME | PII — Full name |
| SNOWFLAKE_LEARNING_DB | MY_NEW_SCHEMA_CORE | USERS_DATA | PHONE_NUMBER | PII — Phone |
| SNOWFLAKE_LEARNING_DB | MY_NEW_SCHEMA_CORE | USERS_DATA | DATE_OF_BIRTH | PII — DOB |
