# APEX_DEV Data Dictionary

> Generated 2026-04-16 via Snowflake MCP (`sql_exec`) — metadata only, no row-level queries.
> Role: `PUBLIC`. Storage in GB (decimal) = `BYTES / 1000000000`.

---

## Database

| Database   | Kind       | Owner          | Retention Days |
| ---------- | ---------- | -------------- | -------------- |
| `APEX_DEV` | `STANDARD` | `ACCOUNTADMIN` | `1`            |

## Schemas

| Schema               | Owner          | Retention Days | Tables | Views | Stages |
| -------------------- | -------------- | -------------- | ------ | ----- | ------ |
| `INFORMATION_SCHEMA` | System-managed | `1`            | 0      | 0     | 0      |
| `MRSFIELDS`          | `ACCOUNTADMIN` | `1`            | 2      | 0     | 0      |
| `MSC`                | `ACCOUNTADMIN` | `1`            | 0      | 0     | 1      |
| `PUBLIC`             | `ACCOUNTADMIN` | `1`            | 0      | 0     | 0      |

## Tables

| Schema      | Table           | Columns | Rows        | Size (GB)  | Owner              | Created                      | Last Altered                 |
| ----------- | --------------- | ------- | ----------- | ---------- | ------------------ | ---------------------------- | ---------------------------- |
| `MRSFIELDS` | `SALES_ORDER`   | 30      | 1,640,565   | 0.058487   | `MRSFIELDS_DEV_L2` | 2026-02-23 22:17 PST         | 2026-04-16 06:02 PDT         |
| `MRSFIELDS` | `SALES_ORDER_1` | 30      | 32,632      | 0.001421   | `MRSFIELDS_DEV_L2` | 2026-03-05 17:38 PST         | 2026-03-05 18:58 PST         |

Both tables have: no primary keys, no column comments, no masking policies, automatic clustering `OFF`, change tracking `OFF`, search optimization `OFF`.

## Stages

| Schema | Stage      | Type       | Owner          | Credentials | Encryption Key | Directory Enabled |
| ------ | ---------- | ---------- | -------------- | ----------- | -------------- | ----------------- |
| `MSC`  | `MRSFIELD` | `INTERNAL` | `ACCOUNTADMIN` | N           | N              | Y                 |

## Column Dictionary — `MRSFIELDS.SALES_ORDER`

`SALES_ORDER_1` shares the identical 30-column schema.

| #  | Column              | Type                | Nullable | Definition |
| -- | ------------------- | ------------------- | -------- | ---------- |
| 1  | `SALESID`           | `VARCHAR(16777216)` | Y        | Sales order identifier (e.g. `SO0405609`) |
| 2  | `LINENUM`           | `FLOAT`             | Y        | Line number within the sales order |
| 3  | `ORDERDATE`         | `VARCHAR(16777216)` | Y        | Date the order was placed (`YYYY-MM-DD`, stored as text) |
| 4  | `REQUESTEDSHIPDATE` | `VARCHAR(16777216)` | Y        | Customer-requested ship date (`YYYY-MM-DD`, stored as text) |
| 5  | `SALESPOOLID`       | `VARCHAR(16777216)` | Y        | Sales channel. Values: `ONLINE`, `PARTNERS`, `CORPSALES`, `ASI`, `NATIONAL`, `CUSTSRVC` |
| 6  | `ITEMID`            | `VARCHAR(16777216)` | Y        | Product SKU code (e.g. `24WNTIN484`) |
| 7  | `ITEMNAME`          | `VARCHAR(16777216)` | Y        | Product description (e.g. `B2B 48 NIBBLER TIN`) |
| 8  | `SALESQTY`          | `FLOAT`             | Y        | Quantity ordered on this line |
| 9  | `NETPRODUCTAMOUNT`  | `FLOAT`             | Y        | Net product amount after line discount (`ITEMAMOUNT − LINEDISCOUNT`) |
| 10 | `NETPLUSFREIGHT`    | `FLOAT`             | Y        | Net product amount plus freight (`NETPRODUCTAMOUNT + FREIGHTAMOUNT`) |
| 11 | `LINETOTALAMOUNT`   | `FLOAT`             | Y        | Total line amount including all charges and tax |
| 12 | `CUSTACCOUNT`       | `VARCHAR(16777216)` | Y        | Customer account identifier (e.g. `C000053978`) |
| 13 | `CUSTOMERNAME`      | `VARCHAR(16777216)` | Y        | Customer display name |
| 14 | `ONHOLD`            | `VARCHAR(16777216)` | Y        | Whether the order is on hold. Values: `Yes` / `No` |
| 15 | `ITEMAMOUNT`        | `FLOAT`             | Y        | Gross item amount (unit price × quantity) before discounts |
| 16 | `LINEDISCOUNT`      | `FLOAT`             | Y        | Discount amount applied to this line |
| 17 | `FREIGHTAMOUNT`     | `FLOAT`             | Y        | Freight / shipping charge for this line |
| 18 | `HANDLINGAMOUNT`    | `FLOAT`             | Y        | Handling charge for this line |
| 19 | `DISCOUNTAMOUNT`    | `FLOAT`             | Y        | Additional order-level discount allocated to this line |
| 20 | `FULFILLFEE`        | `FLOAT`             | Y        | Fulfillment fee for this line |
| 21 | `MISCCHARGE`        | `FLOAT`             | Y        | Miscellaneous charges for this line |
| 22 | `SALESTAX`          | `FLOAT`             | Y        | Sales tax amount for this line |
| 23 | `SOURCEORDERNUMBER` | `VARCHAR(16777216)` | Y        | Originating order number from the source system (e.g. e-commerce order ID) |
| 24 | `ISINVOICED`        | `VARCHAR(16777216)` | Y        | Whether an invoice has been generated. Values: `Yes` / `No` |
| 25 | `FYEAR`             | `NUMBER(38,0)`      | Y        | Fiscal year (fiscal calendar does not align with calendar year) |
| 26 | `FPRD`              | `NUMBER(38,0)`      | Y        | Fiscal period number within the fiscal year |
| 27 | `FISCALPERIODMONTH` | `VARCHAR(16777216)` | Y        | Calendar month name corresponding to the fiscal period (e.g. `Dec`) |
| 28 | `SALESGROUP`        | `VARCHAR(16777216)` | Y        | Sales group or team assignment (may be blank) |
| 29 | `ETL_LOADED_AT`     | `VARCHAR(16777216)` | Y        | Timestamp when the row was loaded by the ETL pipeline (stored as text) |
| 30 | `ETL_SOURCE`        | `VARCHAR(16777216)` | Y        | Source system table identifier (e.g. `mfc.SalesOrders`) |

All 30 columns are nullable with no defaults, no unique keys, and no Snowflake comments.
`ORDERDATE`, `REQUESTEDSHIPDATE`, and `ETL_LOADED_AT` are stored as VARCHAR — should be typed as DATE/TIMESTAMP in downstream models.

## Observations

- No primary keys exist on any visible table.
- No views exist in the database (visible to `PUBLIC`).
- The relationship between `SALES_ORDER` and `SALES_ORDER_1` is undocumented.
- `SHOW OBJECTS IN SCHEMA APEX_DEV.MSC` returned no rows despite a visible stage — use object-specific commands for discovery in that schema.

## Business Review Checklist

- [ ] Confirm business purpose of `SALES_ORDER` vs `SALES_ORDER_1`
- [ ] Confirm purpose and usage of stage `APEX_DEV.MSC.MRSFIELD`
- [ ] Confirm whether `MSC` has objects beyond `MRSFIELD` not visible to `PUBLIC`
- [ ] Define table grain and expected uniqueness (no PK exists)
- [ ] Confirm whether `ORDERDATE`, `REQUESTEDSHIPDATE`, `ETL_LOADED_AT` should be typed as dates
- [ ] Confirm masking/privacy requirements for any column
