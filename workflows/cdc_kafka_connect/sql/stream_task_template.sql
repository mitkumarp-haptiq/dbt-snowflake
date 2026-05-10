-- Copy this template per CDC table (<10 tables) and replace placeholders.
-- The task runs every minute, but executes only when stream has data.

-- Required placeholders:
--   __RAW_SCHEMA__   e.g. RAW
--   __RAW_TABLE__    e.g. CDC_CUSTOMERS_RAW
--   __CURATED_SCHEMA__ e.g. PUBLIC
--   __CURATED_TABLE__  e.g. CDC_CUSTOMERS
--   __PK_COL__       e.g. id
--   __WAREHOUSE__    e.g. TRANSFORM_WH

create stream if not exists __RAW_SCHEMA__.__RAW_TABLE___STREAM
  on table __RAW_SCHEMA__.__RAW_TABLE__;

create task if not exists __CURATED_SCHEMA__.__CURATED_TABLE___TASK
  warehouse = __WAREHOUSE__
  schedule = '1 minute'
  when SYSTEM$STREAM_HAS_DATA('__RAW_SCHEMA__.__RAW_TABLE___STREAM')
as
merge into __CURATED_SCHEMA__.__CURATED_TABLE__ as target
using (
  with src as (
    select
      record_content as payload,
      record_content:"__PK_COL__"::variant as pk_val,
      record_content:"__op"::string as op,
      try_to_number(record_content:"__ts_ms"::string) as ts_ms,
      row_number() over (
        partition by record_content:"__PK_COL__"::variant
        order by try_to_number(record_content:"__ts_ms"::string) desc
      ) as rn
    from __RAW_SCHEMA__.__RAW_TABLE___STREAM
  )
  select payload, pk_val, op
  from src
  where rn = 1
) as source
on target."__PK_COL__" = source.pk_val
when matched and source.op = 'd' then delete
when matched and source.op <> 'd' then update set
  -- Replace this with explicit typed mappings for production:
  -- target.col_a = source.payload:"col_a"::string,
  -- target.col_b = source.payload:"col_b"::number
  target."__PK_COL__" = source.pk_val
when not matched and source.op <> 'd' then
  insert ("__PK_COL__")
  values (source.pk_val);

alter task __CURATED_SCHEMA__.__CURATED_TABLE___TASK resume;
