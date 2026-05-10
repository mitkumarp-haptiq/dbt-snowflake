-- Example for public.customers captured to RAW.CDC_CUSTOMERS_RAW.
-- Curated target table: DBT_MIT.CDC_CUSTOMERS

create table if not exists DBT_MIT.CDC_CUSTOMERS (
  ID variant,
  FIRST_NAME variant,
  LAST_NAME variant,
  EMAIL variant
);

create stream if not exists RAW.CDC_CUSTOMERS_RAW_STREAM
  on table RAW.CDC_CUSTOMERS_RAW;

create task if not exists DBT_MIT.CDC_CUSTOMERS_TASK
  warehouse = TRANSFORM_WH
  schedule = '1 minute'
  when SYSTEM$STREAM_HAS_DATA('RAW.CDC_CUSTOMERS_RAW_STREAM')
as
merge into DBT_MIT.CDC_CUSTOMERS as target
using (
  with src as (
    select
      record_content as payload,
      record_content:"id"::variant as id_val,
      record_content:"__op"::string as op,
      try_to_number(record_content:"__ts_ms"::string) as ts_ms,
      row_number() over (
        partition by record_content:"id"::variant
        order by try_to_number(record_content:"__ts_ms"::string) desc
      ) as rn
    from RAW.CDC_CUSTOMERS_RAW_STREAM
  )
  select payload, id_val, op
  from src
  where rn = 1
) as source
on target.ID = source.id_val
when matched and source.op = 'd' then delete
when matched and source.op <> 'd' then update set
  ID = source.id_val,
  FIRST_NAME = source.payload:"first_name"::variant,
  LAST_NAME = source.payload:"last_name"::variant,
  EMAIL = source.payload:"email"::variant
when not matched and source.op <> 'd' then
  insert (ID, FIRST_NAME, LAST_NAME, EMAIL)
  values (
    source.id_val,
    source.payload:"first_name"::variant,
    source.payload:"last_name"::variant,
    source.payload:"email"::variant
  );

alter task DBT_MIT.CDC_CUSTOMERS_TASK resume;
