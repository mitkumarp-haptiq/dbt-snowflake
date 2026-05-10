with source as (

    select * from {{ source("snowflake", "postgres_snapshot") }}

),

cleaned as (

    select
        trim("name") as name,
        regexp_replace("phone_number", '[^0-9]', '') as phone_number,
        {{ parse_day_first_date('"date_of_birth"') }} as date_of_birth
    from source

),

deduplicated as (

    select distinct
        name,
        phone_number,
        date_of_birth
    from cleaned

),

with_age as (

    select
        name,
        phone_number,
        date_of_birth,
        {{ age_years("date_of_birth") }} as age
    from deduplicated

)

select
    name,
    phone_number,
    date_of_birth,
    age
from with_age
