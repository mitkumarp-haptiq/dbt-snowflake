-- Fails if the final model contains invalid phone formatting or age/date mismatches.
select
    name,
    phone_number,
    date_of_birth,
    age,
    {{ age_years("date_of_birth") }} as expected_age
from {{ ref("users_data") }}
where date_of_birth is null
    or age is null
    or age < 0
    or age != {{ age_years("date_of_birth") }}
    or not regexp_like(phone_number, '^[0-9]+$')
