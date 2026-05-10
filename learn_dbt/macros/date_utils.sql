{% macro parse_day_first_date(date_expression) -%}
    try_to_date({{ date_expression }}, 'DD/MM/YYYY')
{%- endmacro %}


{% macro age_reference_date() -%}
    {% set overridden_date = var('users_data_age_as_of_date', none) %}
    {% if overridden_date %}
        to_date('{{ overridden_date }}')
    {% else %}
        current_date()
    {% endif %}
{%- endmacro %}


{% macro age_years(date_expression, as_of_date_expression=None) -%}
    {% set as_of_date = as_of_date_expression or age_reference_date() %}
    case
        when {{ date_expression }} is null then null
        else
            datediff(year, {{ date_expression }}, {{ as_of_date }})
            - iff(
                dateadd(
                    year,
                    datediff(year, {{ date_expression }}, {{ as_of_date }}),
                    {{ date_expression }}
                ) > {{ as_of_date }},
                1,
                0
            )
    end
{%- endmacro %}
