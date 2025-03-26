{%- macro week_start(date) -%}
{{ adapter.dispatch('week_start', 'zendesk') (date) }}
{%- endmacro -%}

{%- macro default__week_start(date) -%}
cast({{ dbt.date_trunc('week', date) }} as date)
{%- endmacro %}

{%- macro snowflake__week_start(date) -%}
    case
        when date_part('dayofweek', date) = 7 
            then cast({{ dbt.dateadd("day", -1, date) }} as date)
        else cast({{ dbt.dateadd("day", "-1 * date_part('dayofweek', date + 1)", date) }} as date)
        date_part('dayofweek', date + 1)
    end
{%- endmacro %}

{%- macro postgres__week_start(date) -%}
-- Sunday as week start date
cast({{ dbt.dateadd('day', -1, dbt.date_trunc('week', dbt.dateadd('day', 1, date))) }} as date)
{%- endmacro %}

{%- macro duckdb__week_start(date) -%}
{{ return(zendesk.postgres__week_start(date)) }}
{%- endmacro %}
