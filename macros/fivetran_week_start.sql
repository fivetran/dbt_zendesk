{%- macro fivetran_week_start(dt) -%}
{{ adapter.dispatch('fivetran_week_start', 'zendesk') (dt) }}
{%- endmacro -%}

{%- macro default__fivetran_week_start(dt) -%}
cast({{ dbt.date_trunc('week', dt) }} as date)
{%- endmacro %}

{%- macro snowflake__fivetran_week_start(dt) -%}
    -- For Snowflake, adjust week start to Sunday
    cast(
        case 
            when dayofweekiso({{ dt }}) = 7 then {{ dt }} -- dayofweekiso returns 7 for Sunday
            else {{ dbt.dateadd("day", "-1 * dayofweekiso(" ~ dt ~ ")", dt) }}
        end
    as date)
{%- endmacro %}

{%- macro postgres__fivetran_week_start(dt) -%}
-- Sunday as week start date
cast({{ dbt.dateadd('day', -1, dbt.date_trunc('week', dbt.dateadd('day', 1, dt))) }} as date)
{%- endmacro %}

{%- macro duckdb__fivetran_week_start(dt) -%}
{{ return(zendesk.postgres__fivetran_week_start(dt)) }}
{%- endmacro %}
