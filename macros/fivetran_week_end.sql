{%- macro fivetran_week_end(dt) -%}
{{ adapter.dispatch('fivetran_week_end', 'zendesk') (dt) }}
{%- endmacro -%}

{%- macro default__fivetran_week_end(dt) -%}
{{ dbt.last_day(dt, 'week') }}
{%- endmacro %}

{%- macro snowflake__fivetran_week_end(dt) -%}
cast({{ dbt.dateadd('day', 6, zendesk.fivetran_week_start(dt)) }} as date)
{%- endmacro %}

{%- macro postgres__fivetran_week_end(dt) -%}
cast({{ dbt.dateadd('day', 6, zendesk.fivetran_week_start(dt)) }} as date)
{%- endmacro %}

{%- macro duckdb__fivetran_week_end(dt) -%}
{{ return(zendesk.postgres__fivetran_week_end(dt)) }}
{%- endmacro %}
