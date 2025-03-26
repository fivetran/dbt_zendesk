{%- macro week_end(dt) -%}
{{ adapter.dispatch('week_end', 'zendesk') (dt) }}
{%- endmacro -%}

{%- macro default__week_end(dt) -%}
{{ dbt.last_day(dt, 'week') }}
{%- endmacro %}

{%- macro snowflake__week_end(dt) -%}
cast({{ dbt.dateadd('day', 6, zendesk.week_start(dt)) }} as date)
{%- endmacro %}

{%- macro postgres__week_end(dt) -%}
cast({{ dbt.dateadd('day', 6, zendesk.week_start(dt)) }} as date)
{%- endmacro %}

{%- macro duckdb__week_end(dt) -%}
{{ return(zendesk.postgres__week_end(dt)) }}
{%- endmacro %}
