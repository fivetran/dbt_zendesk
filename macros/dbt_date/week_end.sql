{%- macro week_end(date=None, tz=None) -%}
{%-set dt = date if date else dbt_date.today(tz) -%}
{{ adapter.dispatch('week_end', 'zendesk') (dt) }}
{%- endmacro -%}

{%- macro default__week_end(date) -%}
{{ dbt.last_day(date, 'week') }}
{%- endmacro %}

{%- macro snowflake__week_end(date) -%}
{%- set dt = zendesk.week_start(date) -%}
cast({{ dbt.dateadd('day', 6, dt) }} as date)
{%- endmacro %}

{%- macro postgres__week_end(date) -%}
{%- set dt = zendesk.week_start(date) -%}
cast({{ dbt.dateadd('day', 6, dt) }} as date)
{%- endmacro %}

{%- macro duckdb__week_end(date) -%}
{{ return(zendesk.postgres__week_end(date)) }}
{%- endmacro %}
