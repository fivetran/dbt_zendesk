{%- macro fivetran_week_end(date=None, tz=None) -%}
{%-set dt = date if date else zendesk.fivetran_today(tz) -%}
{{ adapter.dispatch('fivetran_week_end', 'zendesk') (dt) }}
{%- endmacro -%}

{%- macro default__fivetran_week_end(date) -%}
{{ last_day(date, 'week') }}
{%- endmacro %}

{%- macro snowflake__fivetran_week_end(date) -%}
{%- set dt = zendesk.fivetran_week_start(date) -%}
{{ zendesk.fivetran_n_days_away(6, dt) }}
{%- endmacro %}

{%- macro postgres__fivetran_week_end(date) -%}
{%- set dt = zendesk.fivetran_week_start(date) -%}
{{ zendesk.fivetran_n_days_away(6, dt) }}
{%- endmacro %}

{%- macro duckdb__fivetran_week_end(date) -%}
{{ return(zendesk.postgres__fivetran_week_end(date)) }}
{%- endmacro %}