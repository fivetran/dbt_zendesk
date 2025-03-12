{%- macro fivetran_n_days_ago(n, date=None, tz=None) -%}
{%- set dt = date if date else zendesk.fivetran_today(tz) -%}
{%- set n = n|int -%}
{{ adapter.dispatch('fivetran_n_days_ago', 'zendesk') (n, date, tz) }}
{%- endmacro -%}

{%- macro default__fivetran_n_days_ago(n, date, tz) -%}
cast({{ dbt.dateadd('day', -1 * n, dt) }} as date)
{%- endmacro -%}