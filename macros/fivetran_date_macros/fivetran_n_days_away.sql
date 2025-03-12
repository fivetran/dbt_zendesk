{%- macro fivetran_n_days_away(n, date=None, tz=None) -%}
{{ adapter.dispatch('fivetran_n_days_away', 'zendesk') (n, date, tz) }}
{%- endmacro -%}

{%- macro default__fivetran_n_days_away(n, date, tz) -%}
{{ zendesk.fivetran_n_days_ago(-1 * n, date, tz) }}
{%- endmacro -%}