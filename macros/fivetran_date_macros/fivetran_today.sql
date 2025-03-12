{%- macro fivetran_today(tz=None) -%}
{{ adapter.dispatch('fivetran_today', 'zendesk') () }}
{%- endmacro -%}

{%- macro default__fivetran_today(tz) -%}

cast({{ dbt.current_timestamp() }} as date)

{%- endmacro -%}