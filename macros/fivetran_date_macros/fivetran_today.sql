{%- macro fivetran_today(tz=None) -%}
    {{ return(adapter.dispatch('fivetran_today', 'zendesk')) (tz) }}
{%- endmacro -%}

{%- macro default__fivetran_today(tz) -%}
    cast({{ dbt.current_timestamp() }} as date)
{%- endmacro -%}