{% macro coalesce_cast(column_list, datatype) -%}
  {{ return(adapter.dispatch('coalesce_cast', 'zendesk')(column_list, datatype)) }}
{%- endmacro %}

{% macro default__coalesce_cast(column_list, datatype) %}
  coalesce(
    {%- for column in column_list %}
      cast({{ column }} as {{ datatype }})
      {%- if not loop.last -%},{%- endif -%}
    {% endfor %}
  )
{% endmacro %}