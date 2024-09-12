{% macro clean_string(string_field, character_list) -%}
  {{ return(adapter.dispatch('clean_string', 'zendesk')(string_field, character_list)) }}
{%- endmacro %}

{% macro default__clean_string(string_field, character_list) %}
  {% for character in character_list -%}
    replace(
  {%- endfor -%}
  {{ string_field }}
  {% for character in character_list -%}
    , {{ "'" ~ character ~ "'"}}, '')
  {%- endfor -%}

{% endmacro %}