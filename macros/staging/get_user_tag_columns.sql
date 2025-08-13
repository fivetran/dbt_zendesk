{% macro get_user_tag_columns() %}

{% set columns = [
    {"name": "_fivetran_synced", "datatype": dbt.type_timestamp()},
    {"name": "user_id", "datatype": dbt.type_int()}
] %}

{% if target.type == 'redshift' %}
    {{ columns.append( {"name": "tag", "datatype": dbt.type_string(), "quote": True } ) }}

{% elif target.type == 'snowflake' %}
    {{ columns.append( {"name": "TAG", "datatype": dbt.type_string(), "quote": True } ) }}

{% else %}
    {{ columns.append( {"name": "tag", "datatype": dbt.type_string()} ) }}

{% endif %}

{{ return(columns) }}

{% endmacro %}
