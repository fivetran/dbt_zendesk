{% macro get_ticket_custom_field_columns() %}

{% set columns = [
    {"name": "_fivetran_synced", "datatype": dbt.type_timestamp()},
    {"name": "_fivetran_deleted", "datatype": dbt.type_boolean()},
    {"name": "id", "datatype": dbt.type_bigint()},
    {"name": "title", "datatype": dbt.type_string()},
    {"name": "raw_title", "datatype": dbt.type_string()},
    {"name": "key", "datatype": dbt.type_string()},
    {"name": "type", "datatype": dbt.type_string()},
    {"name": "active", "datatype": dbt.type_boolean()}
] %}

{{ return(columns) }}

{% endmacro %}
