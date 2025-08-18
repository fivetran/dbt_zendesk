{% macro get_ticket_field_history_columns() %}

{% set columns = [
    {"name": "_fivetran_synced", "datatype": dbt.type_timestamp()},
    {"name": "field_name", "datatype": dbt.type_string()},
    {"name": "ticket_id", "datatype": dbt.type_int()},
    {"name": "updated", "datatype": dbt.type_timestamp()},
    {"name": "user_id", "datatype": dbt.type_int()},
    {"name": "value", "datatype": dbt.type_string()}
] %}

{{ return(columns) }}

{% endmacro %}
