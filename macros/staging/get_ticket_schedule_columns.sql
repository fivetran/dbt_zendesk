{% macro get_ticket_schedule_columns() %}

{% set columns = [
    {"name": "_fivetran_synced", "datatype": dbt.type_timestamp()},
    {"name": "created_at", "datatype": dbt.type_timestamp()},
    {"name": "schedule_id", "datatype": dbt.type_int()},
    {"name": "ticket_id", "datatype": dbt.type_int()}
] %}

{{ return(columns) }}

{% endmacro %}
