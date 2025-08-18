{% macro get_schedule_columns() %}

{% set columns = [
    {"name": "_fivetran_deleted", "datatype": "boolean"},
    {"name": "_fivetran_synced", "datatype": dbt.type_timestamp()},
    {"name": "created_at", "datatype": dbt.type_timestamp()},
    {"name": "end_time", "datatype": dbt.type_int()},
    {"name": "end_time_utc", "datatype": dbt.type_int()},
    {"name": "id", "datatype": dbt.type_int()},
    {"name": "name", "datatype": dbt.type_string()},
    {"name": "start_time", "datatype": dbt.type_int()},
    {"name": "start_time_utc", "datatype": dbt.type_int()},
    {"name": "time_zone", "datatype": dbt.type_string()}
] %}

{{ return(columns) }}

{% endmacro %}
