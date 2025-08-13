{% macro get_schedule_holiday_columns() %}

{% set columns = [
    {"name": "_fivetran_deleted", "datatype": "boolean"},
    {"name": "_fivetran_synced", "datatype": dbt.type_timestamp()},
    {"name": "end_date", "datatype": dbt.type_string()},
    {"name": "id", "datatype": dbt.type_int()},
    {"name": "name", "datatype": dbt.type_string()},
    {"name": "schedule_id", "datatype": dbt.type_int()},
    {"name": "start_date", "datatype": dbt.type_string()}
] %}

{{ return(columns) }}

{% endmacro %}
