{% macro get_daylight_time_columns() %}

{% set columns = [
    {"name": "_fivetran_synced", "datatype": dbt.type_timestamp()},
    {"name": "daylight_end_utc", "datatype": "datetime"},
    {"name": "daylight_offset", "datatype": dbt.type_int()},
    {"name": "daylight_start_utc", "datatype": "datetime"},
    {"name": "time_zone", "datatype": dbt.type_string()},
    {"name": "year", "datatype": dbt.type_int()}
] %}

{{ return(columns) }}

{% endmacro %}
