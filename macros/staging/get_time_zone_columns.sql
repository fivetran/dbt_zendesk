{% macro get_time_zone_columns() %}

{% set columns = [
    {"name": "_fivetran_synced", "datatype": dbt.type_timestamp()},
    {"name": "standard_offset", "datatype": dbt.type_string()},
    {"name": "time_zone", "datatype": dbt.type_string()}
] %}

{{ return(columns) }}

{% endmacro %}
