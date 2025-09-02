{% macro get_domain_name_columns() %}

{% set columns = [
    {"name": "_fivetran_synced", "datatype": dbt.type_timestamp()},
    {"name": "domain_name", "datatype": dbt.type_string()},
    {"name": "index", "datatype": dbt.type_int()},
    {"name": "organization_id", "datatype": dbt.type_int()}
] %}

{{ return(columns) }}

{% endmacro %}
