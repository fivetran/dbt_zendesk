{% macro get_audit_log_columns() %}

{% set columns = [
    {"name": "_fivetran_synced", "datatype": dbt.type_timestamp()},
    {"name": "id", "datatype": dbt.type_int()},
    {"name": "action", "datatype": dbt.type_string()},
    {"name": "actor_id", "datatype": dbt.type_int()},
    {"name": "change_description", "datatype": dbt.type_string()},
    {"name": "created_at", "datatype": dbt.type_timestamp()},
    {"name": "source_id", "datatype": dbt.type_int()},
    {"name": "source_label", "datatype": dbt.type_string()},
    {"name": "source_type", "datatype": dbt.type_string()}
] %}

{{ return(columns) }}

{% endmacro %}