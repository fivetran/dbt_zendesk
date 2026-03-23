{% macro get_sla_policy_metric_history_columns() %}

{% set columns = [
    {"name": "_fivetran_synced", "datatype": dbt.type_timestamp()},
    {"name": "business_hours", "datatype": "boolean"},
    {"name": "index", "datatype": dbt.type_int()},
    {"name": "metric", "datatype": dbt.type_string()},
    {"name": "priority", "datatype": dbt.type_string()},
    {"name": "sla_policy_id", "datatype": dbt.type_int()},
    {"name": "sla_policy_updated_at", "datatype": dbt.type_timestamp()},
    {"name": "target", "datatype": dbt.type_int()}
] %}

{{ return(columns) }}

{% endmacro %}