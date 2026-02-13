{% macro get_ticket_sla_policy_columns() %}

{% set columns = [
    {"name": "_fivetran_synced", "datatype": dbt.type_timestamp()},
    {"name": "policy_applied_at", "datatype": dbt.type_timestamp()},
    {"name": "sla_policy_id", "datatype": dbt.type_int()},
    {"name": "ticket_id", "datatype": dbt.type_int()}
] %}

{{ return(columns) }}

{% endmacro %}
