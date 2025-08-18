{% macro get_organization_columns() %}

{% set columns = [
    {"name": "_fivetran_synced", "datatype": dbt.type_timestamp()},
    {"name": "created_at", "datatype": dbt.type_timestamp()},
    {"name": "details", "datatype": dbt.type_int()},
    {"name": "external_id", "datatype": dbt.type_int()},
    {"name": "group_id", "datatype": dbt.type_int()},
    {"name": "id", "datatype": dbt.type_int()},
    {"name": "name", "datatype": dbt.type_string()},
    {"name": "notes", "datatype": dbt.type_int()},
    {"name": "shared_comments", "datatype": "boolean"},
    {"name": "shared_tickets", "datatype": "boolean"},
    {"name": "updated_at", "datatype": dbt.type_timestamp()},
    {"name": "url", "datatype": dbt.type_string()}
] %}

{{ fivetran_utils.add_pass_through_columns(columns, var('zendesk__organization_passthrough_columns')) }}

{{ return(columns) }}

{% endmacro %}
