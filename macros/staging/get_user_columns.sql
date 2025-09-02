{% macro get_user_columns() %}

{% set columns = [
    {"name": "_fivetran_synced", "datatype": dbt.type_timestamp()},
    {"name": "_fivetran_deleted", "datatype": dbt.type_boolean()},
    {"name": "active", "datatype": "boolean"},
    {"name": "alias", "datatype": dbt.type_string()},
    {"name": "authenticity_token", "datatype": dbt.type_int()},
    {"name": "chat_only", "datatype": "boolean"},
    {"name": "created_at", "datatype": dbt.type_timestamp()},
    {"name": "details", "datatype": dbt.type_int()},
    {"name": "email", "datatype": dbt.type_string()},
    {"name": "external_id", "datatype": dbt.type_int()},
    {"name": "id", "datatype": dbt.type_int()},
    {"name": "last_login_at", "datatype": dbt.type_timestamp()},
    {"name": "locale", "datatype": dbt.type_string()},
    {"name": "locale_id", "datatype": dbt.type_int()},
    {"name": "moderator", "datatype": "boolean"},
    {"name": "name", "datatype": dbt.type_string()},
    {"name": "notes", "datatype": dbt.type_int()},
    {"name": "only_private_comments", "datatype": "boolean"},
    {"name": "organization_id", "datatype": dbt.type_int()},
    {"name": "phone", "datatype": dbt.type_string()},
    {"name": "remote_photo_url", "datatype": dbt.type_int()},
    {"name": "restricted_agent", "datatype": "boolean"},
    {"name": "role", "datatype": dbt.type_string()},
    {"name": "shared", "datatype": "boolean"},
    {"name": "shared_agent", "datatype": "boolean"},
    {"name": "signature", "datatype": dbt.type_int()},
    {"name": "suspended", "datatype": "boolean"},
    {"name": "ticket_restriction", "datatype": dbt.type_string()},
    {"name": "time_zone", "datatype": dbt.type_string()},
    {"name": "two_factor_auth_enabled", "datatype": "boolean"},
    {"name": "updated_at", "datatype": dbt.type_string()},
    {"name": "url", "datatype": dbt.type_string()},
    {"name": "verified", "datatype": "boolean"}
] %}

{{ fivetran_utils.add_pass_through_columns(columns, var('zendesk__user_passthrough_columns')) }}

{{ return(columns) }}

{% endmacro %}
