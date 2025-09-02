{% macro get_ticket_chat_event_columns() %}

{% set columns = [
    {"name": "_fivetran_synced", "datatype": dbt.type_timestamp()},
    {"name": "actor_id", "datatype": dbt.type_string()},
    {"name": "chat_id", "datatype": dbt.type_string()},
    {"name": "chat_index", "datatype": dbt.type_int()},
    {"name": "created_at", "datatype": dbt.type_timestamp()},
    {"name": "external_message_id", "datatype": dbt.type_string()},
    {"name": "filename", "datatype": dbt.type_string()},
    {"name": "is_history_context", "datatype": dbt.type_boolean()},
    {"name": "message", "datatype": dbt.type_string()},
    {"name": "message_id", "datatype": dbt.type_string()},
    {"name": "message_source", "datatype": dbt.type_string()},
    {"name": "mime_type", "datatype": dbt.type_string()},
    {"name": "original_message_type", "datatype": dbt.type_string()},
    {"name": "parent_message_id", "datatype": dbt.type_string()},
    {"name": "reason", "datatype": dbt.type_string()},
    {"name": "size", "datatype": dbt.type_int()},
    {"name": "status", "datatype": dbt.type_string()},
    {"name": "status_updated_at", "datatype": dbt.type_timestamp()},
    {"name": "type", "datatype": dbt.type_string()},
    {"name": "url", "datatype": dbt.type_string()}
] %}

{{ return(columns) }}

{% endmacro %}
