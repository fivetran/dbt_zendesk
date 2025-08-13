{% macro get_ticket_chat_columns() %}

{% set columns = [
    {"name": "_fivetran_synced", "datatype": dbt.type_timestamp()},
    {"name": "authenticated", "datatype": dbt.type_boolean()},
    {"name": "backend", "datatype": dbt.type_string()},
    {"name": "channel", "datatype": dbt.type_string()},
    {"name": "chat_id", "datatype": dbt.type_string()},
    {"name": "conversation_id", "datatype": dbt.type_string()},
    {"name": "initiator", "datatype": dbt.type_string()},
    {"name": "integration_id", "datatype": dbt.type_string()},
    {"name": "ticket_id", "datatype": dbt.type_int()},
    {"name": "user_id", "datatype": dbt.type_int()},
    {"name": "visitor_id", "datatype": dbt.type_string()}
] %}

{{ return(columns) }}

{% endmacro %}
