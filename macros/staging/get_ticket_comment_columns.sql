{% macro get_ticket_comment_columns() %}

{% set columns = [
    {"name": "_fivetran_synced", "datatype": dbt.type_string()},
    {"name": "_fivetran_deleted", "datatype": dbt.type_boolean()},
    {"name": "body", "datatype": dbt.type_string()},
    {"name": "call_duration", "datatype": dbt.type_int()},
    {"name": "call_id", "datatype": dbt.type_int()},
    {"name": "created", "datatype": dbt.type_timestamp()},
    {"name": "facebook_comment", "datatype": "boolean"},
    {"name": "id", "datatype": dbt.type_int()},
    {"name": "location", "datatype": dbt.type_int()},
    {"name": "public", "datatype": "boolean"},
    {"name": "recording_url", "datatype": dbt.type_int()},
    {"name": "started_at", "datatype": dbt.type_timestamp()},
    {"name": "ticket_id", "datatype": dbt.type_int()},
    {"name": "transcription_status", "datatype": dbt.type_int()},
    {"name": "transcription_text", "datatype": dbt.type_int()},
    {"name": "trusted", "datatype": dbt.type_int()},
    {"name": "tweet", "datatype": "boolean"},
    {"name": "user_id", "datatype": dbt.type_int()},
    {"name": "voice_comment", "datatype": "boolean"},
    {"name": "voice_comment_transcription_visible", "datatype": dbt.type_int()}
] %}

{{ return(columns) }}

{% endmacro %}
