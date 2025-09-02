{% macro get_ticket_columns() %}

{% set columns = [
    {"name": "_fivetran_synced", "datatype": dbt.type_timestamp()},
    {"name": "_fivetran_deleted", "datatype": dbt.type_boolean()},
    {"name": "allow_channelback", "datatype": "boolean"},
    {"name": "assignee_id", "datatype": dbt.type_int()},
    {"name": "brand_id", "datatype": dbt.type_int()},
    {"name": "created_at", "datatype": dbt.type_timestamp()},
    {"name": "description", "datatype": dbt.type_string()},
    {"name": "due_at", "datatype": dbt.type_timestamp()},
    {"name": "external_id", "datatype": dbt.type_int()},
    {"name": "forum_topic_id", "datatype": dbt.type_int()},
    {"name": "group_id", "datatype": dbt.type_int()},
    {"name": "has_incidents", "datatype": "boolean"},
    {"name": "id", "datatype": dbt.type_int()},
    {"name": "is_public", "datatype": "boolean"},
    {"name": "merged_ticket_ids", "datatype": dbt.type_string()},
    {"name": "organization_id", "datatype": dbt.type_int()},
    {"name": "priority", "datatype": dbt.type_string()},
    {"name": "problem_id", "datatype": dbt.type_int()},
    {"name": "recipient", "datatype": dbt.type_int()},
    {"name": "requester_id", "datatype": dbt.type_int()},
    {"name": "status", "datatype": dbt.type_string()},
    {"name": "subject", "datatype": dbt.type_string()},
    {"name": "submitter_id", "datatype": dbt.type_int()},
    {"name": "system_ccs", "datatype": dbt.type_int()},
    {"name": "system_client", "datatype": dbt.type_string()},
    {"name": "system_ip_address", "datatype": dbt.type_string()},
    {"name": "system_json_email_identifier", "datatype": dbt.type_int()},
    {"name": "system_latitude", "datatype": dbt.type_float()},
    {"name": "system_location", "datatype": dbt.type_string()},
    {"name": "system_longitude", "datatype": dbt.type_float()},
    {"name": "system_machine_generated", "datatype": dbt.type_int()},
    {"name": "system_message_id", "datatype": dbt.type_int()},
    {"name": "system_raw_email_identifier", "datatype": dbt.type_int()},
    {"name": "ticket_form_id", "datatype": dbt.type_int()},
    {"name": "type", "datatype": dbt.type_string()},
    {"name": "updated_at", "datatype": dbt.type_string()},
    {"name": "url", "datatype": dbt.type_string()},
    {"name": "via_channel", "datatype": dbt.type_string()},
    {"name": "via_source_from_address", "datatype": dbt.type_int()},
    {"name": "via_source_from_id", "datatype": dbt.type_int()},
    {"name": "via_source_from_title", "datatype": dbt.type_int()},
    {"name": "via_source_rel", "datatype": dbt.type_int()},
    {"name": "via_source_to_address", "datatype": dbt.type_int()},
    {"name": "via_source_to_name", "datatype": dbt.type_int()}
] %}

{{ fivetran_utils.add_pass_through_columns(columns, var('zendesk__ticket_passthrough_columns')) }}

{{ return(columns) }}

{% endmacro %}
