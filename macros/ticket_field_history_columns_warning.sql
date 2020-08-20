{% macro ticket_field_history_columns_warning() %}

{% if not var('ticket_field_history_columns') %}
{{ log(
    """
    Warning: You have passed an empty list to the 'ticket_field_history_columns'.
    As a result, you won't see the history of any columns in the 'zendesk_ticket_field_history' model.
    """,
    info=True
) }}
{% endif %}

{% endmacro %}