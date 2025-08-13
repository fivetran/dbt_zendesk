{{ config(enabled=var('using_ticket_chat', False)) }}

{{
    zendesk.union_zendesk_connections(
        connection_dictionary=var('zendesk'), 
        single_source_name='zendesk', 
        single_table_name='ticket_chat_event'
    )
}}