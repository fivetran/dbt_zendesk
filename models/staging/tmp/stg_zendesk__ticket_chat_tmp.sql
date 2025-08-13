{{ config(enabled=var('using_ticket_chat', False)) }}

{{
    zendesk.union_zendesk_connections(
        connection_dictionary=var('zendesks'), 
        single_source_name='zendesk', 
        single_table_name='ticket_chat'
    )
}}