{{ config(enabled=var('using_ticket_chat', False)) }}

{{
    fivetran_utils.union_connections(
        connection_dictionary='zendesk_sources', 
        single_source_name='zendesk', 
        single_table_name='ticket_chat'
    )
}}