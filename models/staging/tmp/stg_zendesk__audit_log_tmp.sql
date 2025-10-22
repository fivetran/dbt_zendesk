{{ config(enabled=var('using_audit_log', False)) }}

{{
    zendesk.union_zendesk_connections(
        connection_dictionary='zendesk_sources', 
        single_source_name='zendesk', 
        single_table_name='audit_log'
    )
}}