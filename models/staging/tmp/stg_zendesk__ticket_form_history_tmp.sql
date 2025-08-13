--To disable this model, set the using_ticket_form_history variable within your dbt_project.yml file to False.
{{ config(enabled=var('using_ticket_form_history', True)) }}

{{
    zendesk.union_zendesk_connections(
        connection_dictionary=var('zendesk'), 
        single_source_name='zendesk', 
        single_table_name='ticket_form_history'
    )
}}