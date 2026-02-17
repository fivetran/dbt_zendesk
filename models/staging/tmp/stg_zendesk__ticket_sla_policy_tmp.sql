--To disable this model, set the using_ticket_sla_policy variable within your dbt_project.yml file to False.
{{ config(enabled=var('using_ticket_sla_policy', True)) }}

{{
    zendesk.union_zendesk_connections(
        connection_dictionary=var('zendesk_sources'),
        single_source_name='zendesk',
        single_table_name='ticket_sla_policy'
    )
}}
