--To enable this model, set the using_sla_policy_metric_history variable within your dbt_project.yml file to True.
{{ config(enabled=var('using_sla_policy_metric_history', False)) }}

{{
    zendesk.union_zendesk_connections(
        connection_dictionary=var('zendesk_sources'),
        single_source_name='zendesk',
        single_table_name='sla_policy_metric_history'
    )
}}
