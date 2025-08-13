--To disable this model, set the using_organization_tags variable within your dbt_project.yml file to False.
{{ config(enabled=var('using_organization_tags', True) and var('using_organizations', True)) }}

{{
    zendesk.union_zendesk_connections(
        connection_dictionary=var('zendesk_sources'), 
        single_source_name='zendesk', 
        single_table_name='organization_tag'
    )
}}