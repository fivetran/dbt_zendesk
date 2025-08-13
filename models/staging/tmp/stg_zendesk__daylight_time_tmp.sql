--To disable this model, set the using_schedules variable within your dbt_project.yml file to False.
{{ config(enabled=var('using_schedules', True)) }}

{{
    zendesk.union_zendesk_connections(
        connection_dictionary=var('zendesk'), 
        single_source_name='zendesk', 
        single_table_name='daylight_time'
    )
}}