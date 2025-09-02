--To disable this model, set the using_schedules or using_holidays variable within your dbt_project.yml file to False.
{{ config(enabled=var('using_schedules', True) and var('using_holidays', True)) }}

{{
    zendesk.union_zendesk_connections(
        connection_dictionary=var('zendesk_sources'), 
        single_source_name='zendesk', 
        single_table_name='schedule_holiday'
    )
}}