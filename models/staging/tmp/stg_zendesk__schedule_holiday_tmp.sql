--To disable this model, set the using_schedules or using_holidays variable within your dbt_project.yml file to False.
{{ config(enabled=var('using_schedules', True) and var('using_holidays', True)) }}

select *
from {{ var('schedule_holiday')}}