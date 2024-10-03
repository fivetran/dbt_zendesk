{{ config(enabled=var('using_schedules', True) and var('using_schedule_histories', True)) }}

with audit_logs as (
    select
        cast(source_id as {{ dbt.type_string() }}) as schedule_id,
        created_at,
        lower(change_description) as change_description
    from {{ var('audit_log') }}
    where lower(change_description) like '%workweek changed from%'

), audit_logs_enhanced as (
    select
        schedule_id,
        created_at,
        replace(replace(replace(replace(change_description,
            'workweek changed from', ''), 
            '&quot;', '"'), 
            'amp;', ''), 
            '=&gt;', ':')
            as change_description_cleaned
    from audit_logs

), split_to_from as (
    select
        schedule_id,
        created_at as valid_from,
        lead(created_at) over (
            partition by schedule_id order by created_at) as valid_until,
        -- we only need what the schedule was changed to
        {{ dbt.split_part('change_description_cleaned', "' to '", 2) }} as schedule_change
    from audit_logs_enhanced

), consolidate_same_day_changes as (
    select
        split_to_from.*
    from split_to_from
    -- Filter out schedules with multiple changes in a day to keep the current one
    where cast(valid_from as date) != cast(valid_until as date)
    and valid_until is not null

), split_days as (
    {% set days_of_week = {'sun': 0, 'mon': 1, 'tue': 2, 'wed': 3, 'thu': 4, 'fri': 5, 'sat': 6} %}
    {% for day, day_number in days_of_week.items() %}
    select
        consolidate_same_day_changes.*,
        '{{ day }}' as day_of_week,
        cast('{{ day_number }}' as {{ dbt.type_int() }}) as day_of_week_number,
        {{ zendesk.regex_extract('schedule_change', "'.*?" ~ day ~ ".*?({.*?})'") }} as day_of_week_schedule
    from consolidate_same_day_changes
    {% if not loop.last %}union all{% endif %}
    {% endfor %}

), unnested_schedules as (
    select
        split_days.*,

{%- if target.type == 'bigquery' %}
        {{ clean_schedule('unnested_schedule') }} as cleaned_unnested_schedule
    from split_days
    cross join unnest(json_extract_array('[' || replace(day_of_week_schedule, ',', '},{') || ']', '$')) as unnested_schedule

{%- elif target.type == 'snowflake' %}
        unnested_schedule.key || ':' || unnested_schedule.value as cleaned_unnested_schedule
    from split_days
    cross join lateral flatten(input => parse_json(replace(replace(day_of_week_schedule, '\}\}', '\}'), '\{\{', '\{'))) as unnested_schedule

{%- elif target.type == 'postgres' %}
        {{ clean_schedule('unnested_schedule::text') }} as cleaned_unnested_schedule
    from split_days
    cross join lateral jsonb_array_elements(('[' || replace(day_of_week_schedule, ',', '},{') || ']')::jsonb) as unnested_schedule

{%- elif target.type in ('databricks', 'spark') %}
        {{ clean_schedule('unnested_schedule') }} as cleaned_unnested_schedule
    from split_days
    lateral view explode(from_json(concat('[', replace(day_of_week_schedule, ',', '},{'), ']'), 'array<string>')) as unnested_schedule

{%- elif target.type == 'redshift' %}
    {# json_parse('[' || replace(replace(day_of_week_schedule, '\}\}', '\}'), '\{\{', '\{') || ']') as json_schedule
    from split_days #}
    {# cross join lateral json_parse(replace(replace(day_of_week_schedule, '\}\}', '\}'), '\{\{', '\{')) as element #}

        cast(null as {{ dbt.type_string() }}) as cleaned_unnested_schedule
    from split_days

{% else %}
        cast(null as {{ dbt.type_string() }}) as cleaned_unnested_schedule
    from split_days
{%- endif %}

), split_times as (

    select 
        unnested_schedules.*,
        cast(nullif({{ dbt.split_part('cleaned_unnested_schedule', "':'", 1) }}, ' ') as {{ dbt.type_int() }}) as start_time_hh, 
        cast(nullif({{ dbt.split_part('cleaned_unnested_schedule', "':'", 2) }}, ' ') as {{ dbt.type_int() }}) as start_time_mm, 
        cast(nullif({{ dbt.split_part('cleaned_unnested_schedule', "':'", 3) }}, ' ') as {{ dbt.type_int() }}) as end_time_hh, 
        cast(nullif({{ dbt.split_part('cleaned_unnested_schedule', "':'", 4) }}, ' ') as {{ dbt.type_int() }}) as end_time_mm
    from unnested_schedules

), calculate_start_end_times as (

    select
        schedule_id,
        start_time_hh * 60 + start_time_mm + 24 * 60 * day_of_week_number as start_time,
        end_time_hh * 60 + end_time_mm + 24 * 60 * day_of_week_number as end_time,
        valid_from,
        valid_until,
        cast({{ dbt.date_trunc('day', 'valid_from') }} as {{ dbt.type_timestamp() }}) as valid_from_day,
        cast({{ dbt.date_trunc('day', 'valid_until') }} as {{ dbt.type_timestamp() }}) as valid_until_day,
        day_of_week,
        day_of_week_number
    from split_times
)

select * 
from calculate_start_end_times
