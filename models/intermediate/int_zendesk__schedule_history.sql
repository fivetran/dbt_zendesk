{{ config(enabled=var('using_schedules', True) and var('using_schedule_histories', False)) }}

with audit_logs as (
    select
        source_relation,
        cast(source_id as {{ dbt.type_string() }}) as schedule_id,
        created_at,
        lower(change_description) as change_description
    from {{ var('audit_log') }}
    where lower(change_description) like '%workweek changed from%'

-- the formats for change_description vary, so it needs to be cleaned
), audit_logs_enhanced as (
    select 
        source_relation,
        schedule_id,
        rank() over (partition by schedule_id, source_relation order by created_at desc) as schedule_id_index,
        created_at,
        -- Clean up the change_description, sometimes has random html stuff in it
        replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(change_description,
            'workweek changed from', ''), 
            '&quot;', '"'), 
            'amp;', ''), 
            '=&gt;', ':'), ':mon:', '"mon":'), ':tue:', '"tue":'), ':wed:', '"wed":'), ':thu:', '"thu":'), ':fri:', '"fri":'), ':sat:', '"sat":'), ':sun:', '"sun":')
            as change_description_cleaned
    from audit_logs

), split_to_from as (
    select
        source_relation,
        schedule_id,
        schedule_id_index,
        created_at,
        cast(created_at as date) as valid_from,
        -- each change_description has two parts: 1-from the old schedule 2-to the new schedule.
        {{ dbt.split_part('change_description_cleaned', "' to '", 1) }} as schedule_change_from,
        {{ dbt.split_part('change_description_cleaned', "' to '", 2) }} as schedule_change
    from audit_logs_enhanced

), find_same_day_changes as (
    select
        source_relation,
        schedule_id,
        schedule_id_index,
        created_at,
        valid_from,
        schedule_change_from,
        schedule_change,
        row_number() over (
            partition by source_relation, schedule_id, valid_from -- valid from is type date
            -- ordering to get the latest change when there are multiple on one day
            order by schedule_id_index, schedule_change_from -- use the length of schedule_change_from to tie break, which will deprioritize empty "from" schedules
        ) as row_number
    from split_to_from

-- multiple changes can occur on one day, so we will keep only the latest change in a day.
), consolidate_same_day_changes as (
    select
        source_relation,
        schedule_id,
        schedule_id_index,
        created_at,
        valid_from,
        lead(valid_from) over (
            partition by source_relation, schedule_id order by schedule_id_index desc) as valid_until,
        schedule_change
    from find_same_day_changes
    where row_number = 1

-- Creates a record for each day of the week for each schedule_change event.
-- This is done by iterating over the days of the week, extracting the corresponding 
-- schedule data for each day, and unioning the results after each iteration.
), split_days as (
    {% set days_of_week = {'sun': 0, 'mon': 1, 'tue': 2, 'wed': 3, 'thu': 4, 'fri': 5, 'sat': 6} %}
    {% for day, day_number in days_of_week.items() %}
    select
        source_relation,
        schedule_id,
        schedule_id_index,
        valid_from,
        valid_until,
        schedule_change,
        '{{ day }}' as day_of_week,
        cast('{{ day_number }}' as {{ dbt.type_int() }}) as day_of_week_number,
        {{ zendesk.regex_extract_schedule_day('schedule_change', day) }} as day_of_week_schedule -- Extracts the schedule data specific to the current day from the schedule_change field.
    from consolidate_same_day_changes
    -- Exclude records with a null valid_until, which indicates it is the current schedule. 
    -- We will to pull in the live schedule downstream, which is necessary when not using schedule histories.
    where valid_until is not null

    {% if not loop.last %}union all{% endif %}
    {% endfor %}

-- A single day may contain multiple start and stop times, so we need to generate a separate record for each.
-- The day_of_week_schedule is structured like a JSON string, requiring warehouse-specific logic to flatten it into individual records.
{% if target.type == 'redshift' %}
-- using PartiQL syntax to work with redshift's SUPER types, which requires an extra CTE
), redshift_parse_schedule as (
    -- Redshift requires another CTE for unnesting 
    select 
        source_relation,
        schedule_id,
        schedule_id_index,
        valid_from,
        valid_until,
        schedule_change,
        day_of_week,
        day_of_week_number,
        day_of_week_schedule,
        json_parse('[' || replace(replace(day_of_week_schedule, ', ', ','), ',', '},{') || ']') as json_schedule

    from split_days
    where day_of_week_schedule != '{}' -- exclude when the day_of_week_schedule in empty. 

), unnested_schedules as (
    select 
        source_relation,
        schedule_id,
        schedule_id_index,
        valid_from,
        valid_until,
        schedule_change,
        day_of_week,
        day_of_week_number,
        -- go back to strings
        cast(day_of_week_schedule as {{ dbt.type_string() }}) as day_of_week_schedule,
        {{ clean_schedule('JSON_SERIALIZE(unnested_schedule)') }} as cleaned_unnested_schedule
    
    from redshift_parse_schedule as schedules, schedules.json_schedule as unnested_schedule

{% else %}
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

    {% else %}
        cast(null as {{ dbt.type_string() }}) as cleaned_unnested_schedule
    from split_days
    {%- endif %}

{% endif %}

-- Each cleaned_unnested_schedule will have the format hh:mm:hh:mm, so we can extract each time part. 
), split_times as (
    select 
        unnested_schedules.*,
        cast(nullif({{ dbt.split_part('cleaned_unnested_schedule', "':'", 1) }}, ' ') as {{ dbt.type_int() }}) as start_time_hh, 
        cast(nullif({{ dbt.split_part('cleaned_unnested_schedule', "':'", 2) }}, ' ') as {{ dbt.type_int() }}) as start_time_mm, 
        cast(nullif({{ dbt.split_part('cleaned_unnested_schedule', "':'", 3) }}, ' ') as {{ dbt.type_int() }}) as end_time_hh, 
        cast(nullif({{ dbt.split_part('cleaned_unnested_schedule', "':'", 4) }}, ' ') as {{ dbt.type_int() }}) as end_time_mm
    from unnested_schedules

-- Calculate the start_time and end_time as minutes from Sunday
), calculate_start_end_times as (
    select
        source_relation,
        schedule_id,
        schedule_id_index,
        start_time_hh * 60 + start_time_mm + 24 * 60 * day_of_week_number as start_time,
        end_time_hh * 60 + end_time_mm + 24 * 60 * day_of_week_number as end_time,
        valid_from,
        valid_until,
        day_of_week,
        day_of_week_number
    from split_times
)

select * 
from calculate_start_end_times