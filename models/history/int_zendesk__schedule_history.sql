{{ config(enabled=var('using_schedules', true) and var('using_schedule_histories', true)) }}

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
        row_number() over (partition by schedule_id order by created_at) as schedule_id_index,
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
        schedule_id,
        schedule_id_index,
        created_at as valid_from,
        lead(created_at) over (
            partition by schedule_id order by schedule_id_index) as valid_until,
        -- we only need what the schedule was changed to
        {{ dbt.split_part('change_description_cleaned', "' to '", 2) }} as schedule_change
    from audit_logs_enhanced

), find_same_day_changes as (
    select
        schedule_id,
        schedule_id_index,
        cast(valid_from as date) as valid_from,
        cast(valid_until as date) as valid_until,
        schedule_change,
        row_number() over (
            partition by schedule_id, cast(valid_from as date)
            -- ordering to get the latest change when there are multiple on one day
            order by valid_from desc, coalesce(valid_until, {{ dbt.current_timestamp_backcompat() }}) desc
        ) as row_number
    from split_to_from

), consolidate_same_day_changes as (
    select
        schedule_id,
        schedule_id_index,
        valid_from,
        valid_until,
        schedule_change,
        -- for use in the next cte
        lag(valid_until) over (partition by schedule_id, schedule_change order by valid_from, valid_until) as previous_valid_until
    from find_same_day_changes
    where row_number = 1
    -- we don't want the most current schedule since it would be captured by the live schedule. we want to use the live schedule in case we're not using histories.
    and valid_until is not null

), find_actual_changes as (
    -- sometimes an audit log record is generated but the schedule is actually unchanged.
    -- accumulate group flags to create unique groupings for adjacent periods
    select 
        schedule_id,
        schedule_id_index,
        valid_from,
        valid_until,
        schedule_change,
        -- calculate if this row is adjacent to the previous row
        sum(case when previous_valid_until = valid_from then 0 else 1 end) 
            over (partition by schedule_id, schedule_change 
                order by valid_from 
                rows between unbounded preceding and current row) -- Redshift needs this frame clause for aggregating
        as group_id
    from consolidate_same_day_changes

), consolidate_actual_changes as (
    -- consolidate the records by finding the min valid_from and max valid_until for each group
    select 
        schedule_id,
        group_id,
        schedule_change,
        max(schedule_id_index) as schedule_id_index,
        min(valid_from) as valid_from,
        max(valid_until) as valid_until
    from find_actual_changes
    {{ dbt_utils.group_by(3) }}

-- now that the schedule changes are cleaned, we can split into the individual schedules periods
), split_days as (
    {% set days_of_week = {'sun': 0, 'mon': 1, 'tue': 2, 'wed': 3, 'thu': 4, 'fri': 5, 'sat': 6} %}
    {% for day, day_number in days_of_week.items() %}
    select
        schedule_id,
        schedule_id_index,
        valid_from,
        valid_until,
        schedule_change,
        '{{ day }}' as day_of_week,
        cast('{{ day_number }}' as {{ dbt.type_int() }}) as day_of_week_number,
        {{ zendesk.regex_extract('schedule_change', day) }} as day_of_week_schedule
    from consolidate_same_day_changes

    {% if not loop.last %}union all{% endif %}
    {% endfor %}

{% if target.type == 'redshift' %}
-- using PartiQL syntax to work with redshift's SUPER types, which requires an extra CTE
), redshift_parse_schedule as (
    -- Redshift requires another CTE for unnesting 
    select 
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
    where day_of_week_schedule != '{}'

), unnested_schedules as (
    select 
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