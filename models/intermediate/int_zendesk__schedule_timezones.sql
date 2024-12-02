{{ config(enabled=var('using_schedules', True)) }}

with split_timezones as (
    select *
    from {{ ref('int_zendesk__timezone_daylight') }}  

), schedule as (
    select 
        *,
        max(created_at) over (partition by source_relation, schedule_id) as max_created_at
    from {{ var('schedule') }}   

{% if var('using_schedule_histories', False) %}
), schedule_history as (
    select *
    from {{ ref('int_zendesk__schedule_history') }}  

-- Select the most recent timezone associated with each schedule based on 
-- the max_created_at timestamp. Historical timezone changes are not yet tracked.
), schedule_id_timezone as (
    select
        distinct schedule_id, source_relation,
        lower(time_zone) as time_zone,
        schedule_name
    from schedule
    where created_at = max_created_at

-- Combine historical schedules with the most recent timezone data. Filter 
-- out records where the timezone is missing, indicating the schedule has 
-- been deleted.
), schedule_history_timezones as (
    select
        schedule_history.source_relation,
        schedule_history.schedule_id,
        schedule_history.schedule_id_index,
        schedule_history.start_time,
        schedule_history.end_time,
        schedule_history.valid_from,
        schedule_history.valid_until,
        lower(schedule_id_timezone.time_zone) as time_zone,
        schedule_id_timezone.schedule_name
    from schedule_history
    left join schedule_id_timezone
        on schedule_id_timezone.schedule_id = schedule_history.schedule_id
        and schedule_id_timezone.source_relation = schedule_history.source_relation
    -- We have to filter these records out since time math requires timezone
    -- revisit later if this becomes a bigger issue
    where time_zone is not null

-- Combine current schedules with historical schedules. Adjust the valid_from and valid_until dates accordingly.
), union_schedule_histories as (
    select
        source_relation,
        schedule_id,
        0 as schedule_id_index, -- set the index as 0 for the current schedule
        created_at,
        start_time,
        end_time,
        lower(time_zone) as time_zone,
        schedule_name,
        cast(null as date) as valid_from, -- created_at is when the schedule was first ever created, so we'll fill this value later
        cast({{ dbt.dateadd('day', 7, dbt.current_timestamp()) }} as date) as valid_until,
        False as is_historical
    from schedule

    union all

    select
        source_relation,
        schedule_id,
        schedule_id_index,
        cast(null as {{ dbt.type_timestamp() }}) as created_at,
        start_time,
        end_time,
        time_zone,
        schedule_name,
        cast(valid_from as date) as valid_from,
        cast(valid_until as date) as valid_until,
        True as is_historical
    from schedule_history_timezones

-- Set the schedule_valid_from for current schedules based on the most recent historical row.
-- This allows the current schedule to pick up where the historical schedule left off.
), fill_current_schedule as (
    select
        source_relation,
        schedule_id,
        schedule_id_index,
        start_time,
        end_time,
        time_zone,
        schedule_name,
        coalesce(case
            when schedule_id_index = 0
            -- get max valid_until from historical rows in the same schedule
            then max(case when schedule_id_index > 0 then valid_until end) 
                over (partition by source_relation, schedule_id)
            else valid_from
            end,
            cast(created_at as date))
        as schedule_valid_from,
        valid_until as schedule_valid_until
    from union_schedule_histories

-- Detect adjacent time periods by lagging the schedule_valid_until value 
-- to identify effectively unchanged schedules.
), lag_valid_until as (
    select 
        fill_current_schedule.*,
        lag(schedule_valid_until) over (partition by source_relation, schedule_id, start_time, end_time 
            order by schedule_valid_from, schedule_valid_until) as previous_valid_until
    from fill_current_schedule

-- Identify distinct schedule groupings based on schedule_id, start_time, and end_time.
-- Consolidate only adjacent schedules; if a schedule changes and later reverts to its original time, 
-- we want to maintain the intermediate schedule change.
), find_actual_changes as (
    select 
        source_relation,
        schedule_id,
        schedule_id_index,
        start_time,
        end_time,
        time_zone,
        schedule_name,
        schedule_valid_from,
        schedule_valid_until,

    -- The group_id increments only when there is a gap between the previous schedule's 
    -- valid_until and the current schedule's valid_from, signaling the schedules are not adjacent.
    -- Adjacent schedules with the same start_time and end_time are grouped together, 
    -- while non-adjacent schedules are treated as separate groups.
        sum(case when previous_valid_until = schedule_valid_from then 0 else 1 end) -- find if this row is adjacent to the previous row
            over (partition by source_relation, schedule_id, start_time, end_time 
                order by schedule_valid_from
                rows between unbounded preceding and current row)
        as group_id
    from lag_valid_until

-- Consolidate records into continuous periods by finding the minimum 
-- valid_from and maximum valid_until for each group.
), consolidate_changes as (
    select 
        source_relation,
        schedule_id,
        start_time,
        end_time,
        time_zone,
        schedule_name,
        group_id,
        min(schedule_id_index) as schedule_id_index, --helps with tracking downstream.
        min(schedule_valid_from) as schedule_valid_from,
        max(schedule_valid_until) as schedule_valid_until
    from find_actual_changes
    {{ dbt_utils.group_by(7) }}

-- For each schedule_id, reset the earliest schedule_valid_from date to 1970-01-01 for full schedule coverage.
), reset_schedule_start as (
    select
        source_relation,
        schedule_id,
        schedule_id_index,
        time_zone,
        schedule_name,
        start_time,
        end_time,
        case 
            when schedule_valid_from = min(schedule_valid_from) over (partition by source_relation, schedule_id) then '1970-01-01'
            else schedule_valid_from
        end as schedule_valid_from,
        schedule_valid_until
    from consolidate_changes

-- Adjust the schedule times to UTC by applying the timezone offset. Join all possible
-- time_zone matches for each schedule. The erroneous timezones will be filtered next.
), schedule_timezones as (
    select 
        reset_schedule_start.source_relation,
        reset_schedule_start.schedule_id,
        reset_schedule_start.schedule_id_index,
        reset_schedule_start.time_zone,
        reset_schedule_start.schedule_name,
        coalesce(split_timezones.offset_minutes, 0) as offset_minutes,
        reset_schedule_start.start_time - coalesce(split_timezones.offset_minutes, 0) as start_time_utc,
        reset_schedule_start.end_time - coalesce(split_timezones.offset_minutes, 0) as end_time_utc,
        cast(reset_schedule_start.schedule_valid_from as {{ dbt.type_timestamp() }}) as schedule_valid_from,
        cast(reset_schedule_start.schedule_valid_until as {{ dbt.type_timestamp() }}) as schedule_valid_until,
        -- we'll use these to determine which schedule version to associate tickets with.
        cast({{ dbt.date_trunc('day', 'split_timezones.valid_from') }} as {{ dbt.type_timestamp() }}) as timezone_valid_from,
        cast({{ dbt.date_trunc('day', 'split_timezones.valid_until') }}  as {{ dbt.type_timestamp() }}) as timezone_valid_until
    from reset_schedule_start
    left join split_timezones
        on split_timezones.time_zone = reset_schedule_start.time_zone
        and split_timezones.source_relation = reset_schedule_start.source_relation

-- Assemble the final schedule-timezone relationship by determining the correct 
-- schedule_valid_from and schedule_valid_until based on overlapping periods 
-- between the schedule and timezone. 
), final_schedule as (
    select
        source_relation,
        schedule_id,
        schedule_id_index,
        time_zone,
        schedule_name,
        offset_minutes,
        start_time_utc,
        end_time_utc,
        timezone_valid_from,
        timezone_valid_until,
    -- Be very careful if changing the order of these case whens--it does matter!
        case
            -- timezone that a schedule start falls within
            when schedule_valid_from >= timezone_valid_from and schedule_valid_from < timezone_valid_until
            then schedule_valid_from
            -- timezone that a schedule end falls within
            when schedule_valid_until >= timezone_valid_from and schedule_valid_until < timezone_valid_until
            then timezone_valid_from
            -- timezones that fall completely within the bounds of the schedule
            when timezone_valid_from >= schedule_valid_from and timezone_valid_until < schedule_valid_until
            then timezone_valid_from
        end as schedule_valid_from,
        case
            -- timezone that a schedule end falls within
            when schedule_valid_until >= timezone_valid_from and schedule_valid_until < timezone_valid_until
            then schedule_valid_until
            -- timezone that a schedule start falls within
            when schedule_valid_from >= timezone_valid_from and schedule_valid_from < timezone_valid_until
            then timezone_valid_until
            -- timezones that fall completely within the bounds of the schedule
            when timezone_valid_from >= schedule_valid_from and timezone_valid_until < schedule_valid_until
            then timezone_valid_until
        end as schedule_valid_until

    from schedule_timezones

    -- Filter records based on whether the schedule periods overlap with timezone periods. Capture
    -- when a schedule start or end falls within a time zone, and also capture timezones that exist
    -- entirely within the bounds of a schedule. 
    -- timezone that a schedule start falls within
    where (schedule_valid_from >= timezone_valid_from and schedule_valid_from < timezone_valid_until)
    -- timezone that a schedule end falls within
    or (schedule_valid_until >= timezone_valid_from and schedule_valid_until < timezone_valid_until)
    -- timezones that fall completely within the bounds of the schedule
    or (timezone_valid_from >= schedule_valid_from and timezone_valid_until < schedule_valid_until)

{% else %} -- when not using schedule histories
), final_schedule as (
    select 
        schedule.source_relation,
        schedule.schedule_id,
        0 as schedule_id_index,
        lower(schedule.time_zone) as time_zone,
        schedule.schedule_name,
        coalesce(split_timezones.offset_minutes, 0) as offset_minutes,
        schedule.start_time - coalesce(split_timezones.offset_minutes, 0) as start_time_utc,
        schedule.end_time - coalesce(split_timezones.offset_minutes, 0) as end_time_utc,
        cast({{ dbt.date_trunc('day', 'split_timezones.valid_from') }} as {{ dbt.type_timestamp() }}) as schedule_valid_from,
        cast({{ dbt.date_trunc('day', 'split_timezones.valid_until') }}  as {{ dbt.type_timestamp() }}) as schedule_valid_until,
        cast({{ dbt.date_trunc('day', 'split_timezones.valid_from') }} as {{ dbt.type_timestamp() }}) as timezone_valid_from,
        cast({{ dbt.date_trunc('day', 'split_timezones.valid_until') }}  as {{ dbt.type_timestamp() }}) as timezone_valid_until
    from schedule
    left join split_timezones
        on split_timezones.time_zone = lower(schedule.time_zone)
        and schedule.source_relation = split_timezones.source_relation
{% endif %}

), final as (
    select
        source_relation,
        schedule_id,
        schedule_id_index,
        time_zone,
        schedule_name,
        offset_minutes,
        start_time_utc,
        end_time_utc,
        schedule_valid_from,
        schedule_valid_until,
        -- use dbt_date.week_start to ensure we truncate to Sunday
        cast({{ dbt_date.week_start('schedule_valid_from','UTC') }} as {{ dbt.type_timestamp() }}) as schedule_starting_sunday,
        cast({{ dbt_date.week_start('schedule_valid_until','UTC') }} as {{ dbt.type_timestamp() }}) as schedule_ending_sunday,
        -- Check if the start fo the schedule was from a schedule or timezone change for tracking downstream.
        case when schedule_valid_from = timezone_valid_from
            then 'timezone'
            else 'schedule'
            end as change_type
    from final_schedule
)

select * 
from final