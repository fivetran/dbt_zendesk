{{ config(enabled=var('using_schedules', True)) }}

with split_timezones as (
    select *
    from {{ ref('int_zendesk__timezone_daylight') }}  

), schedule as (
    select 
        *,
        max(created_at) over (partition by schedule_id) as max_created_at
    from {{ var('schedule') }}   

{% if var('using_schedule_histories', True) %}
), schedule_history as (
    select *
    from {{ ref('int_zendesk__schedule_history') }}  

-- Select the most recent timezone associated with each schedule based on 
-- the max_created_at timestamp. Historical timezone changes are not yet tracked.
), schedule_id_timezone as (
    select
        distinct schedule_id,
        lower(time_zone) as time_zone,
        schedule_name
    from schedule
    where created_at = max_created_at

-- Combine historical schedules with the most recent timezone data. Filter 
-- out records where the timezone is missing, indicating the schedule has 
-- been deleted.
), schedule_history_timezones as (
    select
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
    -- We have to filter these records out since time math requires timezone
    -- revisit later if this becomes a bigger issue
    where time_zone is not null
{% endif %}

-- Combine current schedules with historical schedules, marking if each 
-- record is historical. Adjust the valid_from and valid_until dates accordingly.
), union_schedule_histories as (
    select
        schedule_id,
        0 as schedule_id_index,
        created_at,
        start_time,
        end_time,
        lower(time_zone) as time_zone,
        schedule_name,
        cast(null as date) as valid_from, -- created_at is when the schedule was first ever created, so we'll fill the real value later
        cast({{ dbt.dateadd('year', 1, dbt.current_timestamp_backcompat()) }} as date) as valid_until,
        False as is_historical
    from schedule

{% if var('using_schedule_histories', True) %}
    union all

    select
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
{% endif %}

-- Set the schedule_valid_from for current schedules based on the most recent historical row.
-- This allows the current schedule to pick up where the historical schedule left off.
), fill_current_schedule as (
    select
        schedule_id,
        schedule_id_index,
        start_time,
        end_time,
        time_zone,
        schedule_name,
        coalesce(case
            when not is_historical
            -- get max valid_until from historical rows in the same schedule
            then max(case when is_historical then valid_until end) 
                over (partition by schedule_id)
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
        lag(schedule_valid_until) over (partition by schedule_id, start_time, end_time 
            order by schedule_valid_from, schedule_valid_until) as previous_valid_until
    from fill_current_schedule

-- Identify unique schedule groupings by detecting gaps between adjacent time 
-- periods to group unchanged records for filtering later.
), find_actual_changes as (
    select 
        schedule_id,
        schedule_id_index,
        start_time,
        end_time,
        time_zone,
        schedule_name,
        schedule_valid_from,
        schedule_valid_until,
        sum(case when previous_valid_until = schedule_valid_from then 0 else 1 end) -- find if this row is adjacent to the previous row
            over (partition by schedule_id, start_time, end_time 
                order by schedule_valid_from
                rows between unbounded preceding and current row)
        as group_id
    from lag_valid_until

-- Consolidate records into continuous periods by finding the minimum 
-- valid_from and maximum valid_until for each group of unchanged schedules.
), consolidate_changes as (
    select 
        schedule_id,
        start_time,
        end_time,
        time_zone,
        schedule_name,
        max(schedule_id_index) as schedule_id_index, --this is arbitrary, but helps with keeping groups together downstream.
        min(schedule_valid_from) as schedule_valid_from,
        max(schedule_valid_until) as schedule_valid_until
    from find_actual_changes
    {{ dbt_utils.group_by(5) }}

-- Reset the schedule_valid_from date for the "default schedule" to 1970-01-01
-- for downstream models referencing this schedule. See int_zendesk__ticket_schedules.
), reset_schedule_start as (
    select
        schedule_id,
        schedule_id_index,
        time_zone,
        schedule_name,
        start_time,
        end_time,
        -- this is for the 'default schedule' (see used in int_zendesk__ticket_schedules)
        case 
            when schedule_valid_from = min(schedule_valid_from) over () then '1970-01-01'
            else schedule_valid_from
        end as schedule_valid_from,
        schedule_valid_until
    from consolidate_changes

-- Adjust the schedule times to UTC by applying the timezone offset. Join all possible
-- time_zone matches for each schedule. The erroneous timezones will be filtered next.
), schedule_timezones as (
    select 
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

-- Assemble the final schedule-timezone relationship by determining the correct 
-- schedule_valid_from and schedule_valid_until based on overlapping periods 
-- between the schedule and timezone. 
), assemble_schedule_timezones as (
    select
        schedule_id,
        schedule_id_index,
        time_zone,
        schedule_name,
        offset_minutes,
        start_time_utc,
        end_time_utc,
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
)

select * 
from assemble_schedule_timezones