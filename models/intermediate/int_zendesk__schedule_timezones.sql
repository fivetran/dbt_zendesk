{{ config(enabled=var('using_schedules', True)) }}

with split_timezones as (
    select *
    from {{ ref('int_zendesk__timezone_daylight') }}  

), schedule as (
    select 
        *,
        max(created_at) over (partition by schedule_id order by created_at) as max_created_at
    from {{ var('schedule') }}   

{% if var('using_schedule_histories', True) %}
), schedule_history as (
    select *
    from {{ ref('int_zendesk__schedule_history') }}  

), schedule_id_timezone as (
    select
        distinct schedule_id,
        lower(time_zone) as time_zone,
        schedule_name
    from schedule
    where created_at = max_created_at

), schedule_history_timezones as (
    select
        schedule_history.*,
        lower(schedule_id_timezone.time_zone) as time_zone,
        schedule_id_timezone.schedule_name
    from schedule_history
    left join schedule_id_timezone
        on schedule_id_timezone.schedule_id = schedule_history.schedule_id
    -- if there is not time_zone match, the schedule has been deleted
    -- we have to filter these records out since time math requires timezone
    -- revisit later if this becomes a bigger issue
    where time_zone is not null
{% endif %}

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

), lag_valid_until as (
    -- sometimes an audit log record is generated but the schedule is actually unchanged.
    -- accumulate group flags to create unique groupings for adjacent periods
    select 
        fill_current_schedule.*,
        lag(schedule_valid_until) over (partition by schedule_id, start_time, end_time 
            order by schedule_valid_from, schedule_valid_until) as previous_valid_until
    from fill_current_schedule

), find_actual_changes as (
    -- sometimes an audit log record is generated but the schedule is actually unchanged.
    -- accumulate group flags to create unique groupings for adjacent periods
    select 
        schedule_id,
        schedule_id_index,
        start_time,
        end_time,
        time_zone,
        schedule_name,
        schedule_valid_from,
        schedule_valid_until,
        -- calculate if this row is adjacent to the previous row
        sum(case when previous_valid_until = schedule_valid_from then 0 else 1 end) 
            over (partition by schedule_id, start_time, end_time order by schedule_valid_from)
            as group_id
    from lag_valid_until

), consolidate_changes as (
    -- consolidate the records by finding the min valid_from and max valid_until for each group
    select 
        schedule_id,
        start_time,
        end_time,
        time_zone,
        schedule_name,
        max(schedule_id_index) as schedule_id_index,
        min(schedule_valid_from) as schedule_valid_from,
        max(schedule_valid_until) as schedule_valid_until
    from find_actual_changes
    {{ dbt_utils.group_by(5) }}

), schedule_timezones as (
    select 
        consolidate_changes.schedule_id,
        consolidate_changes.schedule_id_index,
        consolidate_changes.time_zone,
        consolidate_changes.schedule_name,
        coalesce(split_timezones.offset_minutes, 0) as offset_minutes,
        consolidate_changes.start_time - coalesce(split_timezones.offset_minutes, 0) as start_time_utc,
        consolidate_changes.end_time - coalesce(split_timezones.offset_minutes, 0) as end_time_utc,
        cast(consolidate_changes.schedule_valid_from as {{ dbt.type_timestamp() }}) as schedule_valid_from,
        cast(consolidate_changes.schedule_valid_until as {{ dbt.type_timestamp() }}) as schedule_valid_until,
        -- we'll use these to determine which schedule version to associate tickets with.
        cast({{ dbt.date_trunc('day', 'split_timezones.valid_from') }} as {{ dbt.type_timestamp() }}) as timezone_valid_from,
        cast({{ dbt.date_trunc('day', 'split_timezones.valid_until') }}  as {{ dbt.type_timestamp() }}) as timezone_valid_until
        {# , cast({{ dbt_date.week_start('split_timezones.valid_from','UTC') }} as {{ dbt.type_timestamp() }}) as timezone_starting_sunday,
        cast({{ dbt_date.week_start('split_timezones.valid_until','UTC') }} as {{ dbt.type_timestamp() }}) as timezone_ending_sunday #}
    from consolidate_changes
    left join split_timezones
        on split_timezones.time_zone = consolidate_changes.time_zone

), filter_schedule_timezones as (
    select 
        schedule_timezones.*,
        case when schedule_valid_until > timezone_valid_until
            then true else false
            end as is_timezone_spillover
    from schedule_timezones
    -- timezone that a schedule start falls within
    where (schedule_valid_from >= timezone_valid_from and schedule_valid_from < timezone_valid_until)
    -- timezone that a schedule end falls within
    or (schedule_valid_until >= timezone_valid_from and schedule_valid_until < timezone_valid_until)
    -- for schedules that span a long time, also find timezones that fall completely within the bounds of the schedule
    or (timezone_valid_from >= schedule_valid_from and timezone_valid_until < schedule_valid_until)

), assemble_schedule_timezones as (
    select
        schedule_id,
        schedule_id_index,
        time_zone,
        schedule_name,
        offset_minutes,
        start_time_utc,
        end_time_utc,
        case
            when schedule_valid_from >= timezone_valid_from and schedule_valid_from < timezone_valid_until
            then schedule_valid_from
            when schedule_valid_until >= timezone_valid_from and schedule_valid_until < timezone_valid_until
            then timezone_valid_from
            when timezone_valid_from >= schedule_valid_from and timezone_valid_until < schedule_valid_until
            then timezone_valid_from
        end as schedule_valid_from,
        case
            when schedule_valid_until >= timezone_valid_from and schedule_valid_until < timezone_valid_until
            then schedule_valid_until
            when schedule_valid_from >= timezone_valid_from and schedule_valid_from < timezone_valid_until
            then timezone_valid_until
            when timezone_valid_from >= schedule_valid_from and timezone_valid_until < schedule_valid_until
            then timezone_valid_until
        end as schedule_valid_until
    from filter_schedule_timezones
)

select * 
from assemble_schedule_timezones