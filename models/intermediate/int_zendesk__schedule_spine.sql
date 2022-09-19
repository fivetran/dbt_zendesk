{{ config(enabled=var('using_schedules', True)) }}

/*
    The purpose of this model is to create a spine of appropriate timezone offsets to use for schedules, as offsets may change due to Daylight Savings.
    End result will include `valid_from` and `valid_until` columns which we will use downstream to determine which schedule-offset to associate with each ticket (ie standard time vs daylight time)
*/

with timezone as (

    select *
    from {{ var('time_zone') }}

), daylight_time as (

    select *
    from {{ var('daylight_time') }}

), schedule as (

    select *
    from {{ var('schedule') }}   

), timezone_with_dt as (

    select 
        timezone.*,
        daylight_time.daylight_start_utc,
        daylight_time.daylight_end_utc,
        daylight_time.daylight_offset_minutes

    from timezone 
    left join daylight_time 
        on timezone.time_zone = daylight_time.time_zone

), order_timezone_dt as (

    select 
        *,
        -- will be null for timezones without any daylight savings records (and the first entry)
        -- we will coalesce the first entry date with .... the X years ago
        lag(daylight_end_utc, 1) over (partition by time_zone order by daylight_end_utc asc) as last_daylight_end_utc,
        -- will be null for timezones without any daylight savings records (and the last entry)
        -- we will coalesce the last entry date with the current date 
        lead(daylight_start_utc, 1) over (partition by time_zone order by daylight_start_utc asc) as next_daylight_start_utc

    from timezone_with_dt

), split_timezones as (

    -- standard schedule (includes timezones without DT)
    -- starts: when the last Daylight Savings ended
    -- ends: when the next Daylight Savings starts
    select 
        time_zone,
        standard_offset_minutes as offset_minutes,

        -- last_daylight_end_utc is null for the first record of the time_zone's daylight time, or if the TZ doesn't use DT
        coalesce(last_daylight_end_utc, cast('1970-01-01' as date)) as valid_from,

        -- daylight_start_utc is null for timezones that don't use DT
        coalesce(daylight_start_utc, cast( {{ dbt.dateadd('year', 1, dbt_utils.current_timestamp()) }} as date)) as valid_until

    from order_timezone_dt

    union all 

    -- DT schedule (excludes timezones without it)
    -- starts: when this Daylight Savings started
    -- ends: when this Daylight Savings ends
    select 
        time_zone,
        -- Pacific Time is -8h during standard time and -7h during DT
        standard_offset_minutes + daylight_offset_minutes as offset_minutes,
        daylight_start_utc as valid_from,
        daylight_end_utc as valid_until

    from order_timezone_dt
    where daylight_offset_minutes is not null

), calculate_schedules as (

    select 
        schedule.schedule_id,
        schedule.time_zone,
        schedule.start_time,
        schedule.end_time,
        schedule.created_at,
        schedule.schedule_name,
        schedule.start_time - coalesce(split_timezones.offset_minutes, 0) as start_time_utc,
        schedule.end_time - coalesce(split_timezones.offset_minutes, 0) as end_time_utc,

        -- we'll use these to determine which schedule version to associate tickets with
        split_timezones.valid_from,
        split_timezones.valid_until

    from schedule
    left join split_timezones
        on split_timezones.time_zone = schedule.time_zone

), final as (

    select 
        *,
        -- might remove this but for testing this is nice to have
        {{ dbt_utils.surrogate_key(['schedule_id', 'time_zone','start_time', 'valid_from']) }} as unqiue_schedule_spine_key
    
    from calculate_schedules
)

select *
from final