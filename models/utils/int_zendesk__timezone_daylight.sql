{{ config(enabled=var('using_schedules', True)) }}

with timezone as (

    select *
    from {{ var('time_zone') }}

), daylight_time as (

    select *
    from {{ var('daylight_time') }}

), timezone_with_dt as (

    select 
        timezone.*,
        daylight_time.daylight_start_utc,
        daylight_time.daylight_end_utc,
        daylight_time.daylight_offset_minutes

    from timezone 
    left join daylight_time 
        on timezone.time_zone = daylight_time.time_zone
        and timezone.source_relation = daylight_time.source_relation

), order_timezone_dt as (

    select 
        *,
        -- will be null for timezones without any daylight savings records (and the first entry)
        -- we will coalesce the first entry date with .... the X years ago
        lag(daylight_end_utc, 1) over (partition by source_relation, time_zone order by daylight_end_utc asc) as last_daylight_end_utc,
        -- will be null for timezones without any daylight savings records (and the last entry)
        -- we will coalesce the last entry date with the current date 
        lead(daylight_start_utc, 1) over (partition by source_relation, time_zone order by daylight_start_utc asc) as next_daylight_start_utc

    from timezone_with_dt

), split_timezones as (

    -- standard (includes timezones without DT)
    -- starts: when the last Daylight Savings ended
    -- ends: when the next Daylight Savings starts
    select 
        source_relation,
        time_zone,
        standard_offset_minutes as offset_minutes,

        -- last_daylight_end_utc is null for the first record of the time_zone's daylight time, or if the TZ doesn't use DT
        coalesce(last_daylight_end_utc, cast('1970-01-01' as date)) as valid_from,

        -- daylight_start_utc is null for timezones that don't use DT
        coalesce(daylight_start_utc, cast( {{ dbt.dateadd('year', 1, dbt.current_timestamp()) }} as date)) as valid_until

    from order_timezone_dt

    union all 

    -- DT (excludes timezones without it)
    -- starts: when this Daylight Savings started
    -- ends: when this Daylight Savings ends
    select 
        source_relation,
        time_zone,
        -- Pacific Time is -8h during standard time and -7h during DT
        standard_offset_minutes + daylight_offset_minutes as offset_minutes,
        daylight_start_utc as valid_from,
        daylight_end_utc as valid_until

    from order_timezone_dt
    where daylight_offset_minutes is not null

    union all

    select
        source_relation,
        time_zone,
        standard_offset_minutes as offset_minutes,

        -- Get the latest daylight_end_utc time and set that as the valid_from
        max(daylight_end_utc) as valid_from,

        -- If the latest_daylight_end_time_utc is less than todays timestamp, that means DST has ended. Therefore, we will make the valid_until in the future.
        cast( {{ dbt.dateadd('year', 1, dbt.current_timestamp()) }} as date) as valid_until

    from order_timezone_dt
    group by 1, 2, 3
    -- We only want to apply this logic to time_zone's that had daylight saving time and it ended at a point. For example, Hong Kong ended DST in 1979.
    having cast(max(daylight_end_utc) as date) < cast({{ dbt.current_timestamp() }} as date)

), final as (
    select
        source_relation,
        lower(time_zone) as time_zone,
        offset_minutes,
        cast(valid_from as {{ dbt.type_timestamp() }}) as valid_from,
        cast(valid_until as {{ dbt.type_timestamp() }}) as valid_until
    from split_timezones
)

select * 
from final