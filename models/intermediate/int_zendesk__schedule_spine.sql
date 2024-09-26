{{ config(enabled=var('using_schedules', True)) }}

/*
    The purpose of this model is to create a spine of appropriate timezone offsets to use for schedules, as offsets may change due to Daylight Savings.
    End result will include `valid_from` and `valid_until` columns which we will use downstream to determine which schedule-offset to associate with each ticket (ie standard time vs daylight time)
*/

with schedule as (

    select *
    from {{ var('schedule') }}   

), holiday as (

    select *
    from {{ var('schedule_holiday') }}    

), calendar_spine as (

    select *
    from {{ ref('int_zendesk__calendar_spine') }}   

), split_timezones as (

    select *
    from {{ ref('int_zendesk__timezone_daylight') }}  

-- in the below CTE we want to explode out each holiday period into individual days, to prevent potential fanouts downstream in joins to schedules.
), schedule_holiday as ( 

    select
        holiday._fivetran_synced,
        {# cast(calendar_spine.date_day as {{ dbt.type_timestamp() }} ) as holiday_start_date_at, -- For each day within a holiday we want to give it its own record. In the later CTE holiday_start_end_times, we transform these timestamps into minutes-from-beginning-of-the-week.
        cast(calendar_spine.date_day as {{ dbt.type_timestamp() }} ) as holiday_end_date_at, -- Since each day within a holiday now gets its own record, the end_date will then be the same day as the start_date. In the later CTE holiday_start_end_times, we transform these timestamps into minutes-from-beginning-of-the-week. #}
        cast(calendar_spine.date_day as {{ dbt.type_timestamp() }} ) as holiday_date, -- Since each day within a holiday now gets its own record, the end_date will then be the same day as the start_date. In the later CTE holiday_start_end_times, we transform these timestamps into minutes-from-beginning-of-the-week.
        holiday.holiday_id,
        holiday.holiday_name,
        holiday.schedule_id

    from holiday 
    inner join calendar_spine
        on holiday_start_date_at <= cast(date_day as {{ dbt.type_timestamp() }} )
        and holiday_end_date_at >= cast(date_day as {{ dbt.type_timestamp() }} )

), calculate_schedules as (

    select 
        schedule.schedule_id,
        lower(schedule.time_zone) as time_zone,
        schedule.start_time,
        schedule.end_time,
        {# schedule.created_at, #}
        schedule.schedule_name,
        schedule.start_time - coalesce(split_timezones.offset_minutes, 0) as start_time_utc,
        schedule.end_time - coalesce(split_timezones.offset_minutes, 0) as end_time_utc,
        coalesce(split_timezones.offset_minutes, 0) as offset_minutes_to_add,
        -- we'll use these to determine which schedule version to associate tickets with
        cast(split_timezones.valid_from as {{ dbt.type_timestamp() }}) as valid_from,
        cast(split_timezones.valid_until as {{ dbt.type_timestamp() }}) as valid_until

    from schedule
    left join split_timezones
        on split_timezones.time_zone = lower(schedule.time_zone)

), join_holidays as (
    select 
        calculate_schedules.schedule_id,
        calculate_schedules.time_zone,
        calculate_schedules.start_time_utc,
        calculate_schedules.end_time_utc,
        calculate_schedules.schedule_name,
        schedule_holiday.holiday_date,
        schedule_holiday.holiday_name,
        calculate_schedules.valid_from as schedule_valid_from,
        calculate_schedules.valid_until as schedule_valid_until
    from calculate_schedules
    left join schedule_holiday
        on schedule_holiday.schedule_id = calculate_schedules.schedule_id
        and schedule_holiday.holiday_date <= calculate_schedules.valid_until
        and schedule_holiday.holiday_date >= calculate_schedules.valid_from

), holiday_neighbors as(
    select
        schedule_id,
        time_zone,
        start_time_utc,
        end_time_utc,
        schedule_name,
        holiday_name,
        holiday_date,
        schedule_valid_from,
        schedule_valid_until,
        lag(holiday_date) over (partition by schedule_id, start_time_utc order by holiday_date) as prior_holiday,
        lead(holiday_date) over (partition by schedule_id, start_time_utc order by holiday_date) as next_holiday
    from join_holidays

), split_holidays as(
    select
        schedule_id,
        time_zone,
        start_time_utc,
        end_time_utc,
        schedule_name,
        schedule_valid_from,
        schedule_valid_until,
        holiday_name,
        holiday_date,
        case
            when (date_diff(holiday_date, prior_holiday, day) > 1
                or prior_holiday is null)
                then 'start'
            end as holiday_start_or_end,
        schedule_valid_from as valid_from,
        holiday_date as valid_until
    from holiday_neighbors
    where holiday_date is not null

    union all

    select
        schedule_id,
        time_zone,
        start_time_utc,
        end_time_utc,
        schedule_name,
        schedule_valid_from,
        schedule_valid_until,
        holiday_name,
        holiday_date,
        case
            when (date_diff(next_holiday, holiday_date, day) > 1
                or next_holiday is null)
                then 'end'
            end as holiday_start_or_end,
        holiday_date as valid_from,
        schedule_valid_until as valid_until
    from holiday_neighbors
    where holiday_date is not null

    union all

    select
        schedule_id,
        time_zone,
        start_time_utc,
        end_time_utc,
        schedule_name,
        schedule_valid_from,
        schedule_valid_until,
        holiday_name,
        holiday_date,
        cast(null as {{ dbt.type_string() }}) as holiday_start_or_end,
        schedule_valid_from as valid_from,
        schedule_valid_until as valid_until
    from holiday_neighbors
    where holiday_date is null

), valid_from_partition as(
    select
        *
        , row_number() over (partition by schedule_id, start_time_utc, schedule_valid_from order by holiday_date) as valid_from_index
    from split_holidays
    where not (holiday_date is not null and holiday_start_or_end is null)

), adjust_ranges as(
    select
        schedule_id,
        time_zone,
        start_time_utc,
        end_time_utc,
        schedule_name,
        holiday_name,
        holiday_date,
        holiday_start_or_end,

        case 
            when holiday_start_or_end = 'start'
                then case when valid_from_index > 1
                    then lag(holiday_date) over (partition by schedule_id, start_time_utc, schedule_valid_from order by valid_from_index)
                    else schedule_valid_from
                    end
            when holiday_start_or_end = 'end'
                then cast({{ dbt.dateadd(datepart="day", interval=1, from_date_or_timestamp="holiday_date") }} as {{ dbt.type_timestamp() }})
            else cast(schedule_valid_from as {{ dbt.type_timestamp() }})
        end as valid_from,

        case 
            when holiday_start_or_end = 'start'
                then holiday_date
            when holiday_start_or_end = 'end'
                then case when valid_from_index > 1
                    then coalesce(
                        lead(holiday_date) over (partition by schedule_id, start_time_utc, schedule_valid_from order by valid_from_index),
                        schedule_valid_until)
                    else schedule_valid_until
                    end
            else schedule_valid_until
        end as valid_until,

        valid_from_index
    from valid_from_partition
    where not (valid_from_index > 1 and  holiday_start_or_end = 'start')
)

    select
        schedule_id,
        valid_from,
        valid_until,
        start_time_utc,
        end_time_utc
    from adjust_ranges