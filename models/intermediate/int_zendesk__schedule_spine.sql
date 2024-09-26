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
        cast(holiday.holiday_start_date_at as {{ dbt.type_timestamp() }} ) as holiday_valid_from,
        cast(holiday.holiday_end_date_at as {{ dbt.type_timestamp() }}) as holiday_valid_until, -- The valid_until will then be the the day after.
        cast(calendar_spine.date_day as {{ dbt.type_timestamp() }} ) as holiday_date,
        cast({{ dbt.date_trunc("week", "holiday.holiday_start_date_at") }} as {{ dbt.type_timestamp() }}) as holiday_starting_sunday,
        cast({{ dbt.dateadd("week", 1, dbt.date_trunc(
            "week", "holiday.holiday_end_date_at")
            ) }} as {{ dbt.type_timestamp() }}) as holiday_ending_sunday,
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
        schedule_holiday.holiday_valid_from,
        schedule_holiday.holiday_valid_until,
        schedule_holiday.holiday_starting_sunday,
        schedule_holiday.holiday_ending_sunday,
        calculate_schedules.valid_from as schedule_valid_from,
        calculate_schedules.valid_until as schedule_valid_until,
        cast({{ dbt.date_trunc("week", "calculate_schedules.valid_from") }} as {{ dbt.type_timestamp() }}) as schedule_starting_sunday,
        cast({{ dbt.date_trunc("week", "calculate_schedules.valid_until") }} as {{ dbt.type_timestamp() }}) as schedule_ending_sunday
    from calculate_schedules
    left join schedule_holiday
        on schedule_holiday.schedule_id = calculate_schedules.schedule_id
        and schedule_holiday.holiday_date <= calculate_schedules.valid_until
        and schedule_holiday.holiday_date >= calculate_schedules.valid_from

), split_holidays as(
    select
        join_holidays.*,
        case
            when holiday_valid_from = holiday_date
                then '0_start'
            end as holiday_start_or_end,
        schedule_valid_from as valid_from,
        holiday_date as valid_until
    from join_holidays
    where holiday_date is not null

    union all

    select
        join_holidays.*,
        case
            when holiday_valid_until = holiday_date
                then '1_end'
            end as holiday_start_or_end,
        holiday_date as valid_from,
        schedule_valid_until as valid_until,
    from join_holidays
    where holiday_date is not null

    union all

    select
        join_holidays.*,
        cast(null as {{ dbt.type_string() }}) as holiday_start_or_end,
        schedule_valid_from as valid_from,
        schedule_valid_until as valid_until
    from join_holidays
    where holiday_date is null

), valid_from_partition as(
    select
        split_holidays.*
        , row_number() over (partition by schedule_id, start_time_utc, schedule_valid_from order by holiday_date, holiday_start_or_end) as valid_from_index
        , count(*) over (partition by schedule_id, start_time_utc, schedule_valid_from) as max_valid_from_index
    from split_holidays
    where not (holiday_date is not null and holiday_start_or_end is null)

), add_end_row as(
    select
        schedule_id,
        time_zone,
        start_time_utc,
        end_time_utc,
        schedule_name,
        schedule_valid_from,
        schedule_valid_until,
        schedule_starting_sunday,
        schedule_ending_sunday,
        holiday_name,
        holiday_date,
        holiday_valid_from,
        holiday_valid_until,
        holiday_starting_sunday,
        holiday_ending_sunday,
        case when valid_from_index = 1 and holiday_start_or_end is not null
            then 'partition_start'
            else holiday_start_or_end
            end as holiday_start_or_end,
        valid_from,
        valid_until,
        valid_from_index,
        max_valid_from_index
    from valid_from_partition
    
    union all

    select
        schedule_id,
        time_zone,
        start_time_utc,
        end_time_utc,
        schedule_name,
        schedule_valid_from,
        schedule_valid_until,
        schedule_starting_sunday,
        schedule_ending_sunday,
        holiday_name,
        holiday_date,
        holiday_valid_from,
        holiday_valid_until,
        holiday_starting_sunday,
        holiday_ending_sunday,
        'partition_end' as holiday_start_or_end,
        valid_from,
        valid_until,
        max_valid_from_index + 1 as valid_from_index,
        max_valid_from_index
    from valid_from_partition
    where max_valid_from_index > 1
    and valid_from_index = max_valid_from_index

), adjust_ranges as(
    select
        schedule_id,
        time_zone,
        start_time_utc,
        end_time_utc,
        schedule_name,
        holiday_name,
        holiday_date,
        holiday_valid_from,
        holiday_valid_until,
        holiday_starting_sunday,
        holiday_ending_sunday,
        schedule_valid_from,
        schedule_valid_until,
        schedule_starting_sunday,
        schedule_ending_sunday,

        case
            when holiday_start_or_end = 'partition_start'
                then schedule_starting_sunday
            {# when holiday_start_or_end = '0_start'
                then holiday_starting_sunday #}
            when holiday_start_or_end = '1_end'
                then holiday_starting_sunday
            when holiday_start_or_end = 'partition_end'
                then holiday_ending_sunday
            else schedule_starting_sunday
        end as valid_from,

        case 
            when holiday_start_or_end = 'partition_start'
                then holiday_starting_sunday
            {# when holiday_start_or_end = '0_start'
                then holiday_ending_sunday #}
            when holiday_start_or_end = '1_end'
                then holiday_ending_sunday
            when holiday_start_or_end = 'partition_end'
                then schedule_ending_sunday
            else schedule_ending_sunday
        end as valid_until,

        valid_from_index,
        max_valid_from_index,
        holiday_start_or_end
    from add_end_row
    where holiday_start_or_end != '0_start' or holiday_start_or_end is null
    {# where not (valid_from_index > 1 and  holiday_start_or_end = '0_start') #}

{# ), final as(
    select
        schedule_id,
        valid_from,
        valid_until,
        start_time_utc,
        end_time_utc,
        holiday_name
    from adjust_ranges #}
    
)

select *
from adjust_ranges
{# where holiday_start_or_end != '0_start' or holiday_start_or_end is null #}