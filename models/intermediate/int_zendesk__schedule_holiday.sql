{{ config(enabled=fivetran_utils.enabled_vars(['using_schedules','using_schedule_holidays'])) }}

/*
    The purpose of this model is to create a spine of appropriate timezone offsets to use for schedules, as offsets may change due to Daylight Savings.
    End result will include `valid_from` and `valid_until` columns which we will use downstream to determine which schedule-offset to associate with each ticket (ie standard time vs daylight time)
*/

with calendar_spine as (
    select
        cast(date_day as {{ dbt.type_timestamp() }}) as date_day
    from {{ ref('int_zendesk__calendar_spine') }}  

), schedule as (
    select *
    from {{ var('schedule') }}   

), schedule_holiday as (
    select *
    from {{ var('schedule_holiday') }}  

-- Converts holiday_start_date_at and holiday_end_date_at into daily timestamps and finds the week starts/ends using week_start.
), schedule_holiday_ranges as (
    select
        holiday_name,
        schedule_id,
        cast({{ dbt.date_trunc('day', 'holiday_start_date_at') }} as {{ dbt.type_timestamp() }}) as holiday_valid_from,
        cast({{ dbt.date_trunc('day', 'holiday_end_date_at') }}  as {{ dbt.type_timestamp() }}) as holiday_valid_until,
        cast({{ dbt_date.week_start('holiday_start_date_at','UTC') }} as {{ dbt.type_timestamp() }}) as holiday_starting_sunday,
        cast({{ dbt_date.week_start(dbt.dateadd('week', 1, 'holiday_end_date_at'),'UTC') }} as {{ dbt.type_timestamp() }}) as holiday_ending_sunday,
        -- Since the spine is based on weeks, holidays that span multiple weeks need to be broken up in to weeks. First step is to find those holidays.
        {{ dbt.datediff('holiday_start_date_at', 'holiday_end_date_at', 'week') }} + 1 as holiday_weeks_spanned
    from schedule_holiday

-- Creates a record for each week of multi-week holidays. Update valid_from and valid_until in the next cte.
), expanded_holidays as (
    select
        schedule_holiday_ranges.*,
        cast(week_numbers.generated_number as {{ dbt.type_int() }}) as holiday_week_number
    from schedule_holiday_ranges
    -- Generate a sequence of numbers from 0 to the max number of weeks spanned, assuming a holiday won't span more than 52 weeks
    cross join ({{ dbt_utils.generate_series(upper_bound=52) }}) as week_numbers
    where schedule_holiday_ranges.holiday_weeks_spanned > 1
    and week_numbers.generated_number <= schedule_holiday_ranges.holiday_weeks_spanned

-- Define start and end times for each segment of a multi-week holiday.
), split_multiweek_holidays as (

    -- Business as usual for holidays that fall within a single week.
    select
        holiday_name,
        schedule_id,
        holiday_valid_from,
        holiday_valid_until,
        holiday_starting_sunday,
        holiday_ending_sunday,
        holiday_weeks_spanned
    from schedule_holiday_ranges
    where holiday_weeks_spanned = 1

    union all

    -- Split holidays by week that span multiple weeks.
    select
        holiday_name,
        schedule_id,
        case 
            when holiday_week_number = 1 -- first week in multiweek holiday
            then holiday_valid_from
            -- We have to use days in case warehouse does not truncate to Sunday.
            else cast({{ dbt.dateadd('day', '(holiday_week_number - 1) * 7', 'holiday_starting_sunday') }} as {{ dbt.type_timestamp() }})
        end as holiday_valid_from,
        case 
            when holiday_week_number = holiday_weeks_spanned -- last week in multiweek holiday
            then holiday_valid_until
            -- We have to use days in case warehouse does not truncate to Sunday.
            else cast({{ dbt.dateadd('day', -1, dbt.dateadd('day', 'holiday_week_number * 7', 'holiday_starting_sunday')) }} as {{ dbt.type_timestamp() }}) -- saturday
        end as holiday_valid_until,
        case 
            when holiday_week_number = 1 -- first week in multiweek holiday
            then holiday_starting_sunday
            -- We have to use days in case warehouse does not truncate to Sunday.
            else cast({{ dbt.dateadd('day', '(holiday_week_number - 1) * 7', 'holiday_starting_sunday') }} as {{ dbt.type_timestamp() }})
        end as holiday_starting_sunday,
        case 
            when holiday_week_number = holiday_weeks_spanned -- last week in multiweek holiday
            then holiday_ending_sunday
            -- We have to use days in case warehouse does not truncate to Sunday.
            else cast({{ dbt.dateadd('day', 'holiday_week_number * 7', 'holiday_starting_sunday') }} as {{ dbt.type_timestamp() }})
        end as holiday_ending_sunday,
        holiday_weeks_spanned
    from expanded_holidays
    where holiday_weeks_spanned > 1

-- Explodes multi-week holidays into individual days by joining with the calendar_spine. This is necessary to remove schedules
-- that occur during a holiday downstream.
), holiday_spine as ( 

    select
        split_multiweek_holidays.holiday_name,
        split_multiweek_holidays.schedule_id,
        split_multiweek_holidays.holiday_valid_from,
        split_multiweek_holidays.holiday_valid_until,
        split_multiweek_holidays.holiday_starting_sunday,
        split_multiweek_holidays.holiday_ending_sunday,
        calendar_spine.date_day as holiday_date
    from split_multiweek_holidays 
    inner join calendar_spine
        on split_multiweek_holidays.holiday_valid_from <= calendar_spine.date_day
        and split_multiweek_holidays.holiday_valid_until >= calendar_spine.date_day
)

select *
from holiday_spine
