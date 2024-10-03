{{ config(enabled=var('using_schedules', True)) }}

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

), split_timezones as (
    select *
    from {{ ref('int_zendesk__timezone_daylight') }}  

{% if var('using_holidays', True) %}
), schedule_holiday as (
    select *
    from {{ var('schedule_holiday') }}  
{% endif %}

), calculate_schedules as (

    select 
        schedule.schedule_id,
        lower(schedule.time_zone) as time_zone,
        coalesce(split_timezones.offset_minutes, 0) as offset_minutes,
        schedule.start_time,
        schedule.end_time,
        schedule.schedule_name,
        schedule.start_time - coalesce(split_timezones.offset_minutes, 0) as start_time_utc,
        schedule.end_time - coalesce(split_timezones.offset_minutes, 0) as end_time_utc,
        -- we'll use these to determine which schedule version to associate tickets with.
        cast({{ dbt.date_trunc('day', 'split_timezones.valid_from') }} as {{ dbt.type_timestamp() }}) as schedule_valid_from,
        cast({{ dbt.date_trunc('day', 'split_timezones.valid_until') }}  as {{ dbt.type_timestamp() }}) as schedule_valid_until,
        cast({{ dbt_date.week_start('split_timezones.valid_from','UTC') }} as {{ dbt.type_timestamp() }}) as schedule_starting_sunday,
        cast({{ dbt_date.week_start('split_timezones.valid_until','UTC') }} as {{ dbt.type_timestamp() }}) as schedule_ending_sunday
    from schedule
    left join split_timezones
        on split_timezones.time_zone = lower(schedule.time_zone)

{% if var('using_holidays', True) %}
), schedule_holiday_ranges as (
    select
        holiday_name,
        schedule_id,
        cast({{ dbt.date_trunc('day', 'holiday_start_date_at') }} as {{ dbt.type_timestamp() }}) as holiday_valid_from,
        cast({{ dbt.date_trunc('day', 'holiday_end_date_at') }}  as {{ dbt.type_timestamp() }}) as holiday_valid_until,
        cast({{ dbt_date.week_start('holiday_start_date_at','UTC') }} as {{ dbt.type_timestamp() }}) as holiday_starting_sunday,
        cast({{ dbt_date.week_start(dbt.dateadd('week', 1, 'holiday_end_date_at'),'UTC') }} as {{ dbt.type_timestamp() }}) as holiday_ending_sunday
    from schedule_holiday   

), holiday_multiple_weeks_check as (
    -- Since the spine is based on weeks, holidays that span multiple weeks need to be broken up in to weeks.
    -- This first step is to find those holidays.
    select
        schedule_holiday_ranges.*,
        -- calculate weeks the holiday range spans
        {{ dbt.datediff('holiday_valid_from', 'holiday_valid_until', 'week') }} + 1 as holiday_weeks_spanned
    from schedule_holiday_ranges

), expanded_holidays as (
    -- this only needs to be run for holidays spanning multiple weeks
    select
        holiday_multiple_weeks_check.*,
        cast(week_numbers.generated_number as {{ dbt.type_int() }}) as holiday_week_number
    from holiday_multiple_weeks_check
    -- Generate a sequence of numbers from 0 to the max number of weeks spanned, assuming a holiday won't span more than 52 weeks
    cross join ({{ dbt_utils.generate_series(upper_bound=52) }}) as week_numbers
    where holiday_multiple_weeks_check.holiday_weeks_spanned > 1
    and week_numbers.generated_number <= holiday_multiple_weeks_check.holiday_weeks_spanned

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
    from holiday_multiple_weeks_check
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

-- in the below CTE we want to explode out each holiday period into individual days, to prevent potential fanouts downstream in joins to schedules.
), schedule_holiday_spine as ( 

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
{% endif %}

), join_holidays as (
    select 
        calculate_schedules.schedule_id,
        calculate_schedules.time_zone,
        calculate_schedules.offset_minutes,
        calculate_schedules.start_time_utc,
        calculate_schedules.end_time_utc,
        calculate_schedules.schedule_name,
        calculate_schedules.schedule_valid_from,
        calculate_schedules.schedule_valid_until,
        calculate_schedules.schedule_starting_sunday,
        calculate_schedules.schedule_ending_sunday,

        {% if var('using_holidays', True) %}
        schedule_holiday_spine.holiday_date,
        schedule_holiday_spine.holiday_name,
        schedule_holiday_spine.holiday_valid_from,
        schedule_holiday_spine.holiday_valid_until,
        schedule_holiday_spine.holiday_starting_sunday,
        schedule_holiday_spine.holiday_ending_sunday
        {% else %}
        cast(null as {{ dbt.type_timestamp() }}) as holiday_date,
        cast(null as {{ dbt.type_string() }}) as holiday_name,
        cast(null as {{ dbt.type_timestamp() }}) as holiday_valid_from,
        cast(null as {{ dbt.type_timestamp() }}) as holiday_valid_until,
        cast(null as {{ dbt.type_timestamp() }}) as holiday_starting_sunday,
        cast(null as {{ dbt.type_timestamp() }}) as holiday_ending_sunday
        {% endif %}
    
    from calculate_schedules

    {% if var('using_holidays', True) %}
    left join schedule_holiday_spine
        on schedule_holiday_spine.schedule_id = calculate_schedules.schedule_id
        and schedule_holiday_spine.holiday_date >= calculate_schedules.schedule_valid_from
        and schedule_holiday_spine.holiday_date < calculate_schedules.schedule_valid_until
    {% endif %}

), split_holidays as(
    -- create records for the first day of the holiday
    select
        join_holidays.*,
        case
            when holiday_valid_from = holiday_date
                then '0_gap' -- the number is for ordering later
            end as holiday_start_or_end
    from join_holidays
    where holiday_date is not null

    union all

    -- create records for the last day of the holiday
    select
        join_holidays.*,
        case
            when holiday_valid_until = holiday_date
                then '1_holiday' -- the number is for ordering later
            end as holiday_start_or_end
    from join_holidays
    where holiday_date is not null

    union all

    -- keep records for weeks with no holiday
    select
        join_holidays.*,
        cast(null as {{ dbt.type_string() }}) as holiday_start_or_end
    from join_holidays
    where holiday_date is null

), valid_from_partition as(
    select
        split_holidays.*,
        row_number() over (partition by schedule_id, start_time_utc, schedule_valid_from order by holiday_date, holiday_start_or_end) as valid_from_index,
        count(*) over (partition by schedule_id, start_time_utc, schedule_valid_from) as max_valid_from_index
    from split_holidays
    where not (holiday_date is not null and holiday_start_or_end is null)

), add_partition_end_row as(
    select
        schedule_id,
        time_zone,
        offset_minutes,
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
        valid_from_index,
        max_valid_from_index
    from valid_from_partition
    
    union all

    -- when max_valid_from_index > 1, then we want to duplicate the last row to end the partition.
    select
        schedule_id,
        time_zone,
        offset_minutes,
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
        max_valid_from_index + 1 as valid_from_index,
        max_valid_from_index
    from valid_from_partition
    where max_valid_from_index > 1
    and valid_from_index = max_valid_from_index

), adjust_ranges as(
    select
        add_partition_end_row.*,
        case
            when holiday_start_or_end = 'partition_start'
                then schedule_starting_sunday
            when holiday_start_or_end = '0_gap'
                then lag(holiday_ending_sunday) over (partition by schedule_id, start_time_utc, schedule_valid_from order by valid_from_index)
            when holiday_start_or_end = '1_holiday'
                then holiday_starting_sunday
            when holiday_start_or_end = 'partition_end'
                then holiday_ending_sunday
            else schedule_starting_sunday
        end as valid_from,
        case 
            when holiday_start_or_end = 'partition_start'
                then holiday_starting_sunday
            when holiday_start_or_end = '0_gap'
                then lead(holiday_starting_sunday) over (partition by schedule_id, start_time_utc, schedule_valid_from order by valid_from_index)
            when holiday_start_or_end = '1_holiday'
                then holiday_ending_sunday
            when holiday_start_or_end = 'partition_end'
                then schedule_ending_sunday
            else schedule_ending_sunday
        end as valid_until
    from add_partition_end_row

), holiday_weeks as(
    select
        adjust_ranges.*,
        case when holiday_start_or_end = '1_holiday'
            then true
            else false
            end as is_holiday_week
    from adjust_ranges
    -- filter out irrelevant records
    where not (valid_from >= valid_until and holiday_date is not null)

), valid_minutes as(
    select
        holiday_weeks.*,

        -- Calculate holiday_valid_from in minutes from week start
        case when is_holiday_week 
            then ({{ dbt.datediff('holiday_starting_sunday', 'holiday_valid_from', 'minute') }}
                - offset_minutes) -- timezone adjustment
            else null
        end as holiday_valid_from_minutes_from_week_start,

        -- Calculate holiday_valid_until in minutes from week start
        case when is_holiday_week
            then ({{ dbt.datediff('holiday_starting_sunday', 'holiday_valid_until', 'minute') }}
                + 24 * 60 -- add 1 day to set the upper bound of the holiday
                - offset_minutes)-- timezone adjustment
            else null
        end as holiday_valid_until_minutes_from_week_start
    from holiday_weeks

), find_holidays as(
    select 
        schedule_id,
        valid_from,
        valid_until,
        start_time_utc,
        end_time_utc,
        case 
            when start_time_utc < holiday_valid_until_minutes_from_week_start
                and end_time_utc > holiday_valid_from_minutes_from_week_start
                and is_holiday_week
            then holiday_name
            else cast(null as {{ dbt.type_string() }}) 
        end as holiday_name,
        is_holiday_week,
        count(*) over (partition by schedule_id, valid_from, valid_until, start_time_utc, end_time_utc) as number_holidays_in_week
    from valid_minutes

), filter_holidays as(
    select 
        *,
        cast(1 as {{ dbt.type_int() }}) as number_records_for_schedule_start_end
    from find_holidays
    where number_holidays_in_week = 1

    union all

    -- we want to count the number of records for each schedule start_time_utc and end_time_utc for filtering later
    select 
        distinct *,
        cast(count(*) over (partition by schedule_id, valid_from, valid_until, start_time_utc, end_time_utc, holiday_name) 
            as {{ dbt.type_int() }}) as number_records_for_schedule_start_end
    from find_holidays
    where number_holidays_in_week > 1

), final as(
    select 
        schedule_id,
        valid_from,
        valid_until,
        start_time_utc,
        end_time_utc,
        is_holiday_week
    from filter_holidays

    -- This filter ensures that for each schedule, the count of holidays in a week matches the number 
    -- of distinct schedule records with the same start_time_utc and end_time_utc.
    -- Rows where this count doesn't match indicate overlap with a holiday, so we filter out that record.
    -- Additionally, schedule records that fall on a holiday are excluded by checking if holiday_name is null.
    where number_holidays_in_week = number_records_for_schedule_start_end
    and holiday_name is null
)

select *
from final
