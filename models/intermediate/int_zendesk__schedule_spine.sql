{{ config(enabled=var('using_schedules', True)) }}

/*
    This model generates `valid_from` and `valid_until` timestamps for each schedule start_time and stop_time, 
    accounting for timezone changes, holidays, and historical schedule adjustments. The inclusion of holidays 
    and historical changes is controlled by variables `using_holidays` and `using_schedule_histories`.

    !!! Important distinction for holiday ranges: A holiday remains valid through the entire day specified by 
    the `valid_until` field. In contrast, schedule history and timezone `valid_until` values mark the end of 
    validity at the start of the specified day.
*/

with schedule_timezones as (
    select *
    from {{ ref('int_zendesk__schedule_timezones') }}  

{% if var('using_holidays', True) %}
), schedule_holidays as (
    select *
    from {{ ref('int_zendesk__schedule_holiday') }}  

-- Joins the schedules with holidays, ensuring holidays fall within the valid schedule period.
-- If there are no holidays, the columns are filled with null values.
), join_holidays as (
    select 
        schedule_timezones.schedule_id,
        schedule_timezones.time_zone,
        schedule_timezones.offset_minutes,
        schedule_timezones.start_time_utc,
        schedule_timezones.end_time_utc,
        schedule_timezones.schedule_name,
        schedule_timezones.schedule_valid_from,
        schedule_timezones.schedule_valid_until,
        schedule_timezones.schedule_starting_sunday,
        schedule_timezones.schedule_ending_sunday,
        schedule_timezones.change_type,
        schedule_holidays.holiday_date,
        schedule_holidays.holiday_name,
        schedule_holidays.holiday_valid_from,
        schedule_holidays.holiday_valid_until,
        schedule_holidays.holiday_starting_sunday,
        schedule_holidays.holiday_ending_sunday,
        schedule_holidays.holiday_start_or_end
    from schedule_timezones
    left join schedule_holidays
        on schedule_holidays.schedule_id = schedule_timezones.schedule_id
        and schedule_holidays.holiday_date >= schedule_timezones.schedule_valid_from
        and schedule_holidays.holiday_date < schedule_timezones.schedule_valid_until

-- Find and count all holidays that fall within a schedule range.
), valid_from_partition as(
    select
        join_holidays.*,
        row_number() over (partition by schedule_id, start_time_utc, schedule_valid_from order by holiday_date, holiday_start_or_end) as valid_from_index,
        count(*) over (partition by schedule_id, start_time_utc, schedule_valid_from) as max_valid_from_index
    from join_holidays

-- Label the partition start and add a row to account for the partition end if there are multiple valid periods.
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
        change_type,
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
        change_type,
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
    and valid_from_index = max_valid_from_index -- this finds the last rows to duplicate

-- Adjusts and fills the valid from and valid until times for each partition, taking into account the partition start, gap, or holiday.
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
        schedule_id,
        time_zone,
        offset_minutes,
        start_time_utc,
        end_time_utc,
        schedule_name,
        valid_from,
        valid_until,
        holiday_name,
        holiday_valid_from,
        holiday_valid_until,
        holiday_starting_sunday,
        holiday_ending_sunday,
        holiday_start_or_end,
        valid_from_index,
        case when holiday_start_or_end = '1_holiday'
            then 'holiday'
            else change_type
            end as change_type
    from adjust_ranges
    -- filter out irrelevant records after adjusting the ranges
    where not (valid_from >= valid_until and holiday_date is not null)

-- Converts holiday valid_from and valid_until times into minutes from the start of the week, adjusting for timezones.
), valid_minutes as(
    select
        holiday_weeks.*,

        -- Calculate holiday_valid_from in minutes from week start
        case when change_type = 'holiday' 
            then ({{ dbt.datediff('holiday_starting_sunday', 'holiday_valid_from', 'minute') }}
                - offset_minutes) -- timezone adjustment
            else null
        end as holiday_valid_from_minutes_from_week_start,

        -- Calculate holiday_valid_until in minutes from week start
        case when change_type = 'holiday' 
            then ({{ dbt.datediff('holiday_starting_sunday', 'holiday_valid_until', 'minute') }}
                + 24 * 60 -- add 1 day to set the upper bound of the holiday
                - offset_minutes) -- timezone adjustment
            else null
        end as holiday_valid_until_minutes_from_week_start
    from holiday_weeks

-- Identifies whether a schedule overlaps with a holiday by comparing start and end times with holiday minutes.
), find_holidays as(
    select 
        schedule_id,
        valid_from,
        valid_until,
        start_time_utc,
        end_time_utc,
        change_type,
        case 
            when start_time_utc < holiday_valid_until_minutes_from_week_start
                and end_time_utc > holiday_valid_from_minutes_from_week_start
                and change_type = 'holiday' 
            then holiday_name
            else cast(null as {{ dbt.type_string() }}) 
        end as holiday_name,
        count(*) over (partition by schedule_id, valid_from, valid_until, start_time_utc, end_time_utc) as number_holidays_in_week
    from valid_minutes

-- Filter out records where holiday overlaps don't match, ensuring each schedule's holiday status is consistent.
), filter_holidays as(
    select 
        *,
        cast(1 as {{ dbt.type_int() }}) as number_records_for_schedule_start_end
    from find_holidays
    where number_holidays_in_week = 1

    union all

    -- Count the number of records for each schedule start_time_utc and end_time_utc for filtering later.
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
        change_type
    from filter_holidays

    -- This filter ensures that for each schedule, the count of holidays in a week matches the number 
    -- of distinct schedule records with the same start_time_utc and end_time_utc.
    -- Rows where this count doesn't match indicate overlap with a holiday, so we filter out that record.
    -- Additionally, schedule records that fall on a holiday are excluded by checking if holiday_name is null.
    where number_holidays_in_week = number_records_for_schedule_start_end
    and holiday_name is null

{% else %} 
), final as(
    select 
        schedule_id,
        schedule_valid_from as valid_from,
        schedule_valid_until as valid_until,
        start_time_utc,
        end_time_utc,
        change_type
    from schedule_timezones
{% endif %} 
)

select *
from final
