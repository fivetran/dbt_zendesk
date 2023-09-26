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

), schedule_holiday as (

    select *
    from {{ var('schedule_holiday') }}   

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
        coalesce(daylight_start_utc, cast( {{ dbt.dateadd('year', 1, dbt.current_timestamp_backcompat()) }} as date)) as valid_until

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
        cast(split_timezones.valid_from as {{ dbt.type_timestamp() }}) as valid_from,
        cast(split_timezones.valid_until as {{ dbt.type_timestamp() }}) as valid_until

    from schedule
    left join split_timezones
        on split_timezones.time_zone = schedule.time_zone

-- Now we need take holiday's into consideration and perform the following transformations to account for Holidays in existing schedules
), holiday_start_end_times as (

    select 
        calculate_schedules.*,
        schedule_holiday.holiday_name,
        schedule_holiday.holiday_start_date_at,
        cast({{ dbt.dateadd("second", "86399", "schedule_holiday.holiday_end_date_at") }} as {{ dbt.type_timestamp() }}) as holiday_end_date_at, -- add 24*60*60-1 seconds
        cast({{ dbt_date.week_start("schedule_holiday.holiday_start_date_at") }} as {{ dbt.type_timestamp() }}) as holiday_week_start,
        cast({{ dbt_date.week_end("schedule_holiday.holiday_end_date_at") }} as {{ dbt.type_timestamp() }}) as holiday_week_end
    from schedule_holiday
    inner join calculate_schedules
        on calculate_schedules.schedule_id = schedule_holiday.schedule_id
        and schedule_holiday.holiday_start_date_at >= calculate_schedules.valid_from 
        and schedule_holiday.holiday_start_date_at < calculate_schedules.valid_until

-- Let's calculate the start and end date of the Holiday in terms of minutes from Sunday (like other Zendesk schedules)
), holiday_minutes as(

    select
        *,
        {{ dbt.datediff("holiday_week_start", "holiday_start_date_at", "minute") }} as minutes_from_sunday_start,
        {{ dbt.datediff("holiday_week_start", "holiday_end_date_at", "minute") }} as minutes_from_sunday_end
    from holiday_start_end_times

-- Determine which schedule days include a holiday
), holiday_check as (

    select
        *,
        case when minutes_from_sunday_start < start_time_utc and minutes_from_sunday_end > end_time_utc 
            then holiday_name 
        end as holiday_name_check
    from holiday_minutes

-- Consolidate the holiday records that were just created
), holiday_consolidated as (

    select 
        schedule_id, 
        time_zone, 
        schedule_name, 
        valid_from, 
        valid_until, 
        start_time_utc, 
        end_time_utc, 
        holiday_week_start,
        cast({{ dbt.dateadd("second", "86399", "holiday_week_end") }} as {{ dbt.type_timestamp() }}) as holiday_week_end,
        max(holiday_name_check) as holiday_name_check
    from holiday_check
    {{ dbt_utils.group_by(n=9) }}

-- Since we have holiday schedules and normal schedules, we need to union them into a holistic schedule spine
), spine_union as (

    select
        schedule_id, 
        time_zone, 
        schedule_name, 
        valid_from, 
        valid_until, 
        start_time_utc, 
        end_time_utc, 
        holiday_week_start,
        holiday_week_end,
        holiday_name_check
    from holiday_consolidated

    union all

    select
        schedule_id, 
        time_zone, 
        schedule_name, 
        valid_from, 
        valid_until, 
        start_time_utc, 
        end_time_utc, 
        null as holiday_week_start,
        null as holiday_week_end,
        null as holiday_name_check
    from calculate_schedules

-- Now that we have an understanding of which weeks are holiday's let's consolidate them with non holiday weeks
), all_periods as (

    select distinct
        schedule_id,
        holiday_week_start as period_start,
        holiday_week_end as period_end,
        start_time_utc,
        end_time_utc,
        holiday_name_check,
        true as is_holiday_week
    from spine_union
    where holiday_week_start is not null
        and holiday_week_end is not null

    union all

    select distinct
        schedule_id,
        valid_from as period_start,
        valid_until as period_end,
        start_time_utc,
        end_time_utc,
        cast(null as {{ dbt.type_string() }}) as holiday_name_check,
        false as is_holiday_week
    from spine_union

-- We have holiday and non holiday schedules together, now let's sort them to understand the previous end and next start of neighboring schedules
), sorted_periods as (

    select distinct
        *,
        lag(period_end) over (partition by schedule_id order by period_end, start_time_utc) as prev_end,
        lead(period_start) over (partition by schedule_id order by period_start, start_time_utc) as next_start
    from all_periods

-- We need to adjust some non holiday schedules in order to properly fill holiday gaps in the schedules later down the transformation
), non_holiday_period_adjustments as (

    select
        schedule_id, 
        period_start, 
        period_end,
        prev_end,
        next_start,
        coalesce(case 
            when not is_holiday_week and prev_end is not null then first_value(prev_end) over (partition by schedule_id, period_start order by start_time_utc rows between unbounded preceding and unbounded following)
            else period_start
        end, period_start) as valid_from,
        coalesce(case 
            when not is_holiday_week and next_start is not null then last_value(next_start) over (partition by schedule_id, period_start order by start_time_utc rows between unbounded preceding and unbounded following)
            else period_end
        end, period_end) as valid_until,
        start_time_utc,
        end_time_utc,
        holiday_name_check,
        is_holiday_week
    from sorted_periods

-- A few window function results will be leveraged downstream. Let's generate them now.
), gap_starter as (
    select 
        *,
        max(period_end) over (partition by schedule_id) as max_valid_until,
        last_value(next_start) over (partition by schedule_id, period_start order by valid_until rows between unbounded preceding and unbounded following) as lead_next_start,
        first_value(prev_end) over (partition by schedule_id, valid_from order by start_time_utc rows between unbounded preceding and unbounded following) as first_prev_end
    from non_holiday_period_adjustments

-- There may be gaps in holiday and non holiday schedules, so we need to identify where these gaps are
), gap_adjustments as(

    select 
        *,
        -- In order to identify the gaps we check to see if the valid_from and previous valid_until are right next to one. If we add two hours to the previous valid_until it should always be greater than the current valid_from.
        -- However, if the valid_from is greater instead then we can identify that this period has a gap that needs to be filled.
        case when valid_from > cast({{ dbt.dateadd("hour", "2", "first_prev_end") }} as {{ dbt.type_timestamp() }})
            then 'gap'
        when (lead_next_start is null and valid_from < max_valid_until and period_end != max_valid_until)
            then 'gap'
            else null
        end as is_schedule_gap,

        -- Additionally, there may be cases where a gap is identified both before the holiday and after. In those cases we want to make appropriate adjustments after the holiday.
        case when (valid_from > cast({{ dbt.dateadd("hour", "2", "first_prev_end") }} as {{ dbt.type_timestamp() }})) and (lead_next_start is null and valid_from < max_valid_until) and (period_end != max_valid_until)
            then 'double_gap'
            else null
        end as is_schedule_double_gap
    from gap_starter

-- We know where the gaps are, so now lets prime the data to fill those gaps
), schedule_spine_primer as (

    select 
        schedule_id, 
        valid_from,
        valid_until,
        start_time_utc,
        end_time_utc,
        lead_next_start,
        max_valid_until,
        holiday_name_check,
        is_holiday_week,
        max(is_schedule_gap) over (partition by schedule_id, valid_until) as is_gap_period,
        max(is_schedule_double_gap) over (partition by schedule_id, valid_until) as is_double_gap_period,
        lag(valid_until) over (partition by schedule_id order by valid_until, start_time_utc) as fill_primer
    from gap_adjustments

-- We know the gaps and where they are, so let's fill them with the following union
), final_union as (

    -- For all gap periods, let's properly create a schedule filled before the holiday.
    select 
        schedule_id,
        first_value(fill_primer) over (partition by schedule_id, valid_until order by start_time_utc rows between unbounded preceding and unbounded following) as valid_from,
        valid_from as valid_until,
        start_time_utc, 
        end_time_utc, 
        cast(null as {{ dbt.type_string() }}) as holiday_name_check,
        false as is_holiday_week
    from schedule_spine_primer
    where is_gap_period is not null

    union all

    -- For any double gap periods, let's fill a schedule directly after the holiday.
    select
        schedule_id,
        case when lead_next_start is not null
            then first_value(fill_primer) over (partition by schedule_id, valid_until order by start_time_utc rows between unbounded preceding and unbounded following) 
            else last_value(fill_primer) over (partition by schedule_id, valid_until order by start_time_utc rows between unbounded preceding and unbounded following)
        end as valid_from,
        case when lead_next_start is not null
            then valid_from 
            else max_valid_until
        end as valid_until,
        start_time_utc, 
        end_time_utc, 
        cast(null as {{ dbt.type_string() }}) as holiday_name_check,
        false as is_holiday_week
    from schedule_spine_primer
    where is_double_gap_period is not null

    union all

    -- Fill all other normal schedules.
    select
        schedule_id, 
        valid_from,
        valid_until,
        start_time_utc,
        end_time_utc,
        holiday_name_check,
        is_holiday_week
    from schedule_spine_primer

-- We can finally filter out the holiday_name_check results as the gap filling properly filled in the gaps for holidays
), final as(

    select
        schedule_id, 
        valid_from,
        valid_until,
        start_time_utc,
        end_time_utc,
        is_holiday_week
    from final_union
    where holiday_name_check is null
)

select *
from final