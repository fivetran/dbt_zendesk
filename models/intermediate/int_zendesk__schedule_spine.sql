{{ config(enabled=var('using_schedules', True)) }}

/*
    The purpose of this model is to create a spine of appropriate timezone offsets to use for schedules, as offsets may change due to Daylight Savings.
    End result will include `valid_from` and `valid_until` columns which we will use downstream to determine which schedule-offset to associate with each ticket (ie standard time vs daylight time)
*/

with schedule as (

    select *
    from {{ var('schedule') }}   

), calendar_spine as (

    select
        cast(date_day as {{ dbt.type_timestamp() }} ) as date_day
    from {{ ref('int_zendesk__calendar_spine') }}   

), split_timezones as (

    select *
    from {{ ref('int_zendesk__timezone_daylight') }}  

{% if var('using_holidays', True) %}
), holiday as (

    select
        _fivetran_synced,
        holiday_name,
        schedule_id,
        cast(holiday_start_date_at as {{ dbt.type_timestamp() }} ) as holiday_valid_from,
        cast(holiday_end_date_at as {{ dbt.type_timestamp() }}) as holiday_valid_until,
        cast({{ dbt.date_trunc("week", "holiday_start_date_at") }} as {{ dbt.type_timestamp() }}) as holiday_starting_sunday,
        cast({{ dbt.dateadd("week", 1, dbt.date_trunc(
            "week", "holiday_end_date_at")
            ) }} as {{ dbt.type_timestamp() }}) as holiday_ending_sunday
    from {{ var('schedule_holiday') }}    

), holiday_multiple_weeks_check as (

    select
        holiday.*,
        -- calculate weeks the holiday range spans. Takes into account if the holiday extends into the next year.
        (extract(week from holiday_ending_sunday) + extract(year from holiday_ending_sunday) * 52) 
            - (extract(week from holiday_starting_sunday) + extract(year from holiday_starting_sunday) * 52)
            as holiday_weeks_spanned
    from holiday

), split_multiweek_holidays as (

    select
        _fivetran_synced,
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

    -- Split holidays that span a weekend. This is for the first half.
    select
        _fivetran_synced,
        holiday_name,
        schedule_id,
        holiday_valid_from,
        cast({{ dbt.last_day('holiday_valid_from', 'week') }} as {{ dbt.type_timestamp() }}) as holiday_valid_until,
        holiday_starting_sunday,
        cast({{ dbt.dateadd('week', 1, 'holiday_starting_sunday') }} as {{ dbt.type_timestamp() }}) as holiday_ending_sunday,
        holiday_weeks_spanned
    from holiday_multiple_weeks_check
    where holiday_weeks_spanned > 1

    union all

    -- Split holidays that span a weekend. This is for the last half.
    select
        _fivetran_synced,
        holiday_name,
        schedule_id,
        cast({{ dbt.date_trunc('week', 'holiday_valid_until') }} as {{ dbt.type_timestamp() }}) as holiday_valid_from,
        holiday_valid_until,
        cast({{ dbt.dateadd('week', -1, 'holiday_ending_sunday') }} as {{ dbt.type_timestamp() }}) as holiday_starting_sunday,
        holiday_ending_sunday,
        holiday_weeks_spanned
    from holiday_multiple_weeks_check
    where holiday_weeks_spanned > 1

    union all

    -- Fill holidays that span more than two weeks. This will fill entire weeks for those sandwiched between the ends.
    select
        _fivetran_synced,
        holiday_name,
        schedule_id,
        cast({{ dbt.dateadd('week', 1, 'holiday_starting_sunday') }} as {{ dbt.type_timestamp() }}) as holiday_valid_from,
        cast({{ dbt.dateadd('week', -1, 'holiday_ending_sunday') }} as {{ dbt.type_timestamp() }}) as holiday_valid_until,
        cast({{ dbt.dateadd('week', 1, 'holiday_starting_sunday') }} as {{ dbt.type_timestamp() }}) as holiday_starting_sunday,
        cast({{ dbt.dateadd('week', -1, 'holiday_ending_sunday') }} as {{ dbt.type_timestamp() }}) as holiday_ending_sunday,
        holiday_weeks_spanned
    from holiday_multiple_weeks_check
    where holiday_weeks_spanned > 2

-- in the below CTE we want to explode out each holiday period into individual days, to prevent potential fanouts downstream in joins to schedules.
), schedule_holiday as ( 

    select
        split_multiweek_holidays._fivetran_synced,
        split_multiweek_holidays.holiday_name,
        split_multiweek_holidays.schedule_id,
        split_multiweek_holidays.holiday_valid_from,
        split_multiweek_holidays.holiday_valid_until,
        split_multiweek_holidays.holiday_starting_sunday,
        split_multiweek_holidays.holiday_ending_sunday,
        calendar_spine.date_day as holiday_date
    from split_multiweek_holidays 
    inner join calendar_spine
        on holiday_valid_from <= date_day
        and holiday_valid_until >= date_day

{% endif %}

), calculate_schedules as (

    select 
        schedule.schedule_id,
        lower(schedule.time_zone) as time_zone,
        schedule.start_time,
        schedule.end_time,
        schedule.schedule_name,
        schedule.start_time - coalesce(split_timezones.offset_minutes, 0) as start_time_utc,
        schedule.end_time - coalesce(split_timezones.offset_minutes, 0) as end_time_utc,
        coalesce(split_timezones.offset_minutes, 0) as offset_minutes,
        -- we'll use these to determine which schedule version to associate tickets with
        cast(split_timezones.valid_from as {{ dbt.type_timestamp() }}) as schedule_valid_from,
        cast(split_timezones.valid_until as {{ dbt.type_timestamp() }}) as schedule_valid_until

    from schedule
    left join split_timezones
        on split_timezones.time_zone = lower(schedule.time_zone)

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
        cast({{ dbt.date_trunc("week", "calculate_schedules.schedule_valid_from") }} as {{ dbt.type_timestamp() }}) as schedule_starting_sunday,
        cast({{ dbt.date_trunc("week", "calculate_schedules.schedule_valid_until") }} as {{ dbt.type_timestamp() }}) as schedule_ending_sunday,

        {% if var('using_holidays', True) %}
        schedule_holiday.holiday_date,
        schedule_holiday.holiday_name,
        schedule_holiday.holiday_valid_from,
        schedule_holiday.holiday_valid_until,
        schedule_holiday.holiday_starting_sunday,
        schedule_holiday.holiday_ending_sunday
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
    left join schedule_holiday
        on schedule_holiday.schedule_id = calculate_schedules.schedule_id
        and schedule_holiday.holiday_date <= calculate_schedules.schedule_valid_until
        and schedule_holiday.holiday_date >= calculate_schedules.schedule_valid_from
    {% endif %}

), split_holidays as(
    -- create records for the first day of the holiday
    select
        join_holidays.*,
        case
            when holiday_valid_from = holiday_date
                then '0_start' -- the number is for ordering later
            end as holiday_start_or_end,
        schedule_valid_from as valid_from,
        holiday_date as valid_until
    from join_holidays
    where holiday_date is not null

    union all

    -- create records for the last day of the holiday
    select
        join_holidays.*,
        case
            when holiday_valid_until = holiday_date
                then '1_end' -- the number is for ordering later
            end as holiday_start_or_end,
        holiday_date as valid_from,
        schedule_valid_until as valid_until
    from join_holidays
    where holiday_date is not null

    union all

    -- keep records for weeks with no holiday
    select
        join_holidays.*,
        cast(null as {{ dbt.type_string() }}) as holiday_start_or_end,
        schedule_valid_from as valid_from,
        schedule_valid_until as valid_until
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
        valid_from,
        valid_until,
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
        offset_minutes,
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
        valid_from_index,
        max_valid_from_index,
        holiday_start_or_end,
        case
            when holiday_start_or_end = 'partition_start'
                then schedule_starting_sunday
            when holiday_start_or_end = '0_start'
                then lag(holiday_ending_sunday) over (partition by schedule_id, start_time_utc, schedule_valid_from order by valid_from_index)
            when holiday_start_or_end = '1_end'
                then holiday_starting_sunday
            when holiday_start_or_end = 'partition_end'
                then holiday_ending_sunday
            else schedule_starting_sunday
        end as valid_from
        ,
        case 
            when holiday_start_or_end = 'partition_start'
                then holiday_starting_sunday
            when holiday_start_or_end = '0_start'
                then lead(holiday_starting_sunday) over (partition by schedule_id, start_time_utc, schedule_valid_from order by valid_from_index)
            when holiday_start_or_end = '1_end'
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
        holiday_name,
        holiday_valid_from,
        holiday_valid_until,
        valid_from,
        valid_until,
        case when holiday_start_or_end = '1_end' then true
            else false
            end as is_holiday_week
    from adjust_ranges
    where not (valid_from >= valid_until and holiday_date is not null)

), valid_minutes as(
    select
        holiday_weeks.*,
        -- Calculate holiday_valid_from in minutes from Sunday
        case when is_holiday_week then (
            {% if target.type in ('bigquery', 'databricks') %}
            -- BigQuery and Databricks use DAYOFWEEK where Sunday = 1, so subtract 1 to make Sunday = 0
                    ((extract(dayofweek from holiday_valid_from) - 1) * 24 * 60)
            {% else %}
            -- Snowflake and Postgres use DOW where Sunday = 0
                    (extract(dow from holiday_valid_from) * 24 * 60)
            {% endif %}
            + extract(hour from holiday_valid_from) * 60      -- Get hours and convert to minutes
            + extract(minute from holiday_valid_from)         -- Get minutes
            - offset_minutes                                  -- Timezone adjustment
        ) 
        else null end as holiday_valid_from_minutes_from_sunday,
        
        -- Calculate holiday_valid_until in minutes from Sunday
        case when is_holiday_week then (
            (
            {% if target.type in ('bigquery', 'databricks') %}
                    (extract(dayofweek from holiday_valid_until) - 1)
            {% else %}
                    (extract(dow from holiday_valid_until))
            {% endif %}
            + 1) * 24 * 60 -- add 1 day to set the upper bound of the holiday
            + extract(hour from holiday_valid_until) * 60
            + extract(minute from holiday_valid_until)
            - offset_minutes
        )
        else null end as holiday_valid_until_minutes_from_sunday
    from holiday_weeks

), find_holidays as(
    select 
        schedule_id,
        valid_from,
        valid_until,
        start_time_utc,
        end_time_utc,
        case 
            when start_time_utc < holiday_valid_until_minutes_from_sunday
                and end_time_utc > holiday_valid_from_minutes_from_sunday
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

    -- we want to count the number of records for each schedule start_time_utc and end_time_utc for comparison later
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
    -- This filter is for multiple holiday ids in 1 week. We want to check for each schedule start_time_utc and end_time_utc 
    -- that the holiday count matches the number of distinct records.
    -- When rows that don't match, that indicates there is a holiday on that day, and we'll filter them out. 
    where number_holidays_in_week = number_records_for_schedule_start_end
    and holiday_name is null -- this will remove schedules that fall on a holiday
)

select *
from final
