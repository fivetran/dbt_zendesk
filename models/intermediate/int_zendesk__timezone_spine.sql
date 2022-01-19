with timezone as (

    select *
    from {{ var('time_zone') }}

), year_spine as (

    {{ dbt_utils.date_spine(
        datepart = "year", 
        start_date = dbt_utils.dateadd("year", - var('ticket_field_history_timeframe_years'), dbt_utils.date_trunc('year', dbt_utils.current_timestamp())),
        end_date = dbt_utils.dateadd("year", 1, dbt_utils.date_trunc('year', dbt_utils.current_timestamp())) 
        ) 
    }} 

), timezone_year_spine as (

    select 
        timezone.*,
        extract(year from year_spine.date_year) as date_year

    from timezone 
    join year_spine on true

), daylight_time as (

    select *
    from {{ var('daylight_time') }}

), timezone_dt as (

    select
        timezone_year_spine.*,
        daylight_time.daylight_start_utc,
        daylight_time.daylight_end_utc,
        daylight_time.daylight_offset,
        daylight_time.daylight_offset_minutes

    from timezone_year_spine
    left join daylight_time 
        on timezone_year_spine.time_zone = daylight_time.time_zone
        and timezone_year_spine.date_year = daylight_time.year

)


select *
from timezone_dt