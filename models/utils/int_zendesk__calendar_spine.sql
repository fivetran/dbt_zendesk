--Earliest ticket date is found in SQL rather than at compile time, so compiled SQL matches what runs and no extra query is needed.
{%- set timeframe_years = var('ticket_field_history_timeframe_years', 50) %}

with first_date as (

    select
        coalesce(
            min(cast(created_at as date)),
            cast({{ dbt.dateadd("month", -1, "current_date") }} as date)
            ) as min_date
    from {{ ref('stg_zendesk__ticket') }}
    -- by default take all the data
    where cast(created_at as date) >= {{ dbt.dateadd('year', - timeframe_years, "current_date") }}

), series as (

    -- Enough days to cover the full timeframe plus the trailing week; the filter below trims the excess.
    {{ dbt_utils.generate_series(timeframe_years * 366 + 7) }}

), spine as (

    -- generate_series builds its numbers with power(), which returns a float on BigQuery and Redshift; day intervals must be integers.
    select
        {{ dbt.dateadd("day", "cast(series.generated_number as " ~ dbt.type_int() ~ ") - 1", "first_date.min_date") }} as date_day
    from series
    cross join first_date

), recast as (
    select
        cast(date_day as date) as date_day
    from spine
    -- Exclusive, matching the dbt_utils.date_spine call this replaced.
    where cast(date_day as date) < cast({{ dbt.dateadd("week", 1, "current_date") }} as date)
)

select *
from recast
