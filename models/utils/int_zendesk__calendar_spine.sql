-- depends_on: {{ var('ticket') }}
with spine as (

    {% if execute and flags.WHICH in ('run', 'build') %}

    {%- set first_date_query %}
    select 
        coalesce(
            min(cast(created_at as date)), 
            cast({{ dbt.dateadd("month", -1, "current_date") }} as date)
            ) as min_date
    from {{ var('ticket') }}
    -- by default take all the data 
    where cast(created_at as date) >= {{ dbt.dateadd('year', 
        - var('ticket_field_history_timeframe_years', 50), "current_date") }}
    {% endset -%}

    {%- set first_date = dbt_utils.get_single_value(first_date_query) %}

    {% else %}
    {%- set first_date = '2016-01-01' %}

    {% endif %}

{{
    dbt_utils.date_spine(
        datepart = "day", 
        start_date = "cast('" ~ first_date ~ "' as date)",
        end_date = dbt.dateadd("week", 1, "current_date")
    )   
}}

), recast as (
    select
        cast(date_day as date) as date_day
    from spine
)

select *
from recast