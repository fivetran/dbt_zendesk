{{ config(materialized='table') }}

with calendar as (

    select *
    from {{ ref('int_zendesk__calendar_spine') }}

), ticket as (

    select *
    from {{ var('ticket') }}
    
), joined as (

    select 
        calendar.date_day,
        ticket.ticket_id
    from calendar
    inner join ticket
        on calendar.date_day >= cast(ticket.created_at as date)

), surrogate_key as (

    select
        *,
        {{ dbt_utils.surrogate_key(['date_day','ticket_id']) }} as ticket_day_id
    from joined

)

select *
from surrogate_key