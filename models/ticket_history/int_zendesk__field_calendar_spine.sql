{{
    config(
        materialized='incremental',
        partition_by = {'field': 'date_day', 'data_type': 'date'},
        unique_key='ticket_day_id'
    )
}}

with calendar as (

    select *
    from {{ ref('int_zendesk__calendar_spine') }}
    {% if is_incremental() %}
    where date_day >= (select max(date_day) from {{ this }})
    {% endif %}

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