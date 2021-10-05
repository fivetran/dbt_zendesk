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

    select 
        *,
        -- closed tickets cannot be re-opened or updated, and solved tickets are automatically closed after a pre-defined number of days configured in your Zendesk settings
        cast( {{ dbt_utils.date_trunc('day', "case when status != 'closed' then " ~ dbt_utils.current_timestamp() ~ " else updated_at end") }} as date) as open_until
    from {{ var('ticket') }}
    
), joined as (

    select 
        calendar.date_day,
        ticket.ticket_id
    from calendar
    inner join ticket
        on calendar.date_day >= cast(ticket.created_at as date)
        -- use this variable to extend the ticket's history past its close date (for reporting/data viz purposes :-)
        and {{ dbt_utils.dateadd('month', var('ticket_field_history_extension_months', 0), 'ticket.open_until') }} >= calendar.date_day

), surrogate_key as (

    select
        *,
        {{ dbt_utils.surrogate_key(['date_day','ticket_id']) }} as ticket_day_id
    from joined

)

select *
from surrogate_key