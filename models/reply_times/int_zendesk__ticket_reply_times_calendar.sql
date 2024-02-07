with ticket as (

  select *
  from {{ ref('stg_zendesk__ticket') }}

), ticket_reply_times as (

  select *
  from {{ ref('int_zendesk__ticket_reply_times') }}

)

select

  ticket.ticket_id,
  ticket.source_relation,
  sum(case when is_first_comment then reply_time_calendar_minutes
    else null end) as first_reply_time_calendar_minutes,
  sum(reply_time_calendar_minutes) as total_reply_time_calendar_minutes --total combined time the customer waits for internal response
  
from ticket
left join ticket_reply_times
  on ticket.ticket_id = ticket_reply_times.ticket_id 
  and ticket.source_relation = ticket_reply_times.source_relation

group by 1, 2