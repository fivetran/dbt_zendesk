with commenter as (

    select * from {{ ref('stg_zendesk__user') }}
), 

ticket_reply_times as (
  
    select * from {{ ref('int_zendesk__ticket_reply_time_metrics') }}
), 

final as (

  select ticket_reply_times.*,
    commenter.name as responding_agent_name,
    commenter.email as responding_agent_email
  from ticket_reply_times
  left join commenter
    on commenter.user_id = ticket_reply_times.responding_agent_user_id
)

select * 
from final