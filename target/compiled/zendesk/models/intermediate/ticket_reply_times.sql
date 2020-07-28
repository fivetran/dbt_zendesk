with  __dbt__CTE__stg_zendesk_ticket as (
with base as (

    select *
    from zendesk.ticket

), fields as (

    select

      id as ticket_id,
      _fivetran_synced,
      assignee_id,
      brand_id,
      created_at,
      description,
      due_at,
      group_id,
      is_public,
      organization_id,
      priority,
      recipient,
      requester_id,
      status,
      subject,
      submitter_id,
      ticket_form_id,
      type,
      updated_at,
      url,
      via_channel as created_channel,
      via_source_from_id as source_from_id,
      via_source_from_title as source_from_title,
      via_source_rel as source_rel,
      via_source_to_address as source_to_address,
      via_source_to_name as source_to_name

    from base

)

select *
from fields
),  __dbt__CTE__stg_zendesk_user as (
with base as (

    select *
    from zendesk.user

), fields as (

    select

      id as user_id,
      _fivetran_synced,
      created_at,
      email,
      name,
      organization_id,
      role,
      ticket_restriction,
      time_zone,
      active as is_active

    from base

)

select *
from fields
),  __dbt__CTE__tickets_enhanced as (
with ticket as (

    select *
    from __dbt__CTE__stg_zendesk_ticket

), user as (

    select *
    from __dbt__CTE__stg_zendesk_user

), joined as (

    select 

        ticket.*,
        requester.role as requester_role,
        requester.role = 'agent' as agent_created_ticket,
        requester.email as requester_email,
        submitter.role as submitter_role,
        submitter.email as submitter_email,

    
    from ticket

    join user as requester
        on requester.user_id = ticket.requester_id
    
    join user as submitter
        on submitter.user_id = ticket.submitter_id
)

select *
from joined
),  __dbt__CTE__stg_zendesk_ticket_comment as (
with base as (

    select *
    from zendesk.ticket_comment

), fields as (

    select

      id as ticket_comment_id,
      _fivetran_synced,
      body,
      created as created_at,
      public as is_public,
      ticket_id,
      user_id as user_id,
      facebook_comment as is_facebook_comment,
      tweet as is_tweet,
      voice_comment as is_voice_comment

    from base

)

select *
from fields
),  __dbt__CTE__public_comments as (
with public_ticket_comment as (

    select *
    from __dbt__CTE__stg_zendesk_ticket_comment
    where is_public

), user as (

    select *
    from __dbt__CTE__stg_zendesk_user

), ticket as (

    select *
    from __dbt__CTE__stg_zendesk_ticket

), joined as (

    select 

        public_ticket_comment.*,
        commenter.role as commenter_role,
        coalesce(
                lag(commenter.role) over (partition by public_ticket_comment.ticket_id  order by public_ticket_comment.created_at)
                , 'first_comment') 
                as previous_commenter_role,
        row_number() over (partition by public_ticket_comment.ticket_id order by public_ticket_comment.created_at) as public_comment_counter,
        case when role = 'agent' 
            then (row_number() over (partition by public_ticket_comment.ticket_id, role order by public_ticket_comment.created_at))
          else null end as agent_public_comment_counter,
       case when role = 'end-user' 
            then (row_number() over (partition by public_ticket_comment.ticket_id, role order by public_ticket_comment.created_at))
          else null end as end_user_public_comment_counter 
    
    from public_ticket_comment
    
    join user as commenter
        on commenter.user_id = public_ticket_comment.user_id
    
    join ticket
        on ticket.ticket_id = public_ticket_comment.ticket_id

)
select * 
from joined
),tickets_enhanced as (

    select *
    from __dbt__CTE__tickets_enhanced

), ticket_public_comments as (

    select *
    from __dbt__CTE__public_comments

), end_user_comments as (
  
  select 
    ticket_id,
    created_at as end_user_comment_created_at,
    previous_commenter_role = 'first_comment' as is_first_comment
  from ticket_public_comments 
  where commenter_role = 'end-user'
    and ticket_public_comments.previous_commenter_role != 'end-user' -- we only care about net new end user comments

), reply_timestamps as (  

  select 
    end_user_comments.*,
    min(agent_comments.created_at) as agent_responsed_at
  from end_user_comments
  left join ticket_public_comments as agent_comments
    on agent_comments.ticket_id = end_user_comments.ticket_id
    and agent_comments.commenter_role in ('agent','admin')
    and agent_comments.previous_commenter_role not in ('agent','admin') -- we only care about net new agent comments
    and agent_comments.created_at > end_user_comments.end_user_comment_created
  group by 1,2,3

)

  select
    *,
    timestamp_diff(agent_response_timestamp,end_user_comment_created, minute) as reply_time_calendar_minutes
  from reply_timestamps
  order by 1,2