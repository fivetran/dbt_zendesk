with  __dbt__CTE__stg_zendesk_ticket_comment as (
with base as (

    select *
    from `digital-arbor-400`.`zendesk`.`ticket_comment`

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
),  __dbt__CTE__stg_zendesk_user as (
with base as (

    select *
    from `digital-arbor-400`.`zendesk`.`user`

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
),  __dbt__CTE__stg_zendesk_ticket as (
with base as (

    select *
    from `digital-arbor-400`.`zendesk`.`ticket`

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
),public_ticket_comment as (

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