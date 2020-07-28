view: zendesk_ticket_fact {
  derived_table: {
    sql: with public_agent_responses as (
        select
          ticket.id as ticket_id,
          ticket_comment.id as ticket_comment_id,
          timestamp_diff(ticket_comment.created, ticket.created_at, minute) as minute_diff_ticket_created_and_comment,
          row_number() over (partition by ticket_id order by created) as public_comment_count,
          ticket.status as ticket_status
        from `digital-arbor-400`.zendesk_new.ticket_comment as ticket_comment
        join `digital-arbor-400`.zendesk_new.user as user on user.id = ticket_comment.user_id
        join `digital-arbor-400`.zendesk_new.ticket as ticket on ticket.id = ticket_comment.ticket_id
        where public
          and ticket.created_at != ticket_comment.created -- excludes first public comment, regardless of role
          and user.role = 'agent'
      ), agent_response_metrics as (
        select
          distinct ticket_id,
          min(minute_diff_ticket_created_and_comment) over (partition by ticket_id) as ticket_first_reply_minutes,
          max(case when ticket_status in ('closed','solved') then public_comment_count else null end) over (partition by ticket_id) = 1 as is_one_touch_resolution
        from public_agent_responses
       ), ticket_solved_events as (
        select
         ticket.id as ticket_id,
         updated as solved_at,
         timestamp_diff(ticket_field_history.updated, ticket.created_at, minute) as resolution_time_minutes,
         row_number() over (partition by ticket_id order by updated) as solved_count,
        from `digital-arbor-400`.zendesk_new.ticket_field_history as ticket_field_history
        join `digital-arbor-400`.zendesk_new.ticket as ticket on ticket.id = ticket_field_history.ticket_id
        where ticket_field_history.field_name = 'status'
          and ticket_field_history.value = 'solved'
      ), resolution_metrics as (
        select
          distinct ticket_id,
          max(solved_at) over (partition by ticket_id) as last_solved_at,
          min(resolution_time_minutes) over (partition by ticket_id) as first_resolution_time_minutes,
          max(resolution_time_minutes) over (partition by ticket_id) as full_resolution_time_minutes,
          max(solved_count) over (partition by ticket_id) > 1 as is_reopened
        from ticket_solved_events
      ), status_changes as (
        select
          ticket_field_history.ticket_id,
          ticket_field_history.updated as valid_from,
          coalesce(lead(ticket_field_history.updated) over (partition by ticket_id order by ticket_field_history.updated)
              , '2999-12-31 23:59:59 UTC') as valid_until,
          ticket_field_history.value as status
        from `digital-arbor-400`.zendesk_new.ticket_field_history as ticket_field_history
          where ticket_field_history.field_name = 'status'
        ), status_metrics as (
        select
          ticket.id as ticket_id,
          eom_status.status as eom_status,
          thirty_day_status.status as thirty_day_status
        from `digital-arbor-400`.zendesk_new.ticket
        join status_changes as thirty_day_status on thirty_day_status.ticket_id = ticket.id
          and thirty_day_status.valid_from <= timestamp_add(created_at, interval 30 day)
          and thirty_day_status.valid_until > timestamp_add(created_at, interval 30 day)
        join status_changes as eom_status on eom_status.ticket_id = ticket.id
          and eom_status.valid_from <= timestamp_sub(timestamp(date_trunc(date_add(date(created_at), interval 1 month), month)), interval 1 second)
          and eom_status.valid_until > timestamp_sub(timestamp(date_trunc(date_add(date(created_at), interval 1 month), month)), interval 1 second)
      )
       select
        ticket.id as ticket_id,
        ticket.status as ticket_status,
        agent_response_metrics.ticket_first_reply_minutes as ticket_first_reply_minutes,
        agent_response_metrics.is_one_touch_resolution as is_one_touch_resolution,
        resolution_metrics.last_solved_at as last_solved_at,
        resolution_metrics.first_resolution_time_minutes as first_resolution_time_minutes,
        resolution_metrics.full_resolution_time_minutes as full_resolution_time_minutes,
        resolution_metrics.is_reopened as is_reopened,
        status_metrics.eom_status,
        status_metrics.thirty_day_status
       from `digital-arbor-400`.zendesk_new.ticket as ticket
       left join agent_response_metrics on agent_response_metrics.ticket_id = ticket.id
       left join resolution_metrics on resolution_metrics.ticket_id = ticket.id
       left join status_metrics on status_metrics.ticket_id = ticket.id
       order by ticket.id desc
       ;;