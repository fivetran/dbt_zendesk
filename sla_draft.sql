-- step 1, figure out when sla was applied to tickets
-- with sla_policy_applied as ( 
  
--   select
--     ticket_field_history.ticket_id,
--     ticket.created_at as ticket_created_at,
--     ticket.status as ticket_current_status,
--     ticket_field_history.field_name as metric,
--     ticket_field_history.updated as sla_applied_at,
--     cast(json_extract(ticket_field_history.value, '$.minutes') as int64) as target,
--     json_extract(ticket_field_history.value, '$.in_business_hours') as in_business_hours,
--   from zendesk.ticket_field_history
--   join zendesk.ticket
--     on ticket.id = ticket_field_history.ticket_id
--   where value is not null
--     and field_name in ('next_reply_time', 'first_reply_time', 'agent_work_time')

--REPLY TIME BREACH
-- step 2, figure out when the sla will breach for sla's in calendar hours. the calculation is relatively straightforward.
), reply_time_calendar_hours_breached as ( 
  
  select
    *,
    timestamp_add(sla_applied_at, interval cast(target as int64) minute) as breached_at
  from sla_policy_applied
  where in_business_hours = 'false'
    and metric in ('next_reply_time', 'first_reply_time')

-- step 3, figure out when the sla will breach for sla's in business hours.

-- step 3a, understand the working schedules applied to tickets, so that we can understand which business hours apply
-- ZD applies default schedules to tickets, the default schedule is always the first schedule created.  Then, triggers are 
-- applied to make changes to the ticket schedule.  These trigger events are captured in the ticket_schedule table.

), default_schedule_events as (
  
  select
    ticket.id as ticket_id,
    ticket.created_at as schedule_created_at,
    15574 as schedule_id -- Sisense's default schedule ID, which is the first schedule created. (Full office coverage, Jerusalem)
  from zendesk.ticket
  left join zendesk.ticket_schedule as first_schedule
    on first_schedule.ticket_id = ticket.id
    and timestamp_add(first_schedule.created_at, interval -5 second) <= ticket.created_at
    and first_schedule.created_at >= ticket.created_at    
  where first_schedule.ticket_id is null
), schedule_events as (
  
  select
    *
  from default_schedule_events
  
  union all
  
  select 
    ticket_id,
    created_at as schedule_created_at,
    schedule_id
  from zendesk.ticket_schedule

), ticket_schedule as (
  
  select 
    ticket_id,
    schedule_id,
    schedule_created_at,
    coalesce(lead(schedule_created_at) over (partition by ticket_id order by schedule_created_at)
            , timestamp("9999-12-31 01:01:01")) as schedule_invalidated_at
  from schedule_events
      
-- step 3b, using the sla target and sla_applied_at, figure out when the breach will happen for sla's that are in business hours.

), schedule_business_hours as (
  
  select 
    id as schedule_id,
    sum(end_time_utc - start_time_utc) as total_schedule_weekly_business_minutes
  from zendesk.schedule
  group by 1

), ticket_sla_applied_with_schedules as (

  select 
    sla_policy_applied.*,
    ticket_schedule.schedule_id,
    round(
      timestamp_diff(sla_policy_applied.sla_applied_at, 
        timestamp_trunc(sla_policy_applied.sla_applied_at, week), second)/60
      , 0) as start_time_in_minutes_from_week,
      schedule_business_hours.total_schedule_weekly_business_minutes
  from sla_policy_applied
  left join ticket_schedule on sla_policy_applied.ticket_id = ticket_schedule.ticket_id
    and timestamp_add(ticket_schedule.schedule_created_at, interval -1 second) <= sla_policy_applied.sla_applied_at
    and timestamp_add(ticket_schedule.schedule_invalidated_at, interval -1 second) > sla_policy_applied.sla_applied_at
  left join schedule_business_hours 
    on ticket_schedule.schedule_id = schedule_business_hours.schedule_id
  where sla_policy_applied.in_business_hours = 'true'
    and metric in ('next_reply_time', 'first_reply_time')
  
), weekly_periods as (
  
  select 
    ticket_sla_applied_with_schedules.*,
    week_number,
    greatest(0, start_time_in_minutes_from_week - week_number * (7*24*60)) as ticket_week_start_time,
    (7*24*60) as ticket_week_end_time
  from ticket_sla_applied_with_schedules, 
    unnest(generate_array(0, ceiling(target/total_schedule_weekly_business_minutes), 1)) as week_number  --generate the number of possible weeks the SLA breach will take

), intercepted_periods as (

  select 
    weekly_periods.*,
    schedule.start_time_utc as schedule_start_time,
    schedule.end_time_utc as schedule_end_time,
    (schedule.end_time_utc - greatest(ticket_week_start_time,schedule.start_time_utc)) as lapsed_business_minutes,
    sum(schedule.end_time_utc - greatest(ticket_week_start_time,schedule.start_time_utc)) over 
      (partition by ticket_id, metric, sla_applied_at order by week_number, schedule.start_time_utc) as sum_lapsed_business_minutes
  from weekly_periods
  join zendesk.schedule on ticket_week_start_time <= schedule.end_time_utc 
    and ticket_week_end_time >= schedule.start_time_utc
    and weekly_periods.schedule_id = schedule.id
  
), intercepted_periods_with_breach_flag as (
  
  select 
    *,
    target - sum_lapsed_business_minutes as remaining_minutes,
    case when (target - sum_lapsed_business_minutes) < 0 
      and 
        (lag(target - sum_lapsed_business_minutes) over
        (partition by ticket_id, metric, sla_applied_at order by week_number, schedule_start_time) >= 0 
        or 
        lag(target - sum_lapsed_business_minutes) over
        (partition by ticket_id, metric, sla_applied_at order by week_number, schedule_start_time) is null) 
        then true else false end as is_breached_during_schedule -- this flags the scheduled period on which the breach took place
  from intercepted_periods

), intercepted_periods_with_breach_flag_calculated as (

  select
    *,
    schedule_end_time + remaining_minutes as breached_at_minutes,
    timestamp_trunc(sla_applied_at, week) as starting_point,
    timestamp_add(timestamp_trunc(sla_applied_at, week), interval cast(((7*24*60) * week_number) + (schedule_end_time + remaining_minutes) as int64) minute) as breached_at
  from intercepted_periods_with_breach_flag
  where is_breached_during_schedule

), reply_time_business_hours_breached as (

  select
    ticket_id,
    metric,
    sla_applied_at,
    target,
    in_business_hours,
    breached_at
  from intercepted_periods_with_breach_flag_calculated
-- Now we have both calendar and business hours breached_time calculated for SLAs. Unioned together below
), reply_time_breached_at as (

  select 
    ticket_id,
    metric,
    sla_applied_at,
    target,
    in_business_hours,
    breached_at
  from reply_time_calendar_hours_breached

  union all

  select 
    *
  from reply_time_business_hours_breached

-- Now that we have the breach time, see when the first reply after the sla policy was applied took place.
), ticket_solved_times as (
  select
    ticket_id,
    updated as solved_at
  from zendesk.ticket_field_history
  where field_name = 'status'
  and value in ('solved','closed')

), reply_time as (
    select 
      ticket_comment.ticket_id,
      ticket_comment.created as reply_at,
      commenter.role
    from zendesk.ticket_comment
    join zendesk.user as commenter
      on commenter.id = ticket_comment.user_id
    where ticket_comment.public
    and (commenter.role in ('agent','admin')
    or commenter.email like '%@sisense.com')

), reply_time_breached_at_with_next_reply_timestamp as (

  select 
    reply_time_breached_at.*,
    min(reply_at) as agent_reply_at,
    min(solved_at) as next_solved_at
  from reply_time_breached_at
  left join reply_time
    on reply_time.ticket_id = reply_time_breached_at.ticket_id
    and reply_time.reply_at > reply_time_breached_at.sla_applied_at
  left join ticket_solved_times
    on reply_time_breached_at.ticket_id = ticket_solved_times.ticket_id
    and ticket_solved_times.solved_at > reply_time_breached_at.sla_applied_at
  group by 1,2,3,4,5,6
), reply_time_breached_at_remove_old_sla as (
  select 
    *,
    lead(sla_applied_at) over (partition by ticket_id, metric, in_business_hours order by sla_applied_at) as updated_sla_policy_starts_at,
    case when 
      lead(sla_applied_at) over (partition by ticket_id, metric, in_business_hours order by sla_applied_at) --updated sla policy start at time
      < breached_at then true else false end as is_stale_sla_policy
  from reply_time_breached_at_with_next_reply_timestamp
  
-- final query that filters out tickets that were solved or replied to before breach time
), reply_time_breach as (
  select 
    * 
  from reply_time_breached_at_remove_old_sla
  where (breached_at < agent_reply_at and breached_at < next_solved_at)
    or (breached_at < agent_reply_at and next_solved_at is null)
    or (agent_reply_at is null and breached_at < next_solved_at)
    or (agent_reply_at is null and next_solved_at is null)    

-- AGENT WORK TIME
-- This is more complicated, as SLAs minutes are only counted while the ticket is in 'new' or 'open' status.

-- For business hours, only 'new' or 'open' status hours are counted if they are also during business hours
), agent_work_time_business_sla as (
  select
    sla_policy_applied.*
  from sla_policy_applied 
  where sla_policy_applied.metric = 'agent_work_time'
    and sla_policy_applied.in_business_hours = 'true'

-- Figure out when the ticket was in 'new' and 'open'
), ticket_historical_status as (

  select
    ticket_id,
    updated as valid_starting_at,
    coalesce(lead(updated) over (partition by ticket_id, field_name order by updated)
      , timestamp_add(current_timestamp, interval 30 day)) as valid_ending_at,
    value as status,
  from zendesk.ticket_field_history
  where field_name = 'status'

), ticket_agent_work_times as (

  select  
    ticket_historical_status.ticket_id,
    agent_work_time_business_sla.ticket_created_at,
    greatest(ticket_historical_status.valid_starting_at, agent_work_time_business_sla.sla_applied_at) as valid_starting_at,
    ticket_historical_status.valid_ending_at,
    agent_work_time_business_sla.sla_applied_at,
    agent_work_time_business_sla.target,    
  from ticket_historical_status
  join agent_work_time_business_sla
    on ticket_historical_status.ticket_id = agent_work_time_business_sla.ticket_id
  where status in ('new', 'open') -- these are the only statuses that count as "agent work time"
  and sla_applied_at < valid_ending_at

), schedule as (

    select
      id as schedule_id,
      start_time_utc,
      end_time_utc
    from zendesk.schedule

-- cross schedules with work time
), ticket_status_crossed_with_schedule as (
  
    select
      ticket_agent_work_times.ticket_id,
      ticket_agent_work_times.sla_applied_at,
--       ticket_agent_work_times.ticket_created_at,
      ticket_agent_work_times.target,      
      ticket_schedule.schedule_id,
      greatest(valid_starting_at, schedule_created_at) as valid_starting_at,
      least(valid_ending_at, schedule_invalidated_at) as valid_ending_at
    from ticket_agent_work_times
    left join ticket_schedule
      on ticket_agent_work_times.ticket_id = ticket_schedule.ticket_id
    where timestamp_diff(least(valid_ending_at, schedule_invalidated_at), greatest(valid_starting_at, schedule_created_at), second) > 0


), ticket_full_solved_time as (

    select 
      ticket_status_crossed_with_schedule.*,
      round(timestamp_diff(
              ticket_status_crossed_with_schedule.valid_starting_at, 
              timestamp_trunc(
                  ticket_status_crossed_with_schedule.valid_starting_at, 
                  week), 
              second)/60,
            0) as valid_starting_at_in_minutes_from_week,
      round(timestamp_diff(
              ticket_status_crossed_with_schedule.valid_ending_at, 
              ticket_status_crossed_with_schedule.valid_starting_at, 
              second)/60,
            0) as raw_delta_in_minutes
    from ticket_status_crossed_with_schedule
    group by 1, 2, 3, 4, 5, 6, 7

), weekly_period_agent_work_time as (

    select 
      ticket_id,
      sla_applied_at,
      valid_starting_at,
      valid_ending_at,
      target,
      valid_starting_at_in_minutes_from_week,
      raw_delta_in_minutes,
      week_number,
      schedule_id,
      greatest(0, valid_starting_at_in_minutes_from_week - week_number * (7*24*60)) as ticket_week_start_time_minute,
      least(valid_starting_at_in_minutes_from_week + raw_delta_in_minutes - week_number * (7*24*60), (7*24*60)) as ticket_week_end_time_minute
    from ticket_full_solved_time,
        unnest(generate_array(0, floor((valid_starting_at_in_minutes_from_week + raw_delta_in_minutes) / (7*24*60)), 1)) as week_number

), intercepted_periods_agent as (
  
    select 
      weekly_period_agent_work_time.ticket_id,
      weekly_period_agent_work_time.sla_applied_at,
      weekly_period_agent_work_time.target,
      weekly_period_agent_work_time.valid_starting_at,
      weekly_period_agent_work_time.valid_ending_at,
      weekly_period_agent_work_time.week_number,
      weekly_period_agent_work_time.ticket_week_start_time_minute,
      weekly_period_agent_work_time.ticket_week_end_time_minute,
      schedule.start_time_utc as schedule_start_time,
      schedule.end_time_utc as schedule_end_time,
      least(ticket_week_end_time_minute, schedule.end_time_utc) - greatest(weekly_period_agent_work_time.ticket_week_start_time_minute, schedule.start_time_utc) as scheduled_minutes,
    from weekly_period_agent_work_time
    join schedule on ticket_week_start_time_minute <= schedule.end_time_utc 
      and ticket_week_end_time_minute >= schedule.start_time_utc
      and weekly_period_agent_work_time.schedule_id = schedule.schedule_id

), intercepted_periods_with_running_total as (
  
    select 
      *,
      sum(scheduled_minutes) over 
        (partition by ticket_id, sla_applied_at order by valid_starting_at, week_number, schedule_end_time)
        as running_total_scheduled_minutes

    from intercepted_periods_agent

), intercepted_periods_agent_with_breach_flag as (
  select 
    intercepted_periods_with_running_total.*,
    target - running_total_scheduled_minutes as remaining_target_minutes,
    case when (target - running_total_scheduled_minutes) = 0 then true
       when (target - running_total_scheduled_minutes) < 0 
        and 
          (lag(target - running_total_scheduled_minutes) over
          (partition by ticket_id, sla_applied_at order by valid_starting_at, week_number, schedule_end_time) > 0 
          or 
          lag(target - running_total_scheduled_minutes) over
          (partition by ticket_id, sla_applied_at order by valid_starting_at, week_number, schedule_end_time) is null) 
          then true else false end as is_breached_during_schedule
          
  from  intercepted_periods_with_running_total

), intercepted_periods_agent_filtered as (

  select
    *,
    (remaining_target_minutes + scheduled_minutes) as breach_minutes,
    greatest(ticket_week_start_time_minute, schedule_start_time) + (remaining_target_minutes + scheduled_minutes) as breach_minutes_from_week
  from intercepted_periods_agent_with_breach_flag
  where is_breached_during_schedule
  
-- Now we have agent work time business hours breached_at timestamps. Only SLAs that have been breached will appear in this list, otherwise
-- would be filtered out in the above
), agent_work_business_breach as (
  
  select 
    *,
    timestamp_add(
      timestamp_trunc(valid_starting_at, week),
      interval cast(((7*24*60) * week_number) + breach_minutes_from_week as int64) minute) as breached_at
  from intercepted_periods_agent_filtered

-- Calculate breach time for agent work time, calendar hours
), agent_work_time_calendar_sla as (

  select
    sla_policy_applied.*
  from sla_policy_applied 
  where sla_policy_applied.metric = 'agent_work_time'
    and sla_policy_applied.in_business_hours = 'false'
    
), ticket_agent_work_times_post_sla as (
  select  
    ticket_historical_status.ticket_id,
    greatest(ticket_historical_status.valid_starting_at, agent_work_time_calendar_sla.sla_applied_at) as valid_starting_at,
    ticket_historical_status.valid_ending_at,
    ticket_historical_status.status as ticket_status,
    agent_work_time_calendar_sla.metric,
    agent_work_time_calendar_sla.sla_applied_at,
    agent_work_time_calendar_sla.target,    
    agent_work_time_calendar_sla.ticket_created_at
  from ticket_historical_status
  join agent_work_time_calendar_sla
    on ticket_historical_status.ticket_id = agent_work_time_calendar_sla.ticket_id
  where status in ('new', 'open')
  and sla_applied_at < valid_ending_at

), agent_work_time_calendar_minutes as (

  select 
    *,
    timestamp_diff(valid_ending_at, valid_starting_at, minute) as calendar_minutes,
    sum(timestamp_diff(valid_ending_at, valid_starting_at, minute)) 
      over (partition by ticket_id, sla_applied_at order by valid_starting_at) as running_total_calendar_minutes
  from ticket_agent_work_times_post_sla

), agent_work_time_calendar_minutes_flagged as (

select 
  agent_work_time_calendar_minutes.*,
  target - running_total_calendar_minutes as remaining_target_minutes,
  case when (target - running_total_calendar_minutes) < 0 
      and 
        (lag(target - running_total_calendar_minutes) over
        (partition by ticket_id, sla_applied_at order by valid_starting_at) >= 0 
        or 
        lag(target - running_total_calendar_minutes) over
        (partition by ticket_id, sla_applied_at order by valid_starting_at) is null) 
        then true else false end as is_breached_during_schedule
        
from  agent_work_time_calendar_minutes

), agent_work_calendar_breach as (

  select
    *,
    (remaining_target_minutes + calendar_minutes) as breach_minutes,
    timestamp_add(valid_starting_at, 
      interval (remaining_target_minutes + calendar_minutes) minute) as breached_at
  from agent_work_time_calendar_minutes_flagged
  where is_breached_during_schedule

), all_breaches_unioned as (
  select
    ticket_id,
    metric,
    sla_applied_at,
    target,
    in_business_hours,
    breached_at
  from reply_time_breach

union all

  select
    ticket_id,
    'agent_work_time' as metric,
    sla_applied_at,
    target,
    'false' as in_business_hours,
    breached_at
  from agent_work_calendar_breach

union all 

  select 
    ticket_id,
    'agent_work_time' as metric,
    sla_applied_at,
    target,
    'true' as in_business_hours,
    breached_at
  from agent_work_business_breach
)
select 
  *,
  breached_at > current_timestamp as is_upcoming_breach
from all_breaches_unioned
