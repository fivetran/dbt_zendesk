with sla_metric_by_ticket as (
    -- Step 1, Figure out when tickets had SLA policies applied
    with sla_policy_created as (
        select
          id as sla_policy_id,
          created_at
        from zendesk.sla_policy_history
        group by 1,2
    ), sla_policy_metric_history as (
        select
          *,
          coalesce(
            lead(sla_policy_updated_at) over (partition by sla_policy_id, business_hours, metric, priority order by sla_policy_updated_at),
            '2999-12-31 23:59:59 UTC')
            as valid_ending_at,
          row_number () over (partition by sla_policy_id, business_hours, metric, priority order by sla_policy_updated_at) as revision_number
        from zendesk.sla_policy_metric_history
    ), sla_policy_history as (
        select
            sla_policy_metric_history.sla_policy_id,
            case when sla_policy_metric_history.revision_number = 1 then sla_policy_created.created_at
              else sla_policy_metric_history.sla_policy_updated_at end as valid_starting_at,
            sla_policy_metric_history.valid_ending_at,
            sla_policy_metric_history.business_hours, 
            sla_policy_metric_history.metric, 
            sla_policy_metric_history.priority, 
            sla_policy_metric_history.target
        from sla_policy_metric_history
        join sla_policy_created
            on sla_policy_created.sla_policy_id = sla_policy_metric_history.sla_policy_id
    ), ticket_sla_policy as (
        select
          sla_policy_id,
          ticket_id,
          policy_applied_at
        from zendesk.ticket_sla_policy
        where ticket_id = 36871
    ), ticket_priority_history as (
        select
          ticket_id,
          updated as valid_starting_at,
          coalesce(
            lead(updated) over (partition by ticket_id, field_name order by updated),
            '2999-12-31 23:59:59 UTC')
            as valid_ending_at,
          value as ticket_priority
        from zendesk.ticket_field_history
        where field_name = 'priority'
    )
      select 
          ticket_sla_policy.sla_policy_id,
          ticket_sla_policy.ticket_id,
          ticket_sla_policy.policy_applied_at as sla_applied_at,
          ticket_priority_history.ticket_priority,
          sla_policy_history.business_hours,
          sla_policy_history.metric, 
          sla_policy_history.priority, 
          sla_policy_history.target,
          row_number() over (partition by ticket_sla_policy.ticket_id, sla_policy_history.metric, sla_policy_history.business_hours order by ticket_sla_policy.policy_applied_at) -1 as instance_id
      from ticket_sla_policy

      join ticket_priority_history
          on ticket_sla_policy.ticket_id = ticket_priority_history.ticket_id
          and (timestamp_diff(ticket_sla_policy.policy_applied_at, ticket_priority_history.valid_starting_at, second) between -2 and 0
              or ticket_sla_policy.policy_applied_at >= ticket_priority_history.valid_starting_at) -- there can be a 1-2 second diff so accounting for that
          and ticket_sla_policy.policy_applied_at < ticket_priority_history.valid_ending_at

      join sla_policy_history
          on ticket_sla_policy.sla_policy_id = sla_policy_history.sla_policy_id
          and ticket_sla_policy.policy_applied_at >= sla_policy_history.valid_starting_at
          and ticket_sla_policy.policy_applied_at < sla_policy_history.valid_ending_at
          and ticket_priority_history.ticket_priority = sla_policy_history.priority
)


--- Step 2, Figure out when tickets had SLA policies applied


,  max_solved_by_ticket as (
        select 
            ticket_id,
            max(updated) as time 
        from zendesk.ticket_field_history
         where field_name = 'status'
            and value = 'solved'
--             and ticket_id = 36871
        group by 1
    ),
    ----- Look at ticket status to calculate pause times ------
    status_timeline as (
        select 
            ticket_field_history.ticket_id,
            sla_metric_by_ticket.metric,
            sla_metric_by_ticket.business_hours,
            value as status, 
            updated as time,
            value = 'solved' and updated = max_solved_by_ticket.time as is_last_solved,
            case 
                when metric = 'agent_work_time' and (value in ('pending','hold') or (value = 'solved' and updated < max_solved_by_ticket.time)) then updated
                when metric = 'requester_wait_time' and value in ('pending') then updated
                else null
            end as time_paused,
            case
                when metric = 'agent_work_time' and (value in ('pending','hold') or (value = 'solved' and updated < max_solved_by_ticket.time)) then true
                when metric = 'requester_wait_time' and value in ('pending') then true
                else null
            end as paused,
            lag(value) over (partition by ticket_field_history.ticket_id, metric, business_hours order by updated) as previous_status
        from zendesk.ticket_field_history
        join sla_metric_by_ticket on ticket_field_history.ticket_id = sla_metric_by_ticket.ticket_id
        left join max_solved_by_ticket on ticket_field_history.ticket_id = max_solved_by_ticket.ticket_id
        where field_name = 'status'
            and metric in ('agent_work_time', 'requester_wait_time')
            and ticket_field_history.value != 'closed'
    )
--    select * from status_timeline 
    , flagging_paused_session as (
    select 
        ticket_id,
        metric,
        business_hours,
        time,
        status,
        time_paused,
        if(paused and not coalesce(lag(paused) over (partition by ticket_id, metric, business_hours order by time) ,false),1,0)  as new_session,
        paused,
        case 
            when metric = 'agent_work_time' and not coalesce(paused,false) and (previous_status in ('pending','hold','solved')) then time
            when metric = 'requester_wait_time' and not coalesce(paused,false) and previous_status in ('pending') and status not in ('pending') then time
        end as resume_time
    from status_timeline
    )   
    
, pause_session_number as (
    select 
        *,
        if (paused,sum(new_session) over (partition by ticket_id, metric, business_hours order by time), null) as session_number
    from flagging_paused_session
), pause_time_per_session as (
    select 
        *,
        min(time_paused) over (partition by ticket_id, metric, business_hours, session_number order by time) as pause_time 
    from pause_session_number
), combined as (
    select 
        ticket_id,
        metric,
        business_hours,
        time,
        pause_time,
        if(paused,coalesce(min(resume_time) over (partition by ticket_id,metric, business_hours order by time rows between current row and unbounded following),'2999-12-31 23:59:59 UTC'),null) as resume_time
    from pause_time_per_session
)
, pause_times as (
    select distinct
        ticket_id,
        metric,
        pause_time,
        resume_time
    from combined
    where pause_time is not null
), total_pause_time as (
    select 
        ticket_id,
        metric,
        sum(timestamp_diff(least(resume_time,current_timestamp()), pause_time, minute)) total_paused
    from pause_times
    group by 1, 2
), ticket_schedule_picked as (
-- use first schedule picked per ticket
    select distinct 
        ticket.id as ticket_id,
        coalesce(first_value(schedule_id) over (partition by ticket_id order by ticket_schedule.created_at asc),15574) schedule_id
    from zendesk.ticket
    left join zendesk.ticket_schedule
    on ticket.id = ticket_schedule.ticket_id
)

, schedule_business_minutes_in_week as (
-- Total minutes in week, used for weeks array
    select 
        id as schedule_id,
        sum(end_time_utc - start_time_utc) as business_minutes_in_week
    from zendesk.schedule
    group by 1
),business_weeks_to_target as (
    select 
        sla_metric_by_ticket.ticket_id,
        sla_metric_by_ticket.metric,
        sla_metric_by_ticket.business_hours,
        sla_metric_by_ticket.instance_id,
        sla_metric_by_ticket.sla_policy_id,
        sla_metric_by_ticket.sla_applied_at,
        ticket_schedule_picked.schedule_id,
        sla_metric_by_ticket.target,
        cast((ceiling(sla_metric_by_ticket.target + coalesce(total_pause_time.total_paused,0)) / schedule_business_minutes_in_week.business_minutes_in_week) as int64)+1 as  business_weeks_to_target
    from sla_metric_by_ticket
    join ticket_schedule_picked on sla_metric_by_ticket.ticket_id = ticket_schedule_picked.ticket_id 
    join schedule_business_minutes_in_week on ticket_schedule_picked.schedule_id = schedule_business_minutes_in_week.schedule_id
    left join total_pause_time on sla_metric_by_ticket.ticket_id = total_pause_time.ticket_id 
        and sla_metric_by_ticket.metric = total_pause_time.metric
)

, adding_start_end_times as (
    select
        business_weeks_to_target.ticket_id,
        business_weeks_to_target.metric,
        business_weeks_to_target.business_hours,
        business_weeks_to_target.instance_id,
        business_weeks_to_target.sla_policy_id,
        business_weeks_to_target.sla_applied_at,
        business_weeks_to_target.schedule_id,
        business_weeks_to_target.target,
        business_weeks_to_target.business_weeks_to_target,
        coalesce(schedule.start_time_utc, 0) as start_time_utc, -- if not in business hours, use full week schedule, 0 - 10080 minutes
        coalesce(schedule.end_time_utc, 10080) as end_time_utc
    from business_weeks_to_target 
    left join zendesk.schedule on business_weeks_to_target.schedule_id = schedule.id
      and business_weeks_to_target.business_hours
)

, adding_week_number as (
    select
        *
    from adding_start_end_times, unnest(generate_array(0, business_weeks_to_target)) as week_number
), adding_start_of_week as ( 
    select 
        *,
        timestamp_add(timestamp_trunc(sla_applied_at,week), interval week_number*(7*24*60) minute) as start_of_week
    from adding_week_number
)

, start_time_in_minutes as (
    select 
        *,
        greatest(timestamp_diff(sla_applied_at,start_of_week,minute),start_time_utc) as start_time,
        1 as multiplier
    from adding_start_of_week
)
, adding_pause_times as (
    select 
        * 
    from start_time_in_minutes
    union all
    select 
        start_time_in_minutes.ticket_id,
        start_time_in_minutes.metric,
        start_time_in_minutes.business_hours,        
        instance_id,
        sla_policy_id,
        sla_applied_at,
        schedule_id,
        target,
        business_weeks_to_target,
        greatest(start_time_utc, timestamp_diff(pause_time, start_of_week, minute)) start_time_utc,
        least(end_time_utc, timestamp_diff(resume_time, start_of_week, minute)) end_time_utc,
        week_number,
        start_of_week,
        start_time,
        -1 as multiplier
    from start_time_in_minutes
    join pause_times on start_time_in_minutes.ticket_id = pause_times.ticket_id
        and start_time_in_minutes.metric = pause_times.metric 
        and (
        (timestamp_add(start_of_week,interval start_time_utc minute) <= pause_time  and timestamp_add(start_of_week,interval end_time_utc minute) >= resume_time )
        or 
        (timestamp_add(start_of_week,interval start_time_utc minute) between pause_time  and resume_time )
        or 
        (timestamp_add(start_of_week,interval end_time_utc minute) between pause_time  and resume_time )
        ) 
)

, time_left as (
    -- track time worked against the target to calculate breach time
        select 
            *,
            target - sum((end_time_utc -if(end_time_utc +(week_number*(7*24*60)) > start_time, greatest(start_time_utc,start_time),end_time_utc) )*multiplier) over running_total as time_left
        from adding_pause_times
        window running_total as (partition by ticket_id, metric, business_hours, instance_id order by week_number,start_Time_utc, multiplier)
    ) 
    select * from time_left
    where metric = 'agent_work_time'
--     , breach_time as (
--     -- find breach by looking at min negative time_left value, and adding the time back to the schedule end time
--         select
--             ticket_id,
--             metric,
--             business_hours,
--             instance_id,
--             sla_policy_Id,
--             schedule_id,
--             min(timestamp_add(start_of_week,interval end_time_utc+time_left.time_left minute)) as breach_time
--         from time_left
--         where time_left.time_left<=0
--             and multiplier = 1
--         group by 1,2,3,4,5,6
--     )
--     , breach_1 as (

--     select 
--         breach_time.ticket_id,
--         breach_time.metric,
--         breach_time.business_hours,
--         breach_time.instance_id,
--         breach_time.sla_policy_id,
--         breach_time.schedule_id,
--         sla_metric_by_ticket.sla_applied_at sla_start_time,
--         case 
--             when breach_time.metric= 'agent_work_time' then breach_time.breach_time
--             else if(breach_time.business_hours,breach_time.breach_time,timestamp_add(sla_applied_at, interval target minute)) 
--         end as breach_time,
--         case 
--             when breach_time.metric= 'agent_work_time' then if(breach_time.business_hours,breach_time.breach_time,timestamp_add(sla_applied_at , interval target+ coalesce(total_pause_time.total_paused, 0) minute)) 
--             else if(breach_time.business_hours,breach_time.breach_time,timestamp_add(sla_applied_at, interval target minute)) 
--         end as breach_time_old,
--         sla_metric_by_ticket.target
--     from breach_time 
--     join sla_metric_by_ticket on breach_time.ticket_id = sla_metric_by_ticket.ticket_id 
--         and breach_time.metric = sla_metric_by_ticket.metric
--         and breach_time.instance_id = sla_metric_by_ticket.instance_id
--     left join total_pause_time on breach_time.ticket_id = total_pause_time.ticket_id 
--     )
--     select * from breach_1

-- -- -- --    this is the hypothetical breach time. Now need to bring in the real metric to see how it compares