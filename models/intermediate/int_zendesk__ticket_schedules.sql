{{ config(enabled=var('using_schedules', True)) }}

with ticket as (
  
  select *
  from {{ ref('stg_zendesk__ticket') }}

), ticket_schedule as (
 
  select *
  from {{ ref('stg_zendesk__ticket_schedule') }}

), schedule as (
 
  select *
  from {{ ref('stg_zendesk__schedule') }}

), default_schedules as (
-- Goal: understand the working schedules applied to tickets, so that we can then determine the applicable business hours/schedule.
-- Your default schedule is used for all tickets, unless you set up a trigger to apply a specific schedule to specific tickets.

-- This portion of the query creates ticket_schedules for these "default" schedules, as the ticket_schedule table only includes
-- trigger schedules
  select 
    schedule_id,
    source_relation
  from (
    
    select
      schedule_id,
      source_relation,
      row_number() over (partition by source_relation order by created_at) = 1 as is_default_schedule
    from schedule

  ) as order_schedules
  where is_default_schedule

), default_schedule_events as (

{# {% if execute %}

    {% set default_schedule_id_query %}
        with set_default_schedule_flag as (
          select 
            row_number() over (order by created_at) = 1 as is_default_schedule,
            id
          from {{ source('zendesk','schedule') }}
        )
        select 
          id
        from set_default_schedule_flag
        where is_default_schedule

    {% endset %}

    {% set default_schedule_id = run_query(default_schedule_id_query).columns[0][0]|string %}

    {% endif %} #}

  select
    ticket.ticket_id,
    ticket.source_relation,
    ticket.created_at as schedule_created_at,
    default_schedules.schedule_id
  from ticket
  join default_schedules
    on ticket.source_relation = default_schedules.source_relation
  left join ticket_schedule as first_schedule
    on first_schedule.ticket_id = ticket.ticket_id
    and {{ fivetran_utils.timestamp_add('second', -5, 'first_schedule.created_at') }} <= ticket.created_at
    and first_schedule.created_at >= ticket.created_at   
    and first_schedule.source_relation = ticket.source_relation
  where first_schedule.ticket_id is null

), schedule_events as (
  
  select
    *
  from default_schedule_events
  
  union all
  
  select 
    ticket_id,
    source_relation,
    created_at as schedule_created_at,
    schedule_id
  from ticket_schedule

), ticket_schedules as (
  
  select 
    ticket_id,
    source_relation,
    schedule_id,
    schedule_created_at,
    coalesce(lead(schedule_created_at) over (partition by ticket_id, source_relation order by schedule_created_at)
            , {{ fivetran_utils.timestamp_add("hour", 1000, "" ~ dbt.current_timestamp_backcompat() ~ "") }} ) as schedule_invalidated_at
  from schedule_events

)
select
  *
from ticket_schedules