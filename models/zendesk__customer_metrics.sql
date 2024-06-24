with users as (
    
    select *
    from {{ ref('stg_zendesk__user') }}
    where role = 'end-user'
), 

organizations as (
    
    select *
    from {{ ref('stg_zendesk__organization') }}
),

tickets as (

    select *
    from {{ ref('zendesk__ticket_metrics') }}
),

{# 
    Account Age
    Created Ticketss
    Resolved Tickets
    Unresolved Tickets
    Reoponed Tickets
    Average Ticket Priority
    Follow-Up Tickets (Number of follow-up tickets submitted by the customer after a resolution) -- just added
    First Contact Resolution Rate (Percentage of tickets resolved on the first interaction)
    Average Response Time
    Average Resolution Time
    CSAT Score (Average customer satisfaction score)
    Survey Response Rate -- NOT FOUND IN DATA

#}

customer_ticket_metrics as (

    select
        group_id,
        organization_id,
        requester_id,

        count(*) as count_created_tickets,
        sum(case when status in ('solved', 'closed') then 1 else 0 end) as count_resolved_tickets,
        sum(case when status not in ('solved', 'closed') then 1 else 0 end) as count_unresolved_tickets,
        sum(case when count_reopens > 0 then 1 else 0 end) as count_reopened_tickets,
        sum(case when via_followup_source_id is not null then 1 else 0 end) as count_followup_tickets,

        avg(case 
                when priority = 'low' then 0
                when priority = 'normal' then 1
                when priority = 'high' then 2
                when priority = 'urgent' then 3
            end ) as avg_ticket_priority, -- ignores nulls

        sum(case when count_public_agent_comments = 1 then 1 else 0 end) as count_first_contact_resolved_tickets,
        avg(first_reply_time_calendar_minutes) as avg_first_reply_time_calendar_minutes,
        avg(first_resolution_calendar_minutes) as avg_first_resolution_calendar_minutes,
        avg(final_resolution_calendar_minutes) as avg_final_resolution_calendar_minutes,
            
        avg(case 
                when ticket_satisfaction_score = 'bad' then -1 
                when ticket_satisfaction_score = 'good' then 1 
            end) as avg_ticket_satisfaction_score -- ignore nulls
    
        {% if var('using_schedules', true) %}
        , avg(first_reply_time_business_minutes) as avg_first_reply_time_business_minutes
        , avg(first_resolution_business_minutes) as avg_first_resolution_business_minutes
        , avg(final_resolution_business_minutes) as avg_final_resolution_business_minutes
        {% endif %}

    from tickets
    group by 1,2,3
),

final as (

    select 
        {{ dbt_utils.star(from=ref('stg_zendesk__user'), relation_alias='users') }},
        organizations.name as organization_name,
        organizations.details as organization_details,
        organizations.external_id as organization_external_id,
        {{ dbt.datediff('users.created_at', dbt.current_timestamp_backcompat(), 'day') }} as account_age_days,
        {{ dbt.datediff('organizations.created_at', dbt.current_timestamp_backcompat(), 'day') }} as organization_account_age_days,

        count_created_tickets,
        count_resolved_tickets,
        count_unresolved_tickets,
        count_reopened_tickets,
        count_followup_tickets,
        avg_ticket_priority,
        count_first_contact_resolved_tickets,
        avg_first_reply_time_calendar_minutes,
        avg_first_resolution_calendar_minutes,
        avg_final_resolution_calendar_minutes,
        avg_ticket_satisfaction_score

        {% if var('using_schedules', true) %}
        , avg_first_reply_time_business_minutes
        , avg_first_resolution_business_minutes
        , avg_final_resolution_business_minutes
        {% endif %}

    from users
    left join organizations 
        on users.organization_id = organizations.organization_id
    left join customer_ticket_metrics
        on users.user_id = customer_ticket_metrics.requester_id
)

select *
from final