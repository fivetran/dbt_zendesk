with user as (

    select *
    from {{ ref('stg_zendesk__user') }}

), ticket_enriched as (
    select *
    from {{ ref('zendesk__ticket_enriched') }}

), ticket_metrics as (
    select *
    from {{ ref('zendesk__ticket_metrics') }}

), users as (
    select distinct
        user_id
    from user

    where is_active = true

), total_users as (
    select 
        cast(1 as {{ dbt_utils.type_int() }}) as join_helper,
        count(user_id) as user_count,
    from users

    group by 1

), active_agents as (
    select distinct
        user_id
    from user

    where lower(role) != 'end-user' and is_active = true

), total_active_agents as (
    select 
        cast(1 as {{ dbt_utils.type_int() }}) as join_helper,
        count(user_id) as active_agent_count
    from active_agents

    group by 1

), deleted_user as (
    select distinct
        user_id
    from user

    where is_active = false

), total_deleted_users as (
    select 
        cast(1 as {{ dbt_utils.type_int() }}) as join_helper,
        count(user_id) as deleted_user_count
    from deleted_user

    group by 1

), end_user as (
    select distinct
        user_id
    from user

    where lower(role) = 'end-user' and is_active = true

), total_end_users as (
    select 
        cast(1 as {{ dbt_utils.type_int() }}) as join_helper,
        count(user_id) as end_user_count
    from end_user

    group by 1

), suspended_user as (
    select distinct 
        user_id
    from user
    
    where is_suspended = true

), total_suspended_users as (
    select 
        cast(1 as {{ dbt_utils.type_int() }}) as join_helper,
        count(user_id) as suspended_user_count
    from suspended_user

    group by 1

), new_ticket as (
    select distinct
        ticket_id
    from ticket_enriched

    where lower(status) = 'new'

), total_new_tickets as (
    select 
        cast(1 as {{ dbt_utils.type_int() }}) as join_helper,
        count(ticket_id) as new_ticket_count
    from new_ticket

    group by 1

), hold_ticket as (
    select distinct
        ticket_id
    from ticket_enriched

    where lower(status) = 'hold'

), total_hold_tickets as (
    select 
        cast(1 as {{ dbt_utils.type_int() }}) as join_helper,
        count(ticket_id) as on_hold_ticket_count
    from hold_ticket

    group by 1

), open_ticket as (
    select distinct
        ticket_id
    from ticket_enriched

    where lower(status) = 'open'

), total_open_tickets as (
    select 
        cast(1 as {{ dbt_utils.type_int() }}) as join_helper,
        count(ticket_id) as open_ticket_count
    from open_ticket

    group by 1

), pending_ticket as (
    select distinct
        ticket_id
    from ticket_enriched

    where lower(status) = 'pending'

), total_pending_tickets as (
    select 
        cast(1 as {{ dbt_utils.type_int() }}) as join_helper,
        count(ticket_id) as pending_ticket_count
    from pending_ticket

    group by 1

), solved_ticket as (
    select distinct
        ticket_id
    from ticket_enriched

    where lower(status) in ('solved', 'closed')

), total_solved_tickets as (
    select 
        cast(1 as {{ dbt_utils.type_int() }}) as join_helper,
        count(ticket_id) as solved_ticket_count
    from solved_ticket

    group by 1

), problem_ticket as (
    select distinct
        ticket_id
    from ticket_enriched

    where lower(type) = 'problem'

), total_problem_tickets as (
    select 
        cast(1 as {{ dbt_utils.type_int() }}) as join_helper,
        count(ticket_id) as problem_ticket_count
    from problem_ticket

    group by 1

), reassigned_ticket as (
    select distinct
        ticket_id
    from ticket_metrics

    where first_assignee_id != last_assignee_id

), total_reassigned_tickets as (
    select 
        cast(1 as {{ dbt_utils.type_int() }}) as join_helper,
        count(ticket_id) as reassigned_ticket_count
    from reassigned_ticket

    group by 1

), reopened_ticket as (
    select distinct
        ticket_id
    from ticket_metrics

    where count_reopens > 0

), total_reopened_tickets as (
    select 
        cast(1 as {{ dbt_utils.type_int() }}) as join_helper,
        count(ticket_id) as reopened_ticket_count
    from reopened_ticket

    group by 1

), surveyed_satisfaction as (
    select distinct
        ticket_id
    from ticket_enriched

    where lower(ticket_satisfaction_rating) in ('offered', 'good', 'bad')

), total_surveyed_satisfaction_tickets as (
    select 
        cast(1 as {{ dbt_utils.type_int() }}) as join_helper,
        count(ticket_id) as surveyed_satisfaction_ticket_count
    from surveyed_satisfaction

    group by 1

), unassigned_unsolved_ticket as (
    select distinct
        ticket_id
    from ticket_enriched

    where assignee_id is null and lower(status) not in ('solved', 'closed')

), total_unassigned_unsolved_tickets as (
    select 
        cast(1 as {{ dbt_utils.type_int() }}) as join_helper,
        count(ticket_id) as unassigned_unsolved_ticket_count
    from unassigned_unsolved_ticket

    group by 1

), unreplied_ticket as (
    select distinct
        ticket_id
    from ticket_metrics

    where total_agent_replies < 0

), total_unreplied_tickets as (
    select 
        cast(1 as {{ dbt_utils.type_int() }}) as join_helper,
        count(ticket_id) as unreplied_ticket_count
    from unreplied_ticket

    group by 1

), unreplied_unsolved_ticket as (
    select distinct
        ticket_id
    from ticket_metrics

    where total_agent_replies < 0 and lower(status) not in ('solved', 'closed')

), total_unreplied_unsolved_tickets as (
    select 
        cast(1 as {{ dbt_utils.type_int() }}) as join_helper,
        count(ticket_id) as unreplied_unsolved_ticket_count
    from unreplied_unsolved_ticket

    group by 1

), unsolved_ticket as (
    select distinct
        ticket_id
    from ticket_metrics

    where lower(status) not in ('solved', 'closed')

), total_unsolved_tickets as (
    select 
        cast(1 as {{ dbt_utils.type_int() }}) as join_helper,
        count(ticket_id) as unsolved_ticket_count
    from unsolved_ticket

    group by 1

), final as (
    select
        total_users.join_helper,
        total_users.user_count,
        total_active_agents.active_agent_count,
        total_deleted_users.deleted_user_count,
        total_end_users.end_user_count,
        total_suspended_users.suspended_user_count,
        total_new_tickets.new_ticket_count,
        total_hold_tickets.on_hold_ticket_count,
        total_open_tickets.open_ticket_count,
        total_pending_tickets.pending_ticket_count,
        total_solved_tickets.solved_ticket_count,
        total_problem_tickets.problem_ticket_count,
        total_reassigned_tickets.reassigned_ticket_count,
        total_reopened_tickets.reopened_ticket_count,
        total_surveyed_satisfaction_tickets.surveyed_satisfaction_ticket_count,
        total_unassigned_unsolved_tickets.unassigned_unsolved_ticket_count,
        total_unreplied_tickets.unreplied_ticket_count,
        total_unreplied_unsolved_tickets.unreplied_unsolved_ticket_count,
        total_unsolved_tickets.unsolved_ticket_count
    from total_users

    left join total_active_agents
        using(join_helper)
    
    left join total_deleted_users
        using(join_helper)
    
    left join total_end_users
        using(join_helper)

    left join total_suspended_users
        using(join_helper)

    left join total_new_tickets
        using(join_helper)

    left join total_hold_tickets
        using(join_helper)

    left join total_open_tickets
        using(join_helper)

    left join total_pending_tickets
        using(join_helper)

    left join total_solved_tickets
        using(join_helper)

    left join total_problem_tickets
        using(join_helper)

    left join total_reassigned_tickets
        using(join_helper)

    left join total_reopened_tickets
        using(join_helper)

    left join total_surveyed_satisfaction_tickets
        using(join_helper)

    left join total_unassigned_unsolved_tickets
        using(join_helper)

    left join total_unreplied_tickets
        using(join_helper)

    left join total_unreplied_unsolved_tickets
        using(join_helper)

    left join total_unsolved_tickets
        using(join_helper)
)

select *
from final