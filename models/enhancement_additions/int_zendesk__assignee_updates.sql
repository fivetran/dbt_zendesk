with ticket as (
    select *
    from {{ ref('stg_zendesk__ticket') }}

), ticket_history as (
    select *
    from {{ ref('stg_zendesk__ticket_field_history') }}

), ticket_comment as (
    select *
    from {{ ref('stg_zendesk__ticket_comment') }}

), field_comment_union as (
    select 
        ticket_id,
        user_id,
        valid_starting_at as updated_at
    from ticket_history

    union all

    select
        ticket_id,
        user_id,
        created_at as updated_at
    from ticket_comment

), ticket_combine as (
    select
        ticket.ticket_id,
        ticket.assignee_id,
        field_comment_union.updated_at

    from ticket

    left join field_comment_union
        on field_comment_union.ticket_id = ticket.ticket_id
            and field_comment_union.user_id = ticket.assignee_id

), final as (
    select 
        ticket_id,
        assignee_id,
        max(updated_at) as last_updated,
        count(*) as total_updates
    from ticket_combine

    group by 1, 2
)

select * 
from final