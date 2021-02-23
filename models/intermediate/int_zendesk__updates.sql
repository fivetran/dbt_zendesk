with ticket_history as (
    select *
    from {{ ref('stg_zendesk__ticket_field_history') }}

), ticket_comment as (
    select *
    from {{ ref('stg_zendesk__ticket_comment') }}

), updates_union as (
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
)

select *
from updates_union