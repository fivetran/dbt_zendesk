with ticket_history as (
    select *
    from {{ ref('stg_zendesk__ticket_field_history') }}

), ticket_comment as (
    select *
    from {{ ref('stg_zendesk__ticket_comment') }}

), updates_union as (
    select 
        ticket_id,
        field_name,
        value,
        null as is_public,
        user_id,
        valid_starting_at,
        valid_ending_at
    from ticket_history

    union all

    select
        ticket_id,
        cast('comment' as {{ dbt_utils.type_string() }}) as field_name,
        body as value,
        is_public,
        user_id,
        created_at as valid_starting_at,
        lead(created_at) over (partition by ticket_id order by created_at) as valid_ending_at
    from ticket_comment
)

select *
from updates_union