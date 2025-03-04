with ticket_history as (
    select *
    from {{ ref('stg_zendesk__ticket_field_history') }}

), ticket_comment as (
    select *
    from {{ ref('stg_zendesk__ticket_comment') }}

), tickets as (
    select *
    from {{ ref('stg_zendesk__ticket') }}

{% if var('using_ticket_chat', False) %}
), ticket_chat as (

    select *
    from {{ ref('stg_zendesk__ticket_chat') }}

), ticket_chat_event as (

    select *
    from {{ ref('stg_zendesk__ticket_chat_event') }}
    where lower(type) = 'chatmessage'

), users as (

    select *
    from {{ ref('stg_zendesk__user') }}

), ticket_chat_join as (

    select 
        ticket_chat_event.*,
        ticket_chat.ticket_id 

    from ticket_chat_event 
    join ticket_chat 
        on ticket_chat_event.chat_id = ticket_chat.chat_id
        and ticket_chat_event.source_relation = ticket_chat.source_relation
    join users
        on ticket_chat_event.actor_id = users.user_id 
        and ticket_chat_event.source_relation = ticket_chat_event.source_relation
    where users.role in ('admin', 'agent') -- limit to internal users

{% endif %}
), comments_with_channel as (

    select 
        ticket_comment.*

    from ticket_comment 
    join tickets 
        on ticket_comment.ticket_id = tickets.ticket_id
        and ticket_comment.source_relation = tickets.source_relation
    where lower(tickets.created_channel) not in ('chat', 'native_messaging')

), updates_union as (
    select 
        source_relation,
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
        source_relation,
        ticket_id,
        cast('comment' as {{ dbt.type_string() }}) as field_name,
        body as value,
        is_public,
        user_id,
        created_at as valid_starting_at,
        lead(created_at) over (partition by source_relation, ticket_id order by created_at) as valid_ending_at
    from comments_with_channel

{% if var('using_ticket_chat', False) %}
    union all

    select
        source_relation,
        ticket_id,
        cast('comment' as {{ dbt.type_string() }}) as field_name,
        message as value,
        true as is_public,
        actor_id as user_id,
        created_at as valid_starting_at,
        lead(created_at) over (partition by source_relation, ticket_id order by created_at) as valid_ending_at
    from ticket_chat_join
{% endif %}

), final as (
    select
        updates_union.*,
        tickets.created_at as ticket_created_date
    from updates_union

    left join tickets
        on tickets.ticket_id = updates_union.ticket_id
        and tickets.source_relation = updates_union.source_relation

)

select *
from final