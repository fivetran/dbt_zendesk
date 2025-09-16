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

), ticket_chat_join as (

    select 
        ticket_chat_event.*,
        ticket_chat.ticket_id 

    from ticket_chat_event 
    inner join ticket_chat 
        on ticket_chat_event.chat_id = ticket_chat.chat_id
        and ticket_chat_event.source_relation = ticket_chat.source_relation
{% endif %}

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
        {# 
        We want to be able to differentiate between ticket_comment and ticket_chat comments in the next CTE 
        This is necessary because ticket_comment will batch together individual chat messages to the conversation level (in 1 record). 
        We want to remove these aggregate conversations in favor of the individual messages
        #}
        cast('comment - not chat' as {{ dbt.type_string() }}) as field_name,
        body as value,
        is_public,
        user_id,
        created_at as valid_starting_at,
        lead(created_at) over (partition by ticket_id {{ partition_by_source_relation() }} order by created_at) as valid_ending_at
    from ticket_comment

{% if var('using_ticket_chat', False) %}
    union all

    select
        source_relation,
        ticket_id,
        {# 
        We want to be able to differentiate between ticket_comment and ticket_chat comments in the next CTE 
        This is necessary because ticket_comment will batch together individual chat messages to the conversation level (in 1 record). 
        We want to remove these aggregate conversations in favor of the individual messages
        #}
        cast('comment - chat' as {{ dbt.type_string() }}) as field_name,
        message as value,
        true as is_public,
        actor_id as user_id,
        created_at as valid_starting_at,
        lead(created_at) over (partition by ticket_id {{ partition_by_source_relation() }} order by created_at) as valid_ending_at
    from ticket_chat_join
{% endif %}

), final as (
    select
        updates_union.source_relation,
        updates_union.ticket_id,
        {# Now group comments back together since the conversation batches are filtered out in the where clause #}
        case 
            when updates_union.field_name in ('comment - chat', 'comment - not chat') then 'comment' 
        else updates_union.field_name end as field_name,
        updates_union.value,
        updates_union.is_public,
        updates_union.user_id,
        updates_union.valid_starting_at,
        updates_union.valid_ending_at,
        tickets.created_at as ticket_created_date
    from updates_union

    left join tickets
        on tickets.ticket_id = updates_union.ticket_id
        and tickets.source_relation = updates_union.source_relation

    {# 
    What's excluded: The chat conversation batches from ticket_comment. These are marked as `comment - not chat` and are associated with tickets from `chat` or `native_messaging` channels
    What's included: 
        - Individual chat messages from ticket_chat_event. These are marked as `comment - chat`
        - True comments from ticket_comment. We know a record is a true ticket_comment if the ticket is NOT from `chat` or `native_messaging` channels
    #}
    where not (updates_union.field_name = 'comment - not chat' and lower(coalesce(tickets.created_channel, '')) in ('chat', 'native_messaging'))

)

select *
from final