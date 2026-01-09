{{ config(enabled=var('using_ticket_chat', False)) }}

with base as (

    select * 
    from {{ ref('stg_zendesk__ticket_chat_event_tmp') }}
),

fields as (

    select
        {{
            fivetran_utils.fill_staging_columns(
                source_columns=adapter.get_columns_in_relation(ref('stg_zendesk__ticket_chat_event_tmp')),
                staging_columns=get_ticket_chat_event_columns()
            )
        }}

            from base
),

final as (
    
    select 
        cast(null as {{ dbt.type_string() }}) as source_relation, 
        _fivetran_synced,
        {# Very infrequently, the actor_id field may look like agent:####### instead of just ####### #}
        cast( (case when actor_id like 'agent%' then nullif({{ dbt.split_part('actor_id', "'agent:'", 2) }},'') else actor_id end) as {{ dbt.type_bigint() }}) as actor_id,
        chat_id,
        chat_index,
        cast(created_at as {{ dbt.type_timestamp() }}) as created_at,
        external_message_id,
        filename,
        is_history_context,
        message,
        message_id,
        message_source,
        mime_type,
        original_message_type,
        parent_message_id,
        reason,
        size,
        status,
        cast(status_updated_at as {{ dbt.type_timestamp() }}) as status_updated_at,
        type,
        url

    from fields
    {# Exclude these types of chat events from downstream metrics #}
    where actor_id not in ('__trigger', '__system', 'agent:', '')
)

select *
from final
