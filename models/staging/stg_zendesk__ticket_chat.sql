{{ config(enabled=var('using_ticket_chat', False)) }}

with base as (

    select * 
    from {{ ref('stg_zendesk__ticket_chat_tmp') }}
),

fields as (

    select
        {{
            fivetran_utils.fill_staging_columns(
                source_columns=adapter.get_columns_in_relation(ref('stg_zendesk__ticket_chat_tmp')),
                staging_columns=get_ticket_chat_columns()
            )
        }}
        
        {{ zendesk.apply_source_relation() }}

    from base
),

final as (
    
    select 
        source_relation, 
        _fivetran_synced,
        authenticated as is_authenticated,
        backend,
        channel,
        chat_id,
        conversation_id,
        integration_id,
        ticket_id,
        user_id,
        visitor_id,
        cast(initiator as {{ dbt.type_string() }}) as initiator

    from fields
)

select 
    *,
    case 
        when initiator = '1' then 'agent'
        when initiator = '2' then 'end-user'
        when initiator = '5' then 'system'
        else 'unknown' 
    end as initiator_type
from final
