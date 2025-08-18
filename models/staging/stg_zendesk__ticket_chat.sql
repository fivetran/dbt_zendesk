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
        visitor_id

    from fields
)

select *
from final
