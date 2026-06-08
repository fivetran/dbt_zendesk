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
        
        {{ fivetran_utils.apply_source_relation(package_name='zendesk') }}

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

select *
from final
