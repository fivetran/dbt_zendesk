
with base as (

    select * 
    from {{ ref('stg_zendesk__user_tmp') }}

),

fields as (

    select
        /*
        The below macro is used to generate the correct SQL for package staging models. It takes a list of columns 
        that are expected/needed (staging_columns from dbt_zendesk/models/tmp/) and compares it with columns 
        in the source (source_columns from dbt_zendesk/macros/).
        For more information refer to our dbt_fivetran_utils documentation (https://github.com/fivetran/dbt_fivetran_utils.git).
        */
        {{
            fivetran_utils.fill_staging_columns(
                source_columns=adapter.get_columns_in_relation(ref('stg_zendesk__user_tmp')),
                staging_columns=get_user_columns()
            )
        }}
        
            from base
),

final as ( 
    
    select 
        cast(null as {{ dbt.type_string() }}) as source_relation,
        id as user_id,
        external_id,
        _fivetran_synced,
        _fivetran_deleted,
        cast(last_login_at as {{ dbt.type_timestamp() }}) as last_login_at,
        cast(created_at as {{ dbt.type_timestamp() }}) as created_at,
        cast(updated_at as {{ dbt.type_timestamp() }}) as updated_at,
        email,
        name,
        organization_id,
        phone,
        {% if var('internal_user_criteria', false) -%}
            case 
                when role in ('admin', 'agent') then role
                when {{ var('internal_user_criteria', false) }} then 'agent'
            else role end as role,
        {% else -%}
        role,
        {% endif -%}
        ticket_restriction,
        time_zone,
        locale,
        active as is_active,
        suspended as is_suspended

        {{ fivetran_utils.fill_pass_through_columns('zendesk__user_passthrough_columns') }}
        
    from fields
)

select * 
from final
