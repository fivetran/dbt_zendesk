{{ config(enabled=var('using_audit_log', False)) }}

with base as (

    select * 
    from {{ ref('stg_zendesk__audit_log_tmp') }}

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
                source_columns=adapter.get_columns_in_relation(ref('stg_zendesk__audit_log_tmp')),
                staging_columns=get_audit_log_columns()
            )
        }}

        {{ zendesk.apply_source_relation() }}
        
    from base
),

final as (
    select 
        cast(id as {{ dbt.type_string() }}) as audit_log_id,
        action,
        actor_id,
        change_description,
        cast(created_at as {{ dbt.type_timestamp() }}) as created_at,
        source_id,
        source_label,
        source_type,
        _fivetran_synced,
        source_relation

    from fields
)

select * 
from final