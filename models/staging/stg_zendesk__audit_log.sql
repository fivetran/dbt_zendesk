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
                source_columns=zendesk_staging_source_columns(ref('stg_zendesk__audit_log_tmp'), get_audit_log_columns()),
                staging_columns=get_audit_log_columns()
            )
        }}

        {{ fivetran_utils.apply_source_relation(package_name='zendesk') }}
        
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