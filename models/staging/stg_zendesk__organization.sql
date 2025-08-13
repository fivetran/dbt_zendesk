{{ config(enabled=var('using_organizations', True)) }}

with base as (

    select * 
    from {{ ref('stg_zendesk__organization_tmp') }}

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
                source_columns=adapter.get_columns_in_relation(ref('stg_zendesk__organization_tmp')),
                staging_columns=get_organization_columns()
            )
        }}
        
        {{ zendesk.apply_source_relation() }}

    from base
),

final as (
    
    select 
        id as organization_id,
        created_at,
        updated_at,
        details,
        name,
        external_id,
        source_relation

        {{ fivetran_utils.fill_pass_through_columns('zendesk__organization_passthrough_columns') }}

    from fields
)

select * 
from final
