
with base as (

    select * 
    from {{ ref('stg_zendesk__group_tmp') }}

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
                source_columns=adapter.get_columns_in_relation(ref('stg_zendesk__group_tmp')),
                staging_columns=get_group_columns()
            )
        }}

        {{ zendesk.apply_source_relation() }}

    from base
),

final as (
    
    select 
        id as group_id,
        name,
        source_relation

    from fields
    where not coalesce(_fivetran_deleted, false)
)

select * 
from final