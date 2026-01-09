--To disable this model, set the using_ticket_form_history variable within your dbt_project.yml file to False.
{{ config(enabled=var('using_ticket_form_history', True)) }}

with base as (

    select * 
    from {{ ref('stg_zendesk__ticket_form_history_tmp') }}

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
                source_columns=adapter.get_columns_in_relation(ref('stg_zendesk__ticket_form_history_tmp')),
                staging_columns=get_ticket_form_history_columns()
            )
        }}

            from base
),

final as (
    
    select 
        cast(null as {{ dbt.type_string() }}) as source_relation,
        id as ticket_form_id,
        cast(created_at as {{ dbt.type_timestamp() }}) as created_at,
        cast(updated_at as {{ dbt.type_timestamp() }}) as updated_at,
        display_name,
        active as is_active,
        name
        
    from fields
    where not coalesce(_fivetran_deleted, false)
    
)

select * 
from final
