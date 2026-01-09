
with base as (

    select * 
    from {{ ref('stg_zendesk__ticket_field_history_tmp') }}

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
                source_columns=adapter.get_columns_in_relation(ref('stg_zendesk__ticket_field_history_tmp')),
                staging_columns=get_ticket_field_history_columns()
            )
        }}
        
            from base
),

final as (
    
    select 
        cast(null as {{ dbt.type_string() }}) as source_relation,
        ticket_id,
        field_name,
        cast(updated as {{ dbt.type_timestamp() }}) as valid_starting_at,
        cast(lead(updated) over (partition by ticket_id, field_name {{ partition_by_source_relation() }} order by updated) as {{ dbt.type_timestamp() }}) as valid_ending_at,
        value,
        user_id
        
    from fields
)

select * 
from final
