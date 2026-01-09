--To disable this model, set the using_schedules variable within your dbt_project.yml file to False.
{{ config(enabled=var('using_schedules', True)) }}

with base as (

    select * 
    from {{ ref('stg_zendesk__schedule_tmp') }}

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
                source_columns=adapter.get_columns_in_relation(ref('stg_zendesk__schedule_tmp')),
                staging_columns=get_schedule_columns()
            )
        }}
        
            from base
),

final as (
    
    select 
        cast(null as {{ dbt.type_string() }}) as source_relation,
        cast(id as {{ dbt.type_string() }}) as schedule_id, --need to convert from numeric to string for downstream models to work properly
        end_time,
        start_time,
        name as schedule_name,
        created_at,
        time_zone
        
    from fields
    where not coalesce(_fivetran_deleted, false)
)

select * 
from final
