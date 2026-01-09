--To disable this model, set the using_schedules variable within your dbt_project.yml file to False.
{{ config(enabled=var('using_schedules', True)) }}

with base as (

    select * 
    from {{ ref('stg_zendesk__time_zone_tmp') }}

),

fields as (

    select
        {{
            fivetran_utils.fill_staging_columns(
                source_columns=adapter.get_columns_in_relation(ref('stg_zendesk__time_zone_tmp')),
                staging_columns=get_time_zone_columns()
            )
        }}
        
            from base
),

final as (
    
    select 
        cast(null as {{ dbt.type_string() }}) as source_relation,
        standard_offset,
        time_zone,
        -- the standard_offset is a string written as [+/-]HH:MM
        -- let's convert it to an integer value of minutes
        cast( {{ dbt.split_part(string_text='standard_offset', delimiter_text="':'", part_number=1) }} as {{ dbt.type_int() }} ) * 60 +
            (cast( {{ dbt.split_part(string_text='standard_offset', delimiter_text="':'", part_number=2) }} as {{ dbt.type_int() }} ) *
                (case when standard_offset like '-%' then -1 else 1 end) ) as standard_offset_minutes
    
    from fields
)

select * 
from final
