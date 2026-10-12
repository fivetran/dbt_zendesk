-- model needs to materialize as a table to avoid erroneous null values
{{ config( materialized='table') }} 

{# Pivoted field columns to backfill; static mode derives them from vars instead of reading the pivot. #}
{% set ticket_columns = get_ticket_field_history_value_columns(ref('int_zendesk__field_history_pivot')) %}

with change_data as (

    select *
    from {{ ref('int_zendesk__field_history_pivot') }}

), set_values as (

-- each row of the pivoted table includes field values if that field was updated on that day
-- we need to backfill to persist values that have been previously updated and are still valid 
    select 
        source_relation,
        date_day as valid_from,
        ticket_id,
        ticket_day_id

        {% for col in ticket_columns if col|lower not in ['source_relation','date_day','ending_day','ticket_id','ticket_day_id'] %} 

        ,{{ col }}
        ,sum(case when {{ col }} is null 
                then 0 
                else 1 
                    end) over ({{ fivetran_utils.partition_by_source_relation(package_name='zendesk', has_other_partitions='no') }} order by ticket_id, date_day rows unbounded preceding) as {{ col }}_field_partition
        {% endfor %}

    from change_data

), fill_values as (
    select
        source_relation,
        valid_from, 
        ticket_id,
        ticket_day_id

        {% for col in ticket_columns if col|lower not in ['source_relation','date_day','ending_day','ticket_id','ticket_day_id'] %} 

        ,first_value( {{ col }} ) over (partition by {{ col }}_field_partition, ticket_id {{ fivetran_utils.partition_by_source_relation(package_name='zendesk') }} order by valid_from asc rows between unbounded preceding and current row) as {{ col }}
        
        {% endfor %}
    from set_values
) 

select *
from fill_values