-- model needs to materialize as a table to avoid erroneous null values
{{ config( materialized='table') }} 

{%- set ticket_columns = adapter.get_columns_in_relation(ref('int_zendesk__field_history_pivot')) -%}

with change_data as (

    select *
    from {{ ref('int_zendesk__field_history_pivot') }}

), set_values as (

-- each row of the pivoted table includes field values if that field was updated on that day
-- we need to backfill to persist values that have been previously updated and are still valid 
    select 
        date_day as valid_from, 
        ending_day as valid_to,
        ticket_id,
        ticket_day_id

        {% for col in ticket_columns if col.name|lower not in ['date_day','ending_day','ticket_id','ticket_day_id'] %} 

        ,{{ col.name }}
        ,sum(case when {{ col.name }} is null 
                then 0 
                else 1 
                    end) over (order by ticket_id, date_day rows unbounded preceding) as {{ col.name }}_field_patition
        {% endfor %}

    from change_data

), fill_values as (
    select
        valid_from, 
        valid_to,
        ticket_id,
        ticket_day_id

        {% for col in ticket_columns if col.name|lower not in ['date_day','ending_day','ticket_id','ticket_day_id'] %} 

        ,first_value( {{ col.name }} ) over (partition by {{ col.name }}_field_patition order by valid_from asc rows between unbounded preceding and current row) as {{ col.name }}
        
        {% endfor %}
    from set_values
) 

select *
from fill_values