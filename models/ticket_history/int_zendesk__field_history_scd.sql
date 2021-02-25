{{
    config(
        materialized='incremental',
        partition_by = {'field': 'valid_from', 'data_type': 'date'},
        unique_key='ticket_day_id'
        ) 
}}

{%- set ticket_columns = adapter.get_columns_in_relation(ref('int_zendesk__field_history_pivot')) -%}

with change_data as (

    select *
    from {{ ref('int_zendesk__field_history_pivot') }}
    {% if is_incremental() %}
    where date_day >= (select max(valid_from) from {{ this }})
    {% endif %}

), fill_values as (

    select 
        date_day as valid_from, 
        ticket_id,
        ticket_day_id

        {% for col in ticket_columns if col.name|lower not in ['date_day','ticket_id','ticket_day_id'] %} 

        ,last_value({{ col.name }} ignore nulls) over 
          (partition by ticket_id order by date_day asc rows between unbounded preceding and current row) as {{ col.name }}

        {% endfor %}

    from change_data

)

select *
from fill_values