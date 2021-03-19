-- model needs to materialize as a table to avoid erroneous null values
{{ config( materialized='table') }} 

{%- set ticket_columns = adapter.get_columns_in_relation(ref('int_zendesk__field_history_pivot')) -%}

with change_data as (

    select *
    from {{ ref('int_zendesk__field_history_pivot') }}

), fill_values as (

-- each row of the pivoted table includes field values if that field was updated on that day
-- we need to backfill to persist values that have been previously updated and are still valid 
    select 
        date_day as valid_from, 
        ticket_id,
        ticket_day_id

        {% for col in ticket_columns if col.name|lower not in ['date_day','ticket_id','ticket_day_id'] %} 
            {% if target.type == 'postgres' %}
                ,last_value( {{ col.name }} ) over
                    (partition by ticket_id order by case when date_day is not null then 0 else 1 end asc, date_day rows between unbounded preceding and current row) as {{ col.name }} 

            {% else %}
                ,last_value({{ col.name }} ignore nulls) over 
                    (partition by ticket_id order by date_day asc rows between unbounded preceding and current row) as {{ col.name }}

            {% endif %}
        {% endfor %}

    from change_data

)

select *
from fill_values