{{ 
    config(
        materialized='incremental',
        partition_by = {'field': 'date_day', 'data_type': 'date'},
        unique_key='ticket_day_id'
        ) 
}}

{%- set change_data_columns = adapter.get_columns_in_relation(ref('int_zendesk__field_history_pivot')) -%}

with change_data as (

    select *
    from {{ ref('int_zendesk__field_history_pivot') }}
    {% if is_incremental() %}
    where valid_from >= (select max(date_day) from {{ this }})
    {% endif %}

), calendar as (

    select *
    from {{ ref('int_zendesk__field_calendar_spine') }}
    where date_day <= current_date
    {% if is_incremental() %}
    and date_day >= (select max(date_day) from {{ this }})
    {% endif %}

), joined as (

    select 
        calendar.date_day,
        calendar.ticket_id
        {% for col in change_data_columns if col.name|lower not in ['ticket_id','valid_from','ticket_day_id'] %} 
        , {{ col.name }}
        {% endfor %}
    from calendar
    left join change_data
        on calendar.ticket_id = change_data.ticket_id
        and calendar.date_day = change_data.valid_from

), fill_values as (

    select
        date_day,
        ticket_id    
        -- For each ticket on each day, find the state of each column from the last record where a change occurred,
        -- identified by the presence of a record from the SCD table on that day
        {% for col in change_data_columns if col.name|lower not in  ['ticket_id','valid_from','ticket_day_id'] %} 
        
        ,last_value({{ col.name }} ignore nulls) over 
          (partition by ticket_id order by date_day asc rows between unbounded preceding and current row) as {{ col.name }}

        {% endfor %}

    from joined

), surrogate_key as (

    select 
        *,
        {{ dbt_utils.surrogate_key(['date_day','ticket_id'] )}} as ticket_day_id
    from fill_values

)

select *
from surrogate_key