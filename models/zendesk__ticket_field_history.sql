{{ 
    config(
        materialized='incremental',
        partition_by = {'field': 'date_day', 'data_type': 'date'},
        unique_key='ticket_day_id'
        ) 
}}

{%- set change_data_columns = adapter.get_columns_in_relation(ref('int_zendesk__field_history_scd')) -%}

with change_data as (

    select *
    from {{ ref('int_zendesk__field_history_scd') }}
  
    {% if is_incremental() %}
    where valid_to >= (select max(date_day) from {{ this }})

-- If no issue fields have been updated since the last incremental run, the pivoted_daily_history CTE will return no record/rows.
-- When this is the case, we need to grab the most recent day's records from the previously built table so that we can persist 
-- those values into the future.

), most_recent_data as ( 

    select 
        *
    from {{ this }}
    where date_day > (select max(date_day) from {{ this }} )

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
        {% if is_incremental() %}    
            ,coalesce(change_data.valid_from, most_recent_data.valid_from) as valid_from
            ,coalesce(change_data.valid_to, most_recent_data.valid_to) as valid_to
            {% for col in change_data_columns if col.name|lower not in ['ticket_id','valid_from','valid_to','ticket_day_id'] %} 
            , coalesce(change_data.{{ col.name }}, most_recent_data.{{ col.name }}) as {{ col.name }}
            {% endfor %}
        
        {% else %}
            ,change_data.valid_from
            ,change_data.valid_to
            {% for col in change_data_columns if col.name|lower not in ['ticket_id','valid_from','valid_to','ticket_day_id'] %} 
            , {{ col.name }}
            {% endfor %}
        {% endif %}

    from calendar
    left join change_data
        on calendar.ticket_id = change_data.ticket_id
        and (calendar.date_day >= change_data.valid_from and calendar.date_day < change_data.valid_to)
    
    {% if is_incremental() %}
    left join most_recent_data
        on calendar.ticket_id = most_recent_data.ticket_id
        and (calendar.date_day >= most_recent_data.valid_from and calendar.date_day < most_recent_data.valid_to)
    {% endif %}

), fix_null_values as (

    select  
        date_day,
        ticket_id,
        valid_from,
        valid_to
        {% for col in change_data_columns if col.name|lower not in  ['ticket_id','valid_from','valid_to','ticket_day_id'] %} 

        -- we de-nulled the true null values earlier in order to differentiate them from nulls that just needed to be backfilled
        , case when  cast( {{ col.name }} as {{ dbt_utils.type_string() }} ) = 'is_null' then null else {{ col.name }} end as {{ col.name }}
        {% endfor %}

    from joined

), surrogate_key as (

    select
        {{ dbt_utils.surrogate_key(['date_day','ticket_id']) }} as ticket_day_id,
        *

    from fix_null_values
)

select *
from surrogate_key

