{{ 
    config(
        materialized='incremental',
        partition_by = {'field': 'date_day', 'data_type': 'date', 'granularity': 'month' } if target.type not in ['spark', 'databricks', 'duckdb'] else ['date_day'],
        unique_key='ticket_day_id',
        incremental_strategy = 'merge' if target.type not in ('snowflake', 'postgres', 'redshift') else 'delete+insert',
        file_format='delta'
        ) 
}}

{#- Field columns carried from the scd; static mode derives them from vars instead of reading the scd. -#}
{%- set change_data_columns = get_ticket_field_history_value_columns(ref('int_zendesk__field_history_scd')) -%}

with change_data as (

    select *
    from {{ ref('int_zendesk__field_history_scd') }}
  
    {% if is_incremental() %}
    where valid_from >= (select max(date_day) from {{ this }})

-- If no issue fields have been updated since the last incremental run, the pivoted_daily_history CTE will return no record/rows.
-- When this is the case, we need to grab the most recent day's records from the previously built table so that we can persist 
-- those values into the future.

), most_recent_data as ( 

    select 
        *
    from {{ this }}
    where date_day = (select max(date_day) from {{ this }} )

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
        calendar.source_relation,
        calendar.date_day,
        calendar.ticket_id
        {% if is_incremental() %}    
            {% for col in change_data_columns if col|lower not in ['source_relation','ticket_id','valid_from','valid_to','ticket_day_id'] %} 
            , coalesce(change_data.{{ col }}, most_recent_data.{{ col }}) as {{ col }}
            {% endfor %}
        
        {% else %}
            {% for col in change_data_columns if col|lower not in ['source_relation','ticket_id','valid_from','valid_to','ticket_day_id'] %} 
            , {{ col }}
            {% endfor %}
        {% endif %}

    from calendar
    left join change_data
        on calendar.ticket_id = change_data.ticket_id
        and calendar.date_day = change_data.valid_from
        and calendar.source_relation = change_data.source_relation
    
    {% if is_incremental() %}
    left join most_recent_data
        on calendar.ticket_id = most_recent_data.ticket_id
        and calendar.date_day = most_recent_data.date_day
        and calendar.source_relation = most_recent_data.source_relation
    {% endif %}

), set_values as (

    select
        source_relation,
        date_day,
        ticket_id

        {% for col in change_data_columns if col|lower not in ['source_relation','ticket_id','valid_from','valid_to','ticket_day_id'] %}
        , {{ col }}
        -- create a batch/partition once a new value is provided
        , sum( case when {{ col }} is null then 0 else 1 end) over ( partition by ticket_id {{ fivetran_utils.partition_by_source_relation(package_name='zendesk') }}
            order by date_day rows unbounded preceding) as {{ col }}_field_partition

        {% endfor %}

    from joined
),

fill_values as (

    select  
        source_relation,
        date_day,
        ticket_id

        {% for col in change_data_columns if col|lower not in ['source_relation','ticket_id','valid_from','valid_to','ticket_day_id'] %}
        -- grab the value that started this batch/partition
        , first_value( {{ col }} ) over (
            partition by ticket_id, {{ col }}_field_partition {{ fivetran_utils.partition_by_source_relation(package_name='zendesk') }} 
            order by date_day asc rows between unbounded preceding and current row) as {{ col }}
        {% endfor %}

    from set_values

), fix_null_values as (

    select  
        source_relation, 
        date_day,
        ticket_id
        {% for col in change_data_columns if col|lower not in  ['source_relation','ticket_id','valid_from','valid_to','ticket_day_id'] %} 

        -- we de-nulled the true null values earlier in order to differentiate them from nulls that just needed to be backfilled
        , case when  cast( {{ col }} as {{ dbt.type_string() }} ) = 'is_null' then null else {{ col }} end as {{ col }}
        {% endfor %}

    from fill_values

), surrogate_key as (

    select
        {{ dbt_utils.generate_surrogate_key(['source_relation','date_day','ticket_id']) }} as ticket_day_id,
        *

    from fix_null_values
)

select *
from surrogate_key