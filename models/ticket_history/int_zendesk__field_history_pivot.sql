{{ 
    config(
        materialized='incremental',
        partition_by = {'field': 'date_day', 'data_type': 'date'},
        unique_key='ticket_day_id'
        ) 
}}

{% if execute -%}
    {% set results = run_query('select distinct field_name from ' ~ var('field_history')) %}
    {% set results_list = results.columns[0].values() %}
{% endif -%}

with field_history as (

    select 
        ticket_id,
        field_name,
        user_id,
        valid_starting_at,
        valid_ending_at,
        -- doing this to figure out what values are actually null and what needs to be backfilled in zendesk__ticket_field_history
        case when value is null then 'is_null' else value end as value

    from {{ var('field_history') }}
    {% if is_incremental() %}
    where cast( {{ dbt_utils.date_trunc('day', 'valid_starting_at') }} as date) >= (select max(date_day) from {{ this }})
    {% endif %}

), event_order as (

    select 
        *,
        row_number() over (
            partition by cast(valid_starting_at as date), ticket_id, field_name
            order by valid_starting_at desc
            ) as row_num
    from field_history

), filtered as (

    -- Find the last event that occurs on each day for each ticket

    select *
    from event_order
    where row_num = 1

), pivot as (

    -- For each column that is in both the ticket_field_history_columns variable and the field_history table,
    -- pivot out the value into it's own column. This will feed the daily slowly changing dimension model.

    select 
        ticket_id,
        cast({{ dbt_utils.date_trunc('day', 'valid_starting_at') }} as date) as date_day

        {% for col in results_list if col in var('ticket_field_history_columns') %}
        {% set col_xf = col|lower %}
        {% set col_updater = (col|lower + "_updater_id") %}
        ,min(case when lower(field_name) = '{{ col|lower }}' then value end) as {{ col_xf }}
        ,min(case when lower(field_name) = '{{ col|lower }}' then user_id end) as {{ col_updater }}
        {% endfor %}
    
    from filtered
    group by 1,2

), surrogate_key as (

    select 
        *,
        {{ dbt_utils.surrogate_key(['ticket_id','date_day'])}} as ticket_day_id
    from pivot

)

select *
from surrogate_key