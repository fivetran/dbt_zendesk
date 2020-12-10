{{ 
    config(
        materialized='incremental',
        partition_by = {'field': 'valid_from', 'data_type': 'date'},
        unique_key='ticket_day_id'
        ) 
}}

{% if execute -%}
    {% set results = run_query('select distinct field_name from ' ~ var('field_history')) %}
    {% set results_list = results.columns[0].values() %}
{% endif -%}

with field_history as (

    select *
    from {{ var('field_history') }}
    {% if is_incremental() %}
    where cast({{ dbt_utils.dateadd('day', -1, 'valid_starting_at') }} as date) >= (select max(valid_from) from {{ this }})
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
        cast({{ dbt_utils.dateadd('day', 0, 'valid_starting_at') }} as date) as valid_from

        {% for col in results_list if col in var('ticket_field_history_columns') %}
        {% set col_xf = col|lower %}
        , min(case when lower(field_name) = '{{ col|lower }}' then value end) as {{ col_xf }}
        {% endfor %}
    
    from filtered
    where cast(valid_starting_at as date) < current_date
    group by 1,2

), surrogate_key as (

    select 
        *,
        {{ dbt_utils.surrogate_key(['ticket_id','valid_from'])}} as ticket_day_id
    from pivot

)

select *
from surrogate_key