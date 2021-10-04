-- depends_on: {{ ref('stg_zendesk__ticket') }}

with spine as (

    {% if execute %}
    {% set first_date_query %}
        select  min( created_at ) as min_date from {{ ref('stg_zendesk__ticket') }}
    {% endset %}
    {% set first_date = run_query(first_date_query).columns[0][0]|string %}
    
        {% if target.type == 'postgres' %}
            {% set first_date_adjust = "cast('" ~ first_date[0:10] ~ "' as date)" %}

        {% else %}
            {% set first_date_adjust = "'" ~ first_date[0:10] ~ "'" %}

        {% endif %}

    {% else %} {% set first_date_adjust = "2016-01-01" %}
    {% endif %}

    
{{
    dbt_utils.date_spine(
        datepart = "day", 
        start_date = first_date_adjust,
        end_date = dbt_utils.dateadd("week", 1, "current_date")
    )   
}}

), recast as (

    select cast(date_day as date) as date_day
    from spine

    -- by default take all the data 
    where date_day >= {{ dbt_utils.dateadd('year', - var('ticket_field_history_timeframe_years', 50), dbt_utils.current_timestamp() ) }}

)

select *
from recast