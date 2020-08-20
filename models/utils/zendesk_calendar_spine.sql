-- depends_on: {{ ref('stg_zendesk_ticket') }}

with spine as (

    {% if execute %}
    {% set first_date_query %}
        select  min( created_at ) as min_date from {{ ref('stg_zendesk_ticket') }}
    {% endset %}
    {% set first_date = run_query(first_date_query).columns[0][0]|string %}
    
    {% else %} {% set first_date = "'2016-01-01'" %}
    {% endif %}

{{
    dbt_utils.date_spine(
        datepart = "day", 
        start_date =  "'" ~ first_date[0:10] ~ "'", 
        end_date = dbt_utils.dateadd("week", 1, "current_date")
    )   
}}

), recast as (

    select cast(date_day as date) as date_day
    from spine

)

select *
from recast