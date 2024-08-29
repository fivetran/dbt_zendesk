
{{ config(
    tags="fivetran_validations",
    enabled=var('fivetran_validation_tests_enabled', false)
) }}

-- check that all the tickets are accounted for in the metrics
with stg_count as (
    select
        count(*) as stg_ticket_count
    from {{ ref('stg_zendesk__ticket') }}
),

metric_count as (
    select
        count(*) as metric_ticket_count
    from source
    from {{ ref('zendesk__ticket_metrics') }}
)

select *
from stg_count
join metric_count
    on stg_ticket_count != metric_ticket_count