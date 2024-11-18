
{{ config(
    tags="fivetran_validations",
    enabled=var('fivetran_validation_tests_enabled', false)
) }}

-- check that all the tickets are accounted for in the metrics
with stg_count as (
    select
        source_relation,
        count(*) as stg_ticket_count
    from {{ ref('stg_zendesk__ticket') }}
    group by 1
),

metric_count as (
    select
        source_relation,
        count(*) as metric_ticket_count
    from {{ ref('zendesk__ticket_metrics') }}
    group by 1
)

select
    stg_count.source_relation as stg_source_relation,
    metric_count.source_relation as model_source_relation,
    stg_ticket_count,
    metric_ticket_count
from stg_count
full join metric_count
    using(source_relation)
where coalesce(stg_ticket_count, -1) != coalesce(metric_ticket_count, -2)