
{{ config(
    tags="fivetran_validations",
    enabled=var('fivetran_validation_tests_enabled', false)
) }}

with prod as (
    select
        {{ dbt_utils.star(from=ref('zendesk__ticket_enriched'), except=var('consistency_test_exclude_fields', '[]')) }}
    from {{ target.schema }}_zendesk_prod.zendesk__ticket_enriched
    where  true
    and {{ dbt.datediff(dbt.current_timestamp(), "updated_at", "minute") }} >= 60
),

dev as (
    select
        {{ dbt_utils.star(from=ref('zendesk__ticket_enriched'), except=var('consistency_test_exclude_fields', '[]')) }}
    from {{ target.schema }}_zendesk_dev.zendesk__ticket_enriched
    where true
    and {{ dbt.datediff(dbt.current_timestamp(), "updated_at", "minute") }} >= 60

    {# Make sure we're only comparing one schema since this current update (v0.19.0) added mult-schema support. Can remove for future releases #}
    {{ "and source_relation =  '" ~ (var("zendesk_database", target.database)|lower ~ "." ~ var("zendesk_schema", "zendesk")) ~ "'" if 'source_relation' in var("consistency_test_exclude_fields", '[]') }}
),

prod_not_in_dev as (
    -- rows from prod not found in dev
    select * from prod
    except distinct
    select * from dev
),

dev_not_in_prod as (
    -- rows from dev not found in prod
    select * from dev
    except distinct
    select * from prod
),

final as (
    select
        *,
        'from prod' as source
    from prod_not_in_dev

    union all -- union since we only care if rows are produced

    select
        *,
        'from dev' as source
    from dev_not_in_prod
)

select *
from final