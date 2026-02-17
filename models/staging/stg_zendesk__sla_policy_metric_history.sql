--To disable this model, set the using_sla_policy_metric_history variable within your dbt_project.yml file to False.
{{ config(enabled=var('using_sla_policy_metric_history', True)) }}

with base as (

    select *
    from {{ ref('stg_zendesk__sla_policy_metric_history_tmp') }}

),

fields as (

    select
        /*
        The below macro is used to generate the correct SQL for package staging models. It takes a list of columns
        that are expected/needed (staging_columns from dbt_zendesk/models/tmp/) and compares it with columns
        in the source (source_columns from dbt_zendesk/macros/).
        For more information refer to our dbt_fivetran_utils documentation (https://github.com/fivetran/dbt_fivetran_utils.git).
        */
        {{
            fivetran_utils.fill_staging_columns(
                source_columns=adapter.get_columns_in_relation(ref('stg_zendesk__sla_policy_metric_history_tmp')),
                staging_columns=get_sla_policy_metric_history_columns()
            )
        }}

        {{ zendesk.apply_source_relation() }}

    from base
),

final as (

    select
        source_relation,
        sla_policy_id,
        cast(index as {{ dbt.type_int() }}) as index,
        cast(sla_policy_updated_at as {{ dbt.type_timestamp() }}) as sla_policy_updated_at,
        business_hours as in_business_hours,
        lower(cast(metric as {{ dbt.type_string() }})) as metric,
        lower(cast(priority as {{ dbt.type_string() }})) as priority,
        target,
        cast(_fivetran_synced as {{ dbt.type_timestamp() }}) as _fivetran_synced

    from fields
)

select 
    *,
    row_number() over (partition by sla_policy_id, metric, priority {{ partition_by_source_relation() }} order by sla_policy_updated_at desc) = 1 as is_most_recent_record
from final
