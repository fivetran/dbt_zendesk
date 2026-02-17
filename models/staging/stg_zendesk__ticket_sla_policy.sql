--To disable this model, set the using_ticket_sla_policy variable within your dbt_project.yml file to False.
{{ config(enabled=var('using_ticket_sla_policy', True)) }}

with base as (

    select *
    from {{ ref('stg_zendesk__ticket_sla_policy_tmp') }}

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
                source_columns=adapter.get_columns_in_relation(ref('stg_zendesk__ticket_sla_policy_tmp')),
                staging_columns=get_ticket_sla_policy_columns()
            )
        }}

        {{ zendesk.apply_source_relation() }}

    from base
),

final as (

    select
        source_relation,
        ticket_id,
        sla_policy_id,
        cast(policy_applied_at as {{ dbt.type_timestamp() }}) as policy_applied_at,
        cast(_fivetran_synced as {{ dbt.type_timestamp() }}) as _fivetran_synced

    from fields
)

select *
from final
