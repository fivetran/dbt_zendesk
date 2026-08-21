--To disable this model, set the using_ticket_custom_field variable within your dbt_project.yml file to False.
{{ config(enabled=var('using_ticket_custom_field', True)) }}

with base as (

    select *
    from {{ ref('stg_zendesk__ticket_custom_field_tmp') }}

),

fields as (

    select
        {{
            fivetran_utils.fill_staging_columns(
                source_columns=adapter.get_columns_in_relation(ref('stg_zendesk__ticket_custom_field_tmp')),
                staging_columns=get_ticket_custom_field_columns()
            )
        }}

        {{ fivetran_utils.apply_source_relation(package_name='zendesk') }}

    from base
),

final as (

    -- Intentionally not filtering out _fivetran_deleted records here. Deleted custom fields must still resolve to
    -- their title so historical ticket_field_history values remain readable after the field is removed in Zendesk.
    select
        cast(id as {{ dbt.type_string() }}) as ticket_custom_field_id,
        title,
        raw_title,
        key,
        type,
        active,
        source_relation

    from fields
)

select *
from final
