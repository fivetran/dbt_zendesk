--To disable this model, set the using_ticket_form_history variable within your dbt_project.yml file to False.
{{ config(enabled=var('using_ticket_form_history', True)) }}

with ticket_form_history as (
  select *
  from {{ ref('stg_zendesk__ticket_form_history') }}
),

latest_ticket_form as (
    select
      *,
      row_number() over(partition by ticket_form_id {{ partition_by_source_relation() }} order by updated_at desc) as latest_form_index
    from ticket_form_history
),

final as (
    select 
        source_relation,
        ticket_form_id,
        created_at,
        updated_at,
        display_name,
        is_active,
        name,
        latest_form_index
    from latest_ticket_form

    where latest_form_index = 1
)

select *
from final