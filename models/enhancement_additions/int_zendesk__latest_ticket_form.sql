with ticket_form_history as (
  select *
  from {{ ref('stg_zendesk__ticket_form_history') }}
),

latest_ticket_form as (
    select
      *,
      row_number() over(partition by ticket_form_id order by updated_at desc) as latest_form_index
    from ticket_form_history
),

final as (
    select 
        ticket_form_id,
        created_at,
        updated_at,
        display_name,
        active,
        name
    from latest_ticket_form

    where latest_form_index = 1
)

select *
from final