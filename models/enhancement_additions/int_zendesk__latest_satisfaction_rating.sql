with satisfaction_rating as (
  select *
  from {{ ref('stg_zendesk__satisfaction_rating') }}
),

latest_satisfaction_rating as (
    select
      *,
      row_number() over(partition by ticket_id order by updated_at desc) as latest_satisfaction_index
    from satisfaction_rating
),

final as (
    select 
        satifaction_rating_id,
        ticket_id,
        requester_id,
        assignee_id,
        group_id,
        score,
        created_at,
        updated_at,
        comment,
        reason
    from latest_satisfaction_rating

    where latest_satisfaction_index = 1
)

select *
from final