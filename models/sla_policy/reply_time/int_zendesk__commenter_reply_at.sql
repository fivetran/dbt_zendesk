with comments_enriched as (

  select *
  from {{ ref('int_zendesk__comments_enriched') }}

), public_comments as (

  select *
  from comments_enriched
  where is_public

), final as (

  select
    source_relation,
    ticket_id,
    valid_starting_at as reply_at,
    commenter_role as role
  from public_comments
  where commenter_role = 'internal_comment'

)

select *
from final
