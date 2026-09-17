with comments_enriched as (

  select *
  from {{ ref('int_zendesk__comments_enriched') }}
  where is_public

), final as (

  select
    source_relation,
    ticket_id,
    valid_starting_at as reply_at,
    commenter_role as role
  from comments_enriched
  where commenter_role = 'internal_comment'
    -- Exclude an agent's public comment from counting as a reply when it is the ticket's
    -- first public comment and the ticket was created via a private/internal comment, since
    -- Zendesk doesn't start measuring first-reply-time until the customer's first public
    -- comment. Mirrors the handling in int_zendesk__ticket_reply_times.sql.
    and not (previous_commenter_role = 'first_comment' and previous_internal_comment_count > 0)

)

select *
from final