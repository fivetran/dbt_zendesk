with comments_enriched as (

  select *
  from {{ ref('int_zendesk__comments_enriched') }}

-- The customer's first comment (public or private) on the ticket. A private comment still
-- means the customer has engaged, so it's not enough to check for any prior private comment;
-- we need to know specifically whether the customer has said anything yet.
), first_external_comment as (

  select
    source_relation,
    ticket_id,
    min(valid_starting_at) as first_external_comment_at
  from comments_enriched
  where commenter_role = 'external_comment'
  {{ dbt_utils.group_by(n=2) }}

), public_comments as (

  select *
  from comments_enriched
  where is_public

), final as (

  select
    public_comments.source_relation,
    public_comments.ticket_id,
    public_comments.valid_starting_at as reply_at,
    public_comments.commenter_role as role
  from public_comments
  left join first_external_comment
    on first_external_comment.ticket_id = public_comments.ticket_id
    and first_external_comment.source_relation = public_comments.source_relation
  where public_comments.commenter_role = 'internal_comment'
    -- Exclude an agent's public comment from counting as a reply when it is the ticket's
    -- first public comment, the ticket already had private comments before it (i.e. this
    -- wasn't simply the ticket's first-ever activity, such as an agent proactively opening a
    -- ticket), and the customer hasn't said anything yet (publicly or privately). Zendesk
    -- doesn't start measuring first-reply-time until the customer's first public comment.
    -- Mirrors the handling in int_zendesk__ticket_reply_times.sql.
    and not (
      public_comments.previous_commenter_role = 'first_comment'
      and public_comments.previous_internal_comment_count > 0
      and (first_external_comment.first_external_comment_at is null
        or public_comments.valid_starting_at < first_external_comment.first_external_comment_at)
    )

)

select *
from final