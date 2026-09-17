with comments_enriched as (

  select *
  from {{ ref('int_zendesk__comments_enriched') }}

-- The customer's first comment, public or private, so we can tell if they've engaged at all.
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
    -- Don't count an agent's first public comment as a reply if the ticket was created via
    -- private comments and the customer hasn't engaged yet (mirrors int_zendesk__ticket_reply_times.sql).
    and not (
      public_comments.previous_commenter_role = 'first_comment'
      and public_comments.previous_internal_comment_count > 0
      and (first_external_comment.first_external_comment_at is null
        or public_comments.valid_starting_at < first_external_comment.first_external_comment_at)
    )

)

select *
from final