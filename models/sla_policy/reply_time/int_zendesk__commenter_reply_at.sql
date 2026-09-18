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

), flagged_comments as (

  select
    public_comments.*,
    -- True when this is the ticket's first public comment, it was posted before the customer
    -- said anything (public or private), and private comments already preceded it.
    (public_comments.previous_commenter_role = 'first_comment'
      and public_comments.previous_internal_comment_count > 0
      and (first_external_comment.first_external_comment_at is null
        or public_comments.valid_starting_at < first_external_comment.first_external_comment_at)
    ) as is_unengaged_first_comment
  from public_comments
  left join first_external_comment
    on first_external_comment.ticket_id = public_comments.ticket_id
    and first_external_comment.source_relation = public_comments.source_relation

), final as (

  select
    source_relation,
    ticket_id,
    valid_starting_at as reply_at,
    commenter_role as role
  from flagged_comments
  where commenter_role = 'internal_comment'
    -- Mirrors int_zendesk__ticket_reply_times.sql: an agent's own first public comment isn't a
    -- real reply if the customer hasn't engaged with the ticket yet.
    and not is_unengaged_first_comment

)

select *
from final