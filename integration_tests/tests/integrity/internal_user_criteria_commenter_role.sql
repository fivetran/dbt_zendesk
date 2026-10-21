{{ config(
    tags="fivetran_validations",
    enabled=(
        var('fivetran_validation_tests_enabled', false)
        and var('internal_user_criteria', false)
        and var('using_user_role_histories', True)
        and var('using_audit_log', False)
    )
) }}

-- Comments from users that internal_user_criteria marks as internal must be labeled internal_comment,
-- even when their Zendesk support role at the time of the comment was 'not set' or 'end-user'.
with comments as (
    select
        source_relation,
        ticket_id,
        user_id,
        valid_starting_at,
        commenter_role
    from {{ ref('int_zendesk__comments_enriched') }}

), user_roles as (
    select
        source_relation,
        user_id,
        role,
        is_internal_role,
        valid_starting_at,
        valid_ending_at
    from {{ ref('int_zendesk__user_role_history') }}

)

select
    comments.source_relation,
    comments.ticket_id,
    comments.user_id,
    comments.valid_starting_at,
    user_roles.role,
    comments.commenter_role
from comments
join user_roles
    on comments.user_id = user_roles.user_id
    and comments.source_relation = user_roles.source_relation
    and comments.valid_starting_at >= user_roles.valid_starting_at
    and comments.valid_starting_at < user_roles.valid_ending_at
where user_roles.is_internal_role
    and comments.commenter_role != 'internal_comment'
