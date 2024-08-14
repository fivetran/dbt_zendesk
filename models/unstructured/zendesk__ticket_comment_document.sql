with ticket_comments as (
    select *
    from {{ var('ticket_comment') }}

), users as (
    select *
    from {{ var('user') }}

), comment_details as (
    select 
        ticket_comments.ticket_comment_id,
        ticket_comments.ticket_id,
        {{ zendesk.coalesce_cast(["users.email", "'UNKNOWN'"], dbt.type_string()) }} as commenter_email,
        {{ zendesk.coalesce_cast(["users.name", "'UNKNOWN'"], dbt.type_string()) }} as commenter_name,
        ticket_comments.created_at as comment_time,
        ticket_comments.body as comment_body
    from ticket_comments
    left join users
        on ticket_comments.user_id = users.user_id
    where not coalesce(ticket_comments._fivetran_deleted, False)
        and not coalesce(users._fivetran_deleted, False)

), comment_markdowns as (
    select
        ticket_comment_id,
        ticket_id,
        comment_time,
        cast(
            {{ dbt.concat([
                "'### message from '", "commenter_name", "' ('", "commenter_email", "')\\n'",
                "'##### sent @ '", "comment_time", "'\\n'",
                "comment_body"
            ]) }} as {{ dbt.type_string() }})
            as comment_markdown
    from comment_details

), comments_tokens as (
    select
        *,
        {{ zendesk.count_tokens("comment_markdown") }} as comment_tokens
    from comment_markdowns

), truncated_comments as (
    select
        ticket_comment_id,
        ticket_id,
        comment_time,
        case when comment_tokens > {{ var('max_tokens', 7500) }} then substring(comment_markdown, 1, {{ var('max_tokens', 7500) }} * 4)  -- approximate 4 characters per token
            else comment_markdown
            end as comment_markdown,
        case when comment_tokens > {{ var('max_tokens', 7500) }} then {{ var('max_tokens', 7500) }}
            else comment_tokens
            end as comment_tokens
    from comments_tokens
)

select *
from truncated_comments