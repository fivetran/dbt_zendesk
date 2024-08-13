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
        coalesce(users.email, 'UNKNOWN') as commenter_email,
        coalesce(users.name, 'UNKNOWN')as commenter_name,
        ticket_comments.created_at as comment_time,
        ticket_comments.body as comment_body
    from ticket_comments
    left join users
        on ticket_comments.user_id = users.user_id
    {# where ticket_comments._fivetran_deleted = false
        and users._fivetran_deleted = false #}

), final as (
    select
        ticket_comment_id,
        ticket_id,
        comment_time,
        {{ dbt.concat([
            "'### message from '", "commenter_name", "' ('", "commenter_email", "')\\n'",
            "'##### sent @ '", "comment_time", "'\\n'",
            "comment_body"
        ]) }} as comment_markdown
    from comment_details
)

select
    *,
    {{ zendesk.count_tokens("comment_markdown") }} as comment_tokens
from final