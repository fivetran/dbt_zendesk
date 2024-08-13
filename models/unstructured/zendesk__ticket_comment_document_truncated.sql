{# {{ config(
    pre_hook= "{{ print_truncated_records_count() }}"
) }} #}

with truncated_comments as (
    select
        ticket_comment_id,
        ticket_id,
        comment_time,
        case
            when comment_tokens > 7500 then
                substring(comment_markdown, 1, 7500 * 4)  -- approximate 4 characters per token
            else
                comment_markdown
        end as comment_markdown
    from {{ ref('zendesk__ticket_comment_document') }}

)

select
    *,
    {{ zendesk.count_tokens("comment_markdown") }} as comment_tokens
from truncated_comments