with ticket_comments as (

    select *
    from {{ ref('int_zendesk__comments_enriched') }}
),

comment_counts as (
    select
        source_relation,
        ticket_id,
        last_comment_added_at,
        sum(case when commenter_role = 'internal_comment' and is_public = true
            then 1
            else 0
                end) as count_public_agent_comments,
        sum(case when commenter_role = 'internal_comment'
            then 1
            else 0
                end) as count_agent_comments,
        sum(case when commenter_role = 'external_comment'
            then 1
            else 0
                end) as count_end_user_comments,
        sum(case when is_public = true
            then 1
            else 0
                end) as count_public_comments,
        sum(case when is_public = false
            then 1
            else 0
                end) as count_internal_comments,
        count(*) as total_comments,
        count(distinct case when commenter_role = 'internal_comment'
            then user_id
                end) as count_ticket_handoffs,
        sum(case when commenter_role = 'internal_comment' and is_public = true and previous_commenter_role != 'first_comment'
            then 1
            else 0
                end) as count_agent_replies
    from ticket_comments
    where not is_chat_comment

    group by 1, 2, 3
),

{# 
    Multiple agent messages that an agent sends in one active conversation session count as a single Agent reply. 
    For example, if an agent sends three messages in the same active conversation and then the conversation ends, that exchange counts as one Agent reply.
#}
chat_comment_counts as (
    select
        source_relation,
        ticket_id,
        last_comment_added_at,
        count(distinct case when commenter_role = 'internal_comment' and is_public = true
            then chat_id
            else null
                end) as count_public_agent_comments,
        count(distinct case when commenter_role = 'internal_comment'
            then chat_id
            else null
                end) as count_agent_comments,
        count(distinct case when commenter_role = 'external_comment'
            then chat_id
            else null
                end) as count_end_user_comments,
        count(distinct case when is_public = true
            then chat_id
            else null
                end) as count_public_comments,
        count(distinct case when is_public = false
            then chat_id
            else null
                end) as count_internal_comments,
        count(distinct chat_id) as total_comments,
        count(distinct case when commenter_role = 'internal_comment'
            then user_id
                end) as count_ticket_handoffs,
        count(distinct case when commenter_role = 'internal_comment' and is_public = true and previous_commenter_role != 'first_comment'
            then chat_id
            else null
                end) as count_agent_replies
    from ticket_comments
    where is_chat_comment

    group by 1, 2, 3
),

comment_count_union as (
    select * from comment_counts

    union all

    select * from chat_comment_counts
),

{# Combine public comments and messaging chats #}
consolidate_comment_counts as (
    select
        source_relation,
        ticket_id,
        max(last_comment_added_at) as last_comment_added_at,
        sum(count_public_agent_comments) as count_public_agent_comments,
        sum(count_agent_comments) as count_agent_comments,
        sum(count_end_user_comments) as count_end_user_comments,
        sum(count_public_comments) as count_public_comments,
        sum(count_internal_comments) as count_internal_comments,
        sum(total_comments) as total_comments,
        sum(count_ticket_handoffs) as count_ticket_handoffs,
        sum(count_agent_replies) as count_agent_replies
    from comment_count_union

    group by 1, 2
),

final as (
    select
        *,
        count_public_agent_comments = 1 as is_one_touch_resolution,
        count_public_agent_comments = 2 as is_two_touch_resolution
    from consolidate_comment_counts
)

select * 
from final
