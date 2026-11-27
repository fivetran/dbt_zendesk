{{ config(
    tags="fivetran_validations",
    enabled=var('fivetran_validation_tests_enabled', false)
) }}

-- Seed tickets 20006 and 20007 are agent-created, and each has an internal comment created in the same second as the first public comment.
-- A tied internal comment must not count as preceding the first public comment, whichever has the lower ticket_comment_id,
-- so first reply time runs to the next agent public comment: 30 minutes for 20006 and 60 minutes for 20007.
with expected as (

    select 20006 as ticket_id, 30 as expected_first_reply_time_calendar_minutes
    union all
    select 20007 as ticket_id, 60 as expected_first_reply_time_calendar_minutes

), actual as (

    select
        ticket_id,
        first_reply_time_calendar_minutes
    from {{ ref('zendesk__ticket_metrics') }}
    where ticket_id in (20006, 20007)

)

select
    expected.ticket_id,
    expected.expected_first_reply_time_calendar_minutes,
    actual.first_reply_time_calendar_minutes
from expected
left join actual
    on actual.ticket_id = expected.ticket_id
where actual.first_reply_time_calendar_minutes is null
    or round(actual.first_reply_time_calendar_minutes, 0) != expected.expected_first_reply_time_calendar_minutes
