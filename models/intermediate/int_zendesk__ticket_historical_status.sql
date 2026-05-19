with ticket_status_history as (

    select *
    from {{ ref('int_zendesk__updates') }}
    where field_name = 'status'

)

  select
    source_relation,
    ticket_id,
    valid_starting_at,
    valid_ending_at,
    {{ dbt.datediff(
        'valid_starting_at',
        "coalesce(valid_ending_at, " ~ dbt.current_timestamp() ~ ")",
        'minute') }} as status_duration_calendar_minutes,
    value as status

  from ticket_status_history