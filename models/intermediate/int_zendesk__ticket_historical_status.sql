-- To do -- can we delete ticket_status_counter and unique_status_counter?

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
    value as status,
    -- MIGHT BE ABLE TO DELETE ROWS BELOW
    row_number() over (partition by ticket_id {{ partition_by_source_relation() }} order by valid_starting_at) as ticket_status_counter,
    row_number() over (partition by ticket_id, value {{ partition_by_source_relation() }} order by valid_starting_at) as unique_status_counter

  from ticket_status_history