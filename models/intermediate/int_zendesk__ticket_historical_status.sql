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
    row_number() over (partition by ticket_id {{ partition_by_source_relation() }} order by valid_starting_at) as ticket_status_counter, -- Deprecated as of March, 2026. Will be removed in future release.
    row_number() over (partition by ticket_id, value {{ partition_by_source_relation() }} order by valid_starting_at) as unique_status_counter -- Deprecated as of March, 2026. Will be removed in future release.

  from ticket_status_history