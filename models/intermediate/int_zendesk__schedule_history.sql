with audit_logs as (
    select *
    from {{ var('audit_log') }}   

), audit_logs_cleaned as (
    select 
        _fivetran_synced,
        created_at,
        source_id as schedule_id,
        change_description as original,
        replace(
            {{ clean_string('lower(change_description)', 
                ['workweek changed from', 'amp', 'gt', 'quot', ';', '&', '=', ' ']) }},
            '""', '":"')
        as change_description

    from audit_logs
    where lower(change_description) like '%workweek changed from%'

), split_to_from as (
    select
        _fivetran_synced,
        schedule_id,
        created_at,
        created_at as valid_from,
        min(created_at) over (partition by schedule_id) as min_valid_from,
        coalesce(
            lead(created_at) over (
                partition by schedule_id order by created_at), 
            {{ dbt.current_timestamp_backcompat() }})
            as valid_to,
        {{ dbt.split_part('change_description', "'to'", 1) }} as from_schedule,
        {{ dbt.split_part('change_description', "'to'", 2) }} as to_schedule
    from audit_logs_cleaned

), split_days as (
{% set days_of_week = {'sun': 0, 'mon': 1, 'tue': 2, 'wed': 3, 'thu': 4, 'fri': 5, 'sat': 6} %}
    {% for day, day_number in days_of_week.items() %}
        select
            split_to_from.*,
            '{{ day }}' as day_of_week,
            cast('{{ day_number }}' as {{ dbt.type_int() }}) as day_of_week_number,
            replace({{ zendesk.regex_extract('from_schedule', "'.*?" ~ day ~ ".*?({.*?})'") }}, ',', '},{') as from_schedule_cleaned,
            replace({{ zendesk.regex_extract('to_schedule', "'.*?" ~ day ~ ".*?({.*?})'") }}, ',', '},{') as to_schedule_cleaned
        from split_to_from
    
    {% if not loop.last %}union all{% endif %}
    {% endfor %}

), schedule_arrays as (
    select
        split_days.*,
        {{ zendesk.to_json_array(dbt.concat(['"["', 'from_schedule_cleaned', '"]"'])) }} as from_schedule_array,
        {{ zendesk.to_json_array(dbt.concat(['"["', 'to_schedule_cleaned', '"]"'])) }} as to_schedule_array
    from split_days

), unnest_schedules as (
    -- only want the first "from_schedule" to start off
    select 
        schedule_arrays.*,
        {{ clean_string('unnested_from_schedule', ['{', '}', '"']) }} as unnested_schedule,
        'from' as schedule_source
    from schedule_arrays
    cross join unnest(from_schedule_array) as unnested_from_schedule
    where valid_from = min_valid_from

    union all

    select 
        schedule_arrays.*,
        {{ clean_string('unnested_to_schedule', ['{', '}', '"']) }} as unnested_schedule,
        'to' as schedule_source
    from schedule_arrays
    cross join unnest(to_schedule_array) as unnested_to_schedule
    where valid_from != min_valid_from

), split_times as (

    select 
        schedule_id,
        cast(nullif({{ dbt.split_part('unnested_schedule', "':'", 1) }}, ' ') as {{ dbt.type_int() }}) as start_time_hh, 
        cast(nullif({{ dbt.split_part('unnested_schedule', "':'", 2) }}, ' ') as {{ dbt.type_int() }}) as start_time_mm, 
        cast(nullif({{ dbt.split_part('unnested_schedule', "':'", 3) }}, ' ') as {{ dbt.type_int() }}) as end_time_hh, 
        cast(nullif({{ dbt.split_part('unnested_schedule', "':'", 4) }}, ' ') as {{ dbt.type_int() }}) as end_time_mm, 
        day_of_week,
        day_of_week_number,
        _fivetran_synced,
        valid_from,
        valid_to,
        schedule_source

    from unnest_schedules
),

final as (

    select
        schedule_id,
        start_time_hh * 60 + start_time_mm + day_of_week_number * 24 * 60 as start_time,
        end_time_hh * 60 + end_time_mm + day_of_week_number * 24 * 60 as end_time,
        coalesce(valid_from, '1970-01-01') as valid_from,
        valid_to,
        _fivetran_synced,
        day_of_week,
        schedule_source

    from split_times
)

select * 
from final