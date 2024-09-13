with audit_logs as (
    select
        _fivetran_synced,
        source_id as schedule_id,
        created_at,
        lower(change_description) as change_description
    from {{ var('audit_log') }}   
    where lower(change_description) like '%workweek changed from%'

), audit_logs_enhanced as (
    select 
        _fivetran_synced,
        schedule_id,
        created_at,
        min(created_at) over (partition by schedule_id) as min_created_at,
        replace(replace(replace(replace(change_description, 
            '&quot;', '"') , 
            'amp;', '') , 
            '=&gt;', ':'), 
            ' ', '')
            as change_description_cleaned
    from audit_logs

), split_to_from as (
    -- 'from'
    select
        audit_logs_enhanced.*,
        cast('1970-01-01' as {{ dbt.type_timestamp() }}) as valid_from,
        created_at as valid_to,
        {{ dbt.split_part('change_description_cleaned', "'to'", 1) }} as schedule_change,
        'from' as change_type -- remove before release but helpful for debugging
    from audit_logs_enhanced
    where created_at = min_created_at -- the 'from' portion only matters for the first row

    union all

    -- 'to'
    select
        audit_logs_enhanced.*,
        created_at as valid_from,
        coalesce(
            lead(created_at) over (
                partition by schedule_id order by created_at), 
            {{ dbt.current_timestamp_backcompat() }})
            as valid_to,
        {{ dbt.split_part('change_description_cleaned', "'to'", 2) }} as schedule_change,
        'to' as change_type -- remove before release but helpful for debugging
    from audit_logs_enhanced

), split_days as (
    {% set days_of_week = {'sun': 0, 'mon': 1, 'tue': 2, 'wed': 3, 'thu': 4, 'fri': 5, 'sat': 6} %}
    {% for day, day_number in days_of_week.items() %}
        select
            split_to_from.*,
            '{{ day }}' as day_of_week,
            cast('{{ day_number }}' as {{ dbt.type_int() }}) as day_of_week_number,
            replace(
                {{ dbt.concat([
                    '"["',
                    zendesk.regex_extract('schedule_change', "'.*?" ~ day ~ ".*?({.*?})'"),
                    '"]"']) }}
                , ',', '},{')
                as schedule_change_cleaned
        from split_to_from
    {% if not loop.last %}union all{% endif %}
    {% endfor %}

), unnested_schedules as (
    -- only want the first "from_schedule" to start off
    select 
        split_days.*,
        replace(replace(replace(unnested_schedule, '{', ''), '}', ''), '"', '') as cleaned_unnested_schedule
    from split_days
    -- need to update for all warehouses
    cross join {{ zendesk.unnest_json_array('schedule_change_cleaned') }} as unnested_schedule

), split_times as (

    select 
        schedule_id,
        cast(nullif({{ dbt.split_part('cleaned_unnested_schedule', "':'", 1) }}, ' ') as {{ dbt.type_int() }}) as start_time_hh, 
        cast(nullif({{ dbt.split_part('cleaned_unnested_schedule', "':'", 2) }}, ' ') as {{ dbt.type_int() }}) as start_time_mm, 
        cast(nullif({{ dbt.split_part('cleaned_unnested_schedule', "':'", 3) }}, ' ') as {{ dbt.type_int() }}) as end_time_hh, 
        cast(nullif({{ dbt.split_part('cleaned_unnested_schedule', "':'", 4) }}, ' ') as {{ dbt.type_int() }}) as end_time_mm, 
        day_of_week,
        day_of_week_number,
        _fivetran_synced,
        valid_from,
        valid_to
    from unnested_schedules
),

final as (

    select
        _fivetran_synced,
        schedule_id,
        valid_from,
        valid_to,
        start_time_hh * 60 + start_time_mm + day_of_week_number * 24 * 60 as start_time,
        end_time_hh * 60 + end_time_mm + day_of_week_number * 24 * 60 as end_time,
        day_of_week,
        day_of_week_number
    from split_times
)

select * 
from final