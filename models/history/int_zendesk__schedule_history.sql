with schedule as (

    select *
    from {{ var('schedule') }}  

), audit_logs as (
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
            'workweek changed from', ''), 
            '&quot;', '"'), 
            'amp;', ''), 
            '=&gt;', ':')
            as change_description_cleaned
    from audit_logs

), split_to_from as (
    -- 'from' establishes the schedule from before the change occurred
    select
        audit_logs_enhanced.*,
        cast('1970-01-01' as {{ dbt.type_timestamp() }}) as valid_from,
        created_at as valid_until,
        {{ dbt.split_part('change_description_cleaned', "' to '", 1) }} as schedule_change,
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
            as valid_until,
        {{ dbt.split_part('change_description_cleaned', "' to '", 2) }} as schedule_change,
        'to' as change_type -- remove before release but helpful for debugging
    from audit_logs_enhanced

), split_days as (
    {% set days_of_week = {'sun': 0, 'mon': 1, 'tue': 2, 'wed': 3, 'thu': 4, 'fri': 5, 'sat': 6} %}
    {% for day, day_number in days_of_week.items() %}
    select
        split_to_from.*,
        '{{ day }}' as day_of_week,
        cast('{{ day_number }}' as {{ dbt.type_int() }}) as day_of_week_number,
        {{ zendesk.regex_extract('schedule_change', "'.*?" ~ day ~ ".*?({.*?})'") }} as day_of_week_schedule
    from split_to_from
    {% if not loop.last %}union all{% endif %}
    {% endfor %}

), unnested_schedules as (
    select
        split_days.*,

{%- if target.type == 'bigquery' %}
        {{ clean_schedule('unnested_schedule') }} as cleaned_unnested_schedule
    from split_days
    cross join unnest(json_extract_array('[' || replace(day_of_week_schedule, ',', '},{') || ']', '$')) as unnested_schedule

{%- elif target.type == 'snowflake' %}
        unnested_schedule.key || ':' || unnested_schedule.value as cleaned_unnested_schedule
    from split_days
    cross join lateral flatten(input => parse_json(replace(replace(day_of_week_schedule, '\}\}', '\}'), '\{\{', '\{'))) as unnested_schedule

{%- elif target.type == 'postgres' %}
        {{ clean_schedule('unnested_schedule::text') }} as cleaned_unnested_schedule
    from split_days
    cross join lateral jsonb_array_elements(('[' || replace(day_of_week_schedule, ',', '},{') || ']')::jsonb) as unnested_schedule

{%- elif target.type in ('databricks', 'spark') %}
        {{ clean_schedule('unnested_schedule') }} as cleaned_unnested_schedule
    from split_days
    lateral view explode(from_json(concat('[', replace(day_of_week_schedule, ',', '},{'), ']'), 'array<string>')) as unnested_schedule

{%- elif target.type == 'redshift' %}
    {# json_parse('[' || replace(replace(day_of_week_schedule, '\}\}', '\}'), '\{\{', '\{') || ']') as json_schedule
    from split_days #}
    {# cross join lateral json_parse(replace(replace(day_of_week_schedule, '\}\}', '\}'), '\{\{', '\{')) as element #}

        cast(null as {{ dbt.type_string() }}) as cleaned_unnested_schedule
    from split_days

{% else %}
        cast(null as {{ dbt.type_string() }}) as cleaned_unnested_schedule
    from split_days
{%- endif %}

), split_times as (

    select 
        unnested_schedules.*,
        cast(nullif({{ dbt.split_part('cleaned_unnested_schedule', "':'", 1) }}, ' ') as {{ dbt.type_int() }}) as start_time_hh, 
        cast(nullif({{ dbt.split_part('cleaned_unnested_schedule', "':'", 2) }}, ' ') as {{ dbt.type_int() }}) as start_time_mm, 
        cast(nullif({{ dbt.split_part('cleaned_unnested_schedule', "':'", 3) }}, ' ') as {{ dbt.type_int() }}) as end_time_hh, 
        cast(nullif({{ dbt.split_part('cleaned_unnested_schedule', "':'", 4) }}, ' ') as {{ dbt.type_int() }}) as end_time_mm
    from unnested_schedules

), final as (

    select
        _fivetran_synced,
        schedule_id,
        start_time_hh * 60 + start_time_mm + 24 * 60 * day_of_week_number as start_time,
        end_time_hh * 60 + end_time_mm + 24 * 60 * day_of_week_number as end_time,
        valid_from,
        valid_until,
        day_of_week,
        day_of_week_number
    from split_times
)

select * 
from final