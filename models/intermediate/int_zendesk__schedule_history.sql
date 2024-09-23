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
            'workweek changed from', ''), 
            '&quot;', '"'), 
            'amp;', ''), 
            '=&gt;', ':')
            as change_description_cleaned
    from audit_logs

), split_to_from as (
    -- 'from'
    select
        audit_logs_enhanced.*,
        cast('1970-01-01' as {{ dbt.type_timestamp() }}) as valid_from,
        created_at as valid_to,
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
            as valid_to,
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
        replace(replace(replace(replace(unnested_schedule, '{', ''), '}', ''), '"', ''), ' ', '') as cleaned_unnested_schedule
    from split_days
    cross join unnest(json_extract_array('[' || replace(day_of_week_schedule, ',', '},{') || ']', '$')) as unnested_schedule

{%- elif target.type == 'snowflake' %}
        unnested_schedule.key || ':' || unnested_schedule.value as cleaned_unnested_schedule
    from split_days
    cross join lateral flatten(input => parse_json(replace(replace(day_of_week_schedule, '\}\}', '\}'), '\{\{', '\{'))) as unnested_schedule

{%- elif target.type == 'postgres' %}
        replace(replace(replace(replace(unnested_schedule::text, '{', ''), '}', ''), '"', ''), ' ', '') as cleaned_unnested_schedule
    from split_days
    cross join lateral jsonb_array_elements(('[' || replace(day_of_week_schedule, ',', '},{') || ']')::jsonb) as unnested_schedule

{%- elif target.type in ('databricks', 'spark') %}
        replace(replace(replace(replace(unnested_schedule, '{', ''), '}', ''), '"', ''), ' ', '') as cleaned_unnested_schedule
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
)

select * 
from unnested_schedules