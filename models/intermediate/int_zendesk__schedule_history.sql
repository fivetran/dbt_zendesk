with reformat as (

    select 
        _fivetran_synced,
        created_at,
        source_id as schedule_id,
        change_description as original,
        replace(replace(replace(replace(replace(replace(replace(replace(lower(change_description), '=&amp;gt;', ': '), 
            ':mon', '"mon"'), ':tue', '"tue"'), ':wed', '"wed"'), ':thu', '"thu"'), ':fri', '"fri"'), ':sat', '"sat"'), ':sun', '"sun"')  as change_description

    from {{ ref('stg_zendesk__audit_log') }}
    where lower(change_description) like '%workweek%'
    order by created_at desc
),

jsonify as (

    select 
        _fivetran_synced,
        created_at,
        schedule_id,
        original,
        {{ dbt.split_part('change_description', "'workweek changed from '", 2) }} as change_description
    from reformat
),

split_up as (

    select
        _fivetran_synced,
        created_at,
        schedule_id,
        original,
        {{ dbt.split_part('change_description', "' to '", 1) }} as from_schedule,
        {{ dbt.split_part('change_description', "' to '", 2) }} as to_schedule
    from jsonify
),

split_days as (
    
    select 
        _fivetran_synced,
        created_at,
        schedule_id,
        original
        {%- for day in ['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun'] -%}
            , REGEXP_EXTRACT({{ json_parse_nonscalar('from_schedule', [day]) }}, r'{"([^"]+)"') as from_{{ day }}_start
            , REGEXP_EXTRACT({{ json_parse_nonscalar('from_schedule', [day]) }}, r'":"([^"]+)"}') as from_{{ day }}_end
            , REGEXP_EXTRACT({{ json_parse_nonscalar('to_schedule', [day]) }}, r'{"([^"]+)"') as to_{{ day }}_start
            , REGEXP_EXTRACT({{ json_parse_nonscalar('to_schedule', [day]) }}, r'":"([^"]+)"}') as to_{{ day }}_end
        {% endfor %}

    from split_up
),

verticalize as (

    {%- for day in ['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun'] -%}
    select 
        _fivetran_synced,
        lag(created_at) over (partition by schedule_id order by created_at) as created_at,
        schedule_id,
        original,
        '{{ day }}' as dow,
        from_{{ day }}_start as start_time,
        from_{{ day }}_end as end_time

    from split_days
    where from_{{ day }}_start is not null and from_{{ day }}_end is not null

    union distinct

    select 
        _fivetran_synced,
        created_at,
        schedule_id,
        original,
        '{{ day }}' as dow,
        to_{{ day }}_start as start_time,
        to_{{ day }}_end as end_time
        
    from split_days
    where to_{{ day }}_start is not null and to_{{ day }}_end is not null

    {% if not loop.last %}union distinct{% endif %}

    {% endfor %}
),

split_times as (

    select 
        schedule_id,
        cast({{ dbt.split_part('start_time', "':'", 1) }} as {{ dbt.type_int() }}) as start_time_hh, 
        cast({{ dbt.split_part('start_time', "':'", 2) }} as {{ dbt.type_int() }}) as start_time_mm, 
        cast({{ dbt.split_part('end_time', "':'", 1) }} as {{ dbt.type_int() }}) as end_time_hh, 
        cast({{ dbt.split_part('end_time', "':'", 2) }} as {{ dbt.type_int() }}) as end_time_mm, 
        start_time,
        end_time,
        dow,
        _fivetran_synced,
        created_at as valid_from,
        coalesce(lead(created_at) over (partition by schedule_id, dow order by created_at), {{ dbt.current_timestamp_backcompat() }}) as valid_to

    from verticalize
),

final as (

    select
        schedule_id,
        start_time_hh * 60 + start_time_mm + 24 * 60 * case 
            when dow = 'mon' then 1 
            when dow = 'tue' then 2
            when dow = 'wed' then 3
            when dow = 'thu' then 4
            when dow = 'fri' then 5
            when dow = 'sat' then 6
        else 0 end as start_time,
        end_time_hh * 60 + end_time_mm + 24 * 60 * case 
            when dow = 'mon' then 1 
            when dow = 'tue' then 2
            when dow = 'wed' then 3
            when dow = 'thu' then 4
            when dow = 'fri' then 5
            when dow = 'sat' then 6
        else 0 end as end_time,
        coalesce(valid_from, '1970-01-01') as valid_from,
        valid_to,
        _fivetran_synced,
        dow

    from split_times
)

select * 
from final