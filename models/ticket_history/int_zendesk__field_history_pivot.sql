-- depends_on: {{ ref('int_zendesk__field_history_column_names') }}

{{
    config(
        materialized='incremental',
        partition_by = {'field': 'date_day', 'data_type': 'date', 'granularity': 'month'} if target.type not in ['spark', 'databricks', 'duckdb'] else ['date_day'],
        unique_key='ticket_day_id',
        incremental_strategy = 'merge' if target.type not in ('snowflake', 'postgres', 'redshift') else 'delete+insert',
        file_format='delta'
        )
}}

-- Custom ticket field IDs are stored as the raw numeric ID in field_name. int_zendesk__field_history_column_names
-- already resolved each tracked field to its human-readable title (or its own name for standard fields like
-- status/priority) as an actual table, so this is a cheap read, not a re-join. What's left here is purely the
-- Jinja-side work of turning that into valid, deduplicated SQL identifiers -- resolved_columns stays {} outside
-- the run/build guard (e.g. during dbt compile), so this is always safe to reference below.
{% set resolved_columns = {} %}

{% if execute and flags.WHICH in ('run', 'build') -%}
    -- Uses positional row access (not a name-keyed dict) since some adapters (e.g. Snowflake) fold unquoted
    -- result column names to uppercase, which would silently break a lookup keyed on the lowercase alias.
    {% set resolved_results = run_query("select field_name, resolved_name from " ~ ref('int_zendesk__field_history_column_names')) %}

    -- First pass: slugify each column's resolved name (not yet deduplicated).
    {% set slugified_names = {} %}
    {% for row in resolved_results.rows %}
        {% do slugified_names.update({row[0]: dbt_utils.slugify(row[1])}) %}
    {% endfor %}

    -- Count how many columns land on the same slugified name so we know which ones need disambiguating.
    {% set name_counts = {} %}
    {% for col, name in slugified_names.items() %}
        {% do name_counts.update({name: name_counts.get(name, 0) + 1}) %}
    {% endfor %}

    -- Second pass: append the raw id/name to any column whose slugified name collides with another's.
    {% for col, name in slugified_names.items() %}
        {% if name_counts[name] > 1 %}
            {% set suffix = dbt_utils.slugify(col | string) %}
            {% set suffix = suffix[1:] if suffix.startswith('_') else suffix %}
            {% do resolved_columns.update({col: name ~ '_' ~ suffix}) %}
        {% else %}
            {% do resolved_columns.update({col: name}) %}
        {% endif %}
    {% endfor %}
{% endif -%}

with field_history as (

    select
        source_relation,
        ticket_id,
        field_name,
        valid_ending_at,
        valid_starting_at

        --Only runs if the user passes updater fields through the final ticket field history model
        {% if var('ticket_field_history_updater_columns') %}
        ,
        {{ var('ticket_field_history_updater_columns') | join (", ")}}

        {% endif %}

        -- doing this to figure out what values are actually null and what needs to be backfilled in zendesk__ticket_field_history
        ,case when value is null then 'is_null' else value end as value

    from {{ ref('int_zendesk__field_history_enriched') }}
    {% if is_incremental() %}
    where cast( {{ dbt.date_trunc('day', 'valid_starting_at') }} as date) >= (select max(date_day) from {{ this }})
    {% endif %}

), event_order as (

    select 
        *,
        row_number() over (
            partition by cast(valid_starting_at as date), ticket_id, field_name {{ fivetran_utils.partition_by_source_relation(package_name='zendesk') }}
            order by valid_starting_at desc
            ) as row_num
    from field_history

), filtered as (

    -- Find the last event that occurs on each day for each ticket

    select *
    from event_order
    where row_num = 1

), pivots as (

    -- For each column that is in both the ticket_field_history_columns variable and the field_history table,
    -- pivot out the value into it's own column. This will feed the daily slowly changing dimension model.

    select
        source_relation, 
        ticket_id,
        cast({{ dbt.date_trunc('day', 'valid_starting_at') }} as date) as date_day

        {% for col, col_xf in resolved_columns.items() %}
            ,min(case when lower(field_name) = '{{ col|lower }}' then filtered.value end) as {{ col_xf }}

            --Only runs if the user passes updater fields through the final ticket field history model
            {% if var('ticket_field_history_updater_columns') %}

                {% for upd in var('ticket_field_history_updater_columns') %}

                    {% set upd_xf = (col_xf + '_' + upd ) %} --Creating the appropriate column name based on the history field + update field names.

                    {% if upd == 'updater_is_active' and target.type in ('postgres', 'redshift') %}

                        ,bool_or(case when lower(field_name) = '{{ col|lower }}' then filtered.{{ upd }} end) as {{ upd_xf }}

                    {% else %}

                        ,min(case when lower(field_name) = '{{ col|lower }}' then filtered.{{ upd }} end) as {{ upd_xf }}

                    {% endif %}
                {% endfor %}
            {% endif %}
        {% endfor %}
    
    from filtered
    group by 1,2,3

), surrogate_key as (

    select 
        *,
        {{ dbt_utils.generate_surrogate_key(['source_relation','ticket_id','date_day'])}} as ticket_day_id
    from pivots

)

select *
from surrogate_key