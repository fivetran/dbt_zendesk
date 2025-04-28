{{ config(enabled=var('using_audit_log', True)) }}

with audit_logs as (
    select
        source_relation,
        cast(source_id as {{ dbt.type_string() }}) as user_id,
        source_label as user_name,
        created_at,
        lower(change_description) as change_description
    from {{ var('audit_log') }}
    where 
        lower(change_description) like '%support role changed from%'
        and source_type = 'user'

), find_support_role_changes as (
    select
        source_relation,
        user_id,
        user_name,
        created_at,
        cast(created_at as date) as created_date,
        change_description,
        -- extract change description for the support role
        {{ zendesk.regex_extract_support_role_change('change_description') }} as support_role_change_description
    from audit_logs

), split_to_from as (
    select
        source_relation,
        user_id,
        user_name,
        created_at,
        change_description,
        {{ dbt.split_part('support_role_change_description', "' to '", 1) }} as role_updated_from,
        {{ dbt.split_part('support_role_change_description', "' to '", 2) }} as role_updated_to,

        -- Identify the first change record so we know user's beginning role
        min(created_at) over (partition by source_relation, user_id) as min_user_created_at,
        
        -- Identify multiple changes in a single day when row_number > 1
        row_number() over (
            partition by source_relation, user_id, created_date
            order by created_at
        ) as row_number
    from find_support_role_changes

), first_roles as (
    select
        source_relation,
        user_id,
        user_name,
        change_description,
        cast('1970-01-01' as {{ dbt.type_timestamp() }}) as valid_starting_at,
        created_at as valid_ending_at,
        role_updated_from as support_role,
        role_updated_from != 'not set' as is_internal_role
    from split_to_from
    where created_at = min_user_created_at

), role_changes as (
    select
        source_relation,
        user_id,
        user_name,
        change_description,
        created_at as valid_starting_at,
        coalesce(
            lead(created_at) over (partition by source_relation, user_id order by created_at asc),
            {{ dbt.current_timestamp() }}
        ) as valid_ending_at,
        role_updated_to as support_role,
        role_updated_to != 'not set' as is_internal_role
    from split_to_from
    -- multiple changes can occur on one day, so we will keep only the latest change in a day.
    where row_number = 1

), unioned as (
    select *
    from first_roles

    union all

    select *
    from role_changes
)

select *
from unioned