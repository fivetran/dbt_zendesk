{{ config(enabled=var('using_audit_log', True)) }}

with audit_logs as (
    select
        source_relation,
        source_id as user_id,
        source_label as user_name,
        created_at,
        lower(change_description) as change_description
    from {{ var('audit_log') }}
    where 
        lower(change_description) like '%support role changed from%'
        and source_type = 'user'

), users as (
    select *
    from {{ var('user') }}

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
        min(created_at) over (partition by source_relation, user_id) as min_user_created_at
    from find_support_role_changes

), first_roles as (
    select
        source_relation,
        user_id,
        user_name,
        change_description,
        cast('1970-01-01' as {{ dbt.type_timestamp() }}) as valid_starting_at,
        created_at as valid_ending_at,
        role_updated_from as role,
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
        lead(created_at) over (partition by source_relation, user_id order by created_at asc) as valid_ending_at,
        role_updated_to as role,
        role_updated_to != 'not set' as is_internal_role
    from split_to_from

), unioned as (
    select *
    from first_roles

    union all

    select *
    from role_changes

), users_joined as (

    -- create history records for users with no changes
    select 
        users.source_relation,
        users.user_id,
        coalesce(unioned.role, users.role) as role,
        coalesce(unioned.valid_starting_at, cast('1970-01-01' as {{ dbt.type_timestamp() }})) as valid_starting_at,
        coalesce(unioned.valid_ending_at, {{ dbt.current_timestamp() }}) as valid_ending_at,
        coalesce(unioned.is_internal_role, users.role in ('agent','admin')) as is_internal_role

    from users
    full outer join unioned
    on users.user_id = unioned.user_id
        and users.source_relation = unioned.source_relation
)

select *
from users_joined