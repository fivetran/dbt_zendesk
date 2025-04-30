{{ config(enabled=var('using_user_role_histories', True) and var('using_audit_log', False)) }}

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

), split_to_from as (
    select
        source_relation,
        user_id,
        user_name,
        created_at,
        change_description,
        -- extract and split change description for the support role
        trim({{ dbt.split_part(zendesk.extract_support_role_changes('change_description'), "' to '", 1) }}) as from_role,
        trim({{ dbt.split_part(zendesk.extract_support_role_changes('change_description'), "' to '", 2) }}) as to_role,

        -- Identify the first change record so we know user's beginning role
        min(created_at) over (partition by source_relation, user_id) as min_user_created_at
    from audit_logs

), first_roles as (
    select
        source_relation,
        user_id,
        user_name,
        change_description,
        cast('1970-01-01' as {{ dbt.type_timestamp() }}) as valid_starting_at,
        created_at as valid_ending_at,
        from_role as role
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
        to_role as role
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
        coalesce(users.source_relation, unioned.source_relation) as source_relation,
        coalesce(users.user_id, unioned.user_id) as user_id,
        coalesce(unioned.role, users.role) as role,
        coalesce(unioned.valid_starting_at, cast('1970-01-01' as {{ dbt.type_timestamp() }})) as valid_starting_at,
        coalesce(unioned.valid_ending_at, {{ dbt.current_timestamp() }}) as valid_ending_at,
        coalesce(unioned.role != 'not set', users.role in ('agent','admin')) as is_internal_role,
        unioned.change_description

    from users
    full outer join unioned
    on users.user_id = unioned.user_id
        and users.source_relation = unioned.source_relation
)

select *
from users_joined