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
        min(created_at) over (partition by source_relation, user_id) as min_created_at_per_user
    from audit_logs

-- Create a cte to isolate the first "from" role
), first_roles as (
    select
        source_relation,
        user_id,
        user_name,
        change_description,
        cast(null as {{ dbt.type_timestamp() }}) as valid_starting_at, --fill in with created_at of user later
        created_at as valid_ending_at, -- this it the created_at of the audit log entry
        from_role as role
    from split_to_from
    where created_at = min_created_at_per_user

-- This cte captures all subsequent "to" roles
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
        users.user_id,
        users.source_relation,
        lower(coalesce(unioned.role, users.role)) as role,
        coalesce(unioned.valid_starting_at, users.created_at, cast('1970-01-01' as {{ dbt.type_timestamp() }})) as valid_starting_at,
        coalesce(unioned.valid_ending_at, {{ dbt.current_timestamp() }}) as valid_ending_at,
        unioned.change_description,
        -- include these in case they're needed for the internal_user_criteria
        users.external_id,
        users.email,
        users.last_login_at,
        users.created_at,
        users.updated_at,
        users.name,
        users.organization_id,
        users.phone,
        users.ticket_restriction,
        users.time_zone,
        users.locale,
        users.is_active,
        users.is_suspended
    from users
    left join unioned
        on users.user_id = unioned.user_id
        and users.source_relation = unioned.source_relation

), final as (
    select
        user_id,
        source_relation,
        role,
        valid_starting_at,
        valid_ending_at,
        change_description,

        {% if var('internal_user_criteria', false) -%} -- apply the filter to historical roles if provided
        role in ('admin', 'agent') or {{ var('internal_user_criteria', false) }} as is_internal_role
        {% else -%}
        role not in ('not set', 'end-user') as is_internal_role
        {% endif -%}
    from users_joined
)

select *
from final