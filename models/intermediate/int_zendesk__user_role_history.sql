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

), begin_splitting as (
    select
        source_relation,
        user_id,
        user_name,
        created_at,
        change_description,
        -- grab everything AFTER this
        {{ dbt.split_part('change_description', "'support role changed from '", 2) }} as first_split
    from audit_logs

), split_out_from_role as (
    select
        source_relation,
        user_id,
        user_name,
        created_at,
        change_description,
        {{ dbt.split_part('first_split', "' to '", 1) }} as from_role,
        {{ dbt.split_part('first_split', "' to '", 2) }} as second_split
    from begin_splitting

), split_out_to_role as (
    select
        source_relation,
        user_id,
        user_name,
        created_at,
        change_description,
        from_role,
        -- role changes (support, guide, explore, chat) get batched together. let's make sure we're only looking at support roles
        {{ dbt.split_part('second_split', "'\\n'", 1) }} as role_change
    from split_out_from_role

), user_history as (

    select
        source_relation,
        user_id,
        user_name,
        created_at as valid_starting_at,
        lead(created_at) over (partition by user_id, source_relation order by created_at asc) as valid_ending_at,
        change_description,
        role_change,
        role_change != 'not set' as is_internal_role

    from split_out_to_role
)

select *
from user_history