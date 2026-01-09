{{ config(enabled=var('using_audit_log', False)) }}

select 
    cast(null as {{ dbt.type_string() }}) as source_relation,
    *
from {{ var('audit_log') }}