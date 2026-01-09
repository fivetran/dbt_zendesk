{{ config(enabled=var('using_audit_log', False)) }}

select *
from {{ var('audit_log') }}
