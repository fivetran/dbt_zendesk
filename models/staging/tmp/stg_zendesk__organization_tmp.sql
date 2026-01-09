{{ config(enabled=var('using_organizations', True)) }}

select *
from {{ var('organization')}}
