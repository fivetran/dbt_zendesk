{{ config(enabled=var('using_brands', True)) }}

select *
from {{ var('brand')}}
