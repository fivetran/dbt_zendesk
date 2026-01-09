{{ config(enabled=var('using_ticket_chat', False)) }}

select *
from {{ var('ticket_chat')}}