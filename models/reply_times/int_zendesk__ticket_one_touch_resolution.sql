with public_comments as (
  
  select *
  from {{ ref('int_zendesk__public_comments') }}

), internal_comments as (

  select 
    ticket_id,
    sum(case when commenter_role = 'internal_comment' then 1
        else 0 end) as count_internal_comments
  from public_comments
  group by 1

), flagged as (

  select 
    *,
    count_internal_comments = 1 as is_one_touch_resolution,
    count_internal_comments = 2 as is_two_touch_resolution
  from internal_comments

)
select *
from flagged