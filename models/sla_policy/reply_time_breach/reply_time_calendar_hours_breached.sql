{{ config(enabled=var('using_sla_policy', True)) }}

--REPLY TIME BREACH
-- step 2, figure out when the sla will breach for sla's in calendar hours. the calculation is relatively straightforward.

with sla_policy_applied as (

  select *
  from {{ ref('sla_policy_applied') }}

)

  select
    *,
    timestamp_add(sla_applied_at, interval cast(target as int64) minute) as breached_at -- need to figure out how to do a cross-db timestamp add. dbt_utils currently has dateadd only, which results in a datetime rather than timestamp.
  from sla_policy_applied
  where in_business_hours = 'false'
    and metric in ('next_reply_time', 'first_reply_time')
