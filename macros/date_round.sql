{% macro date_round(field, datepart, round_cutoff=30, interval=1) %}
    
    case when extract(datepart from {{ dbt_utils.date_trunc(datepart,field) }} >= round_cutoff
        then {{ dbt_utils.date_trunc() }}

    case when extract(second from date_trunc(sla_policy_applied.sla_applied_at, second)) >= 30 
    then date_trunc(timestamp_add(timestamp_trunc(sla_policy_applied.sla_applied_at, second), interval 1 minute),minute) 
    else date_trunc(sla_policy_applied.sla_applied_at, minute) end

{% endmacro %}

{% macro round_up(field, datepart, round_cutoff=30, interval=1) %}

{% endmacro %}