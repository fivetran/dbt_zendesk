{{ config(enabled=var('customer360__using_zendesk', true)) }}

with ticket_field_history as (

    select *
    from {{ ref('stg_zendesk__ticket_field_history') }}

), updater_info as (
    select *
    from {{ ref('int_zendesk__updater_information') }}

), final as (
    select
        ticket_field_history.*

        {% if var('ticket_field_history_updater_columns')%} --The below will be run if any fields are included in the variable within the dbt_project.yml.
            {% for col in var('ticket_field_history_updater_columns') %} --Iterating through the updater fields included in the variable.

                --The below statements are needed to populate Zendesk automated fields for when the zendesk triggers automatically change fields based on user defined triggers.
                {% if col in ['updater_is_active'] %}
                    ,coalesce(updater_info.{{ col|lower }}, true) as {{ col }}

                {% elif col in ['updater_user_id','updater_organization_id'] %}
                    ,coalesce(updater_info.{{ col|lower }}, -1) as {{ col }}
                
                {% elif col in ['updater_last_login_at'] %}
                    ,coalesce(updater_info.{{ col|lower }}, current_timestamp) as {{ col }}
                
                {% else %}
                    ,coalesce(updater_info.{{ col|lower }}, concat('zendesk_trigger_change_', '{{ col }}' )) as {{ col }}
  
                {% endif %}
            {% endfor %}
        {% endif %}  

    from ticket_field_history

    left join updater_info
        on ticket_field_history.user_id = updater_info.updater_user_id
)
select *
from final
