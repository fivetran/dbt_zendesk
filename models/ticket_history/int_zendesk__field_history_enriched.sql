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
            {% for col in var('ticket_field_history_updater_columns') %} --Iterating through the updater fields included in the varuable.

                --The below case when statements are needed to populate Zendesk automated fields for -1 user_id. Or, pass through the user defined -1 fields.
                {% if col in ['updater_is_active'] %}
                ,case when ticket_field_history.user_id = -1
                    then coalesce(updater_info.{{ col|lower }}, true)
                    else updater_info.{{ col|lower }}
                        end as {{ col }}

                {% elif col in ['updater_user_id','updater_organization_id'] %}
                ,case when ticket_field_history.user_id = -1 
                    then coalesce(updater_info.{{ col|lower }}, -1)
                    else updater_info.{{ col|lower }}
                        end as {{ col }}
                
                {% elif col in ['updater_last_login_at'] %}
                ,case when ticket_field_history.user_id = -1 
                    then coalesce(updater_info.{{ col|lower }}, current_timestamp)
                    else updater_info.{{ col|lower }}
                        end as {{ col }}
                
                {% else %}
                ,case when ticket_field_history.user_id = -1 
                    then coalesce(updater_info.{{ col|lower }}, concat('zendesk_automated_', '{{ col_xf }}' ))
                    else updater_info.{{ col|lower }}
                        end as {{ col }}
  
                {% endif %}
            {% endfor %}
        {% endif %}  

    from ticket_field_history

    left join updater_info
        on ticket_field_history.user_id = updater_info.updater_user_id
)
select *
from final
