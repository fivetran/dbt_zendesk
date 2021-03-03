with ticket_field_history as (

    select *
    from {{ ref('stg_zendesk__ticket_field_history') }}

), updater_info as (
    select *
    from {{ ref('int_zendesk__updater_information') }}

), final as (
    select
        ticket_field_history.*

        {% if var('ticket_field_history_updater_columns')%}
            {% for col in var('ticket_field_history_updater_columns') %}

                {% if col in ['updater_is_active'] %}
                ,case when ticket_field_history.user_id = -1
                    then true
                    else updater_info.{{ col|lower }}
                        end as {{ col }}

                {% elif col in ['updater_user_id','updater_organization_id'] %}
                ,case when ticket_field_history.user_id = -1 
                    then -1
                    else updater_info.{{ col|lower }}
                        end as {{ col }}
                
                {% elif col in ['updater_last_login_at'] %}
                ,case when ticket_field_history.user_id = -1 
                    then current_timestamp
                    else updater_info.{{ col|lower }}
                        end as {{ col }}
                
                {% else %}
                ,case when ticket_field_history.user_id = -1 
                    then concat('zendesk_automated_', '{{ col_xf }}' )
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
