{% set updater_fields = [] %}
with ticket_field_history as (

    select *
    from {{ ref('stg_zendesk__ticket_field_history') }}

), users as (
    select *
    from {{ ref('int_zendesk__user_aggregates') }}

), organizations as (
    select *
    from {{ ref('int_zendesk__organization_aggregates') }}

), final as (
    select
        ticket_field_history.*

        {% if var('ticket_field_history_updater_user_columns') != []%}       
            {% for col in var('ticket_field_history_updater_user_columns') %}
                {% set col_fx = ("updater_" + col|lower) %}

                {% if col in ['is_active'] %}
                ,case when ticket_field_history.user_id = -1 
                    then true
                    else users.{{ col|lower }}
                        end as {{ col_fx }}

                {% elif col in ['user_id'] %}
                ,case when ticket_field_history.user_id = -1 
                    then -1
                    else users.{{ col|lower }}
                        end as {{ col_fx }}
                
                {% elif col in ['last_login_at'] %}
                ,case when ticket_field_history.user_id = -1 
                    then current_timestamp
                    else users.{{ col|lower }}
                        end as {{ col_fx }}
                
                {% else %}

                ,case when ticket_field_history.user_id = -1 
                    then concat('zendesk_automated_', '{{ col_fx }}' )
                    else users.{{ col|lower }}
                        end as {{ col_fx }}

                {% endif %}
            {% endfor %}
        {% endif %}  

        {% if var('ticket_field_history_updater_organization_columns') != []%}
            {% for col in var('ticket_field_history_updater_organization_columns') %}
                {% set col_fx = ("updater_organization_" + col|lower) %}
                
                {% if col in ['organization_id'] %}
                ,case when ticket_field_history.user_id = -1
                    then -1
                    else organizations.{{ col|lower }}
                        end as updater_organization_id

                {% else %}
                ,case when ticket_field_history.user_id = -1 
                    then concat('zendesk_automated_', '{{ col_fx }}' )
                    else organizations.{{ col|lower }}
                        end as {{ col_fx }}
                
                {% endif %}  
            {% endfor %}
        {% endif %}  
    from ticket_field_history

    left join users
        using(user_id)

    left join organizations
        on users.organization_id = organizations.organization_id
)

select *
from final
