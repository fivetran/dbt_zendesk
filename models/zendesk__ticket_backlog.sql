with ticket_field_history as (
    select *
    from {{ ref('zendesk__ticket_field_history') }}

), tickets as (
    select *
    from {{ ref('stg_zendesk__ticket') }}

), group_names as (
    select *
    from {{ ref('stg_zendesk__group') }}

), users as (
    select *
    from {{ ref('stg_zendesk__user') }}

), brands as (
    select *
    from {{ ref('stg_zendesk__brand') }}

--The below model is excluded if the user does not include ticket_form_id in the variable as a low percentage of accounts use ticket forms.
{% if 'ticket_form_id' in var('ticket_field_history_columns') %}
), ticket_forms as (
    select *
    from {{ ref('stg_zendesk__ticket_form_history') }}
{% endif %}

), organizations as (
    select *
    from {{ ref('stg_zendesk__organization') }}

), backlog as (
    select
        ticket_field_history.date_day
        ,tickets.created_channel
        {% set field_count = var('ticket_field_history_columns')|length + 2 %} --Adding 2 in order to include date_day and created_channel in group by
        {% for col in var('ticket_field_history_columns') %} --Looking at all history fields the users passed through in their dbt_project.yml file
            {% if col in ['assignee_id'] %} --Standard ID field where the name can easily be joined from stg model.
                ,users.name as assignee_name

            {% elif col in ['requester_id'] %} --Standard ID field where the name can easily be joined from stg model.
                ,users.name as requester_name

            {% elif col in ['ticket_form_id'] %} --Standard ID field where the name can easily be joined from stg model.
                ,ticket_forms.name as ticket_form_name

            {% elif col in ['organization_id'] %} --Standard ID field where the name can easily be joined from stg model.
                ,organizations.name as organization_name

            {% elif col in ['brand_id'] %} --Standard ID field where the name can easily be joined from stg model.
                ,brands.name as brand_name

            {% elif col in ['group_id'] %} --Standard ID field where the name can easily be joined from stg model.
                ,group_names.name as group_name

            {% elif col in ['locale_id'] %} --Standard ID field where the name can easily be joined from stg model.
                ,users.locale as local_name

            {% else %} --All other fields are not ID's and can simply be included in the query.
                ,ticket_field_history.{{ col }}
            {% endif %}
        {% endfor %}

        ,count(*) as ticket_count
    from ticket_field_history

    left join tickets
        using(ticket_id)

    {% if 'ticket_form_id' in var('ticket_field_history_columns') %} --Join not needed if field is not located in variable, otherwise it is included.
    left join ticket_forms
        on ticket_forms.ticket_form_id = cast(ticket_field_history.ticket_form_id as {{ dbt_utils.type_int() }})
    {% endif %}

    {% if 'group_id' in var('ticket_field_history_columns') %}--Join not needed if field is not located in variable, otherwise it is included.
    left join group_names
        on group_names.group_id = cast(ticket_field_history.group_id as {{ dbt_utils.type_int() }})
    {% endif %}

    {% if 'assignee_id' in var('ticket_field_history_columns') or 'requester_id' in var('ticket_field_history_columns') or 'locale_id' in var('ticket_field_history_columns')%} --Join not needed if fields is not located in variable, otherwise it is included.
    left join users
        on users.user_id = cast(ticket_field_history.assignee_id as {{ dbt_utils.type_int() }})
    {% endif %}

    {% if 'brand_id' in var('ticket_field_history_columns') %} --Join not needed if field is not located in variable, otherwise it is included.
    left join brands
        on brands.brand_id = cast(ticket_field_history.brand_id as {{ dbt_utils.type_int() }})
    {% endif %}

    {% if 'organization_id' in var('ticket_field_history_columns') %} --Join not needed if field is not located in variable, otherwise it is included.
    left join organizations
        on organizations.organization_id = cast(ticket_field_history.organization_id as {{ dbt_utils.type_int() }})
    {% endif %}

    {{ dbt_utils.group_by(n=field_count) }}
)

select *
from backlog 