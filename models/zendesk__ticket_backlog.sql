--This model will only run if 'status' is included within the `ticket_field_history_columns` variable.
{{ config(enabled = 'status' in var('ticket_field_history_columns')) }}

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

{% if var('using_brands', True) %}
), brands as (
    select *
    from {{ ref('stg_zendesk__brand') }}
{% endif %}

--The below model is excluded if the user does not include ticket_form_id in the variable as a low percentage of accounts use ticket forms.
{% if 'ticket_form_id' in var('ticket_field_history_columns') %}
), ticket_forms as (
    select *
    from {{ ref('int_zendesk__latest_ticket_form') }}
{% endif %}

--If using organizations, this will be included, if not it will be ignored.
{% if var('using_organizations', True) %}
), organizations as (
    select *
    from {{ ref('stg_zendesk__organization') }}
{% endif %}

), backlog as (
    select
        ticket_field_history.source_relation,
        ticket_field_history.date_day
        ,ticket_field_history.ticket_id
        ,ticket_field_history.status
        ,tickets.created_channel
        {% for col in var('ticket_field_history_columns') if col != 'status' %} --Looking at all history fields the users passed through in their dbt_project.yml file
            {% if col in ['assignee_id'] %} --Standard ID field where the name can easily be joined from stg model.
                ,assignee.name as assignee_name

            {% elif col in ['requester_id'] %} --Standard ID field where the name can easily be joined from stg model.
                ,requester.name as requester_name

            {% elif col in ['ticket_form_id'] %} --Standard ID field where the name can easily be joined from stg model.
                ,ticket_forms.name as ticket_form_name

            {% elif var('using_organizations', True) and col in ['organization_id'] %} --Standard ID field where the name can easily be joined from stg model.
                ,organizations.name as organization_name

            {% elif var('using_brands', True) and col in ['brand_id'] %} --Standard ID field where the name can easily be joined from stg model.
                ,brands.name as brand_name

            {% elif col in ['group_id'] %} --Standard ID field where the name can easily be joined from stg model.
                ,group_names.name as group_name

            {% elif col in ['locale_id'] %} --Standard ID field where the name can easily be joined from stg model.
                ,assignee.locale as local_name

            {% else %} --All other fields are not ID's and can simply be included in the query.
                ,ticket_field_history.{{ col }}
            {% endif %}
        {% endfor %}

    from ticket_field_history

    left join tickets
        on tickets.ticket_id = ticket_field_history.ticket_id
        and tickets.source_relation = ticket_field_history.source_relation

    {% if 'ticket_form_id' in var('ticket_field_history_columns') %} --Join not needed if field is not located in variable, otherwise it is included.
    left join ticket_forms
        on ticket_forms.ticket_form_id = cast(ticket_field_history.ticket_form_id as {{ dbt.type_bigint() }})
        and ticket_forms.source_relation = ticket_field_history.source_relation
    {% endif %}

    {% if 'group_id' in var('ticket_field_history_columns') %}--Join not needed if field is not located in variable, otherwise it is included.
    left join group_names
        on group_names.group_id = cast(ticket_field_history.group_id as {{ dbt.type_bigint() }})
        and group_names.source_relation = ticket_field_history.source_relation
    {% endif %}

    {% if 'assignee_id' in var('ticket_field_history_columns') or 'requester_id' in var('ticket_field_history_columns') or 'locale_id' in var('ticket_field_history_columns')%} --Join not needed if fields is not located in variable, otherwise it is included.
    left join users as assignee
        on assignee.user_id = cast(ticket_field_history.assignee_id as {{ dbt.type_bigint() }})
        and assignee.source_relation = ticket_field_history.source_relation
    {% endif %}

    {% if 'requester_id' in var('ticket_field_history_columns') %} --Join not needed if field is not located in variable, otherwise it is included.
    left join users as requester
        on requester.user_id = cast(ticket_field_history.requester_id as {{ dbt.type_bigint() }})
        and requester.source_relation = ticket_field_history.source_relation
    {% endif %}

    {% if var('using_brands', True) and 'brand_id' in var('ticket_field_history_columns') %} --Join not needed if field is not located in variable, otherwise it is included.
    left join brands
        on brands.brand_id = cast(ticket_field_history.brand_id as {{ dbt.type_bigint() }})
        and brands.source_relation = ticket_field_history.source_relation
    {% endif %}

    {% if var('using_organizations', True) and 'organization_id' in var('ticket_field_history_columns') %} --Join not needed if field is not located in variable, otherwise it is included.
    left join organizations
        on organizations.organization_id = cast(ticket_field_history.organization_id as {{ dbt.type_bigint() }})
        and organizations.source_relation = ticket_field_history.source_relation
    {% endif %}

    where ticket_field_history.status not in ('closed', 'solved', 'deleted')
)

select *
from backlog 