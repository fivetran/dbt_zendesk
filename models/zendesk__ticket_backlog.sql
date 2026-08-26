--This model will only run if 'status' is included within the `ticket_field_history_columns` variable.
{{ config(enabled = 'status' in var('ticket_field_history_columns')) }}

--Passes through the upstream columns as-is, rather than re-resolving custom field names.
{% set ticket_field_history_relation_columns = adapter.get_columns_in_relation(ref('zendesk__ticket_field_history')) %}

--ID fields that resolve to a readable name via a join, declared once so the passthrough loop below can exclude
--exactly what was handled here. A field left out of this mapping passes through as its raw ID instead.
{% set id_fields_with_names = {
    'assignee_id': 'ticket_field_history.assignee_id, assignee.name as assignee_name',
    'requester_id': 'ticket_field_history.requester_id, requester.name as requester_name',
    'ticket_form_id': 'ticket_forms.name as ticket_form_name',
    'group_id': 'group_names.name as group_name',
    'locale_id': 'assignee.locale as local_name'
} %}
{% if var('using_organizations', True) %}
    {% do id_fields_with_names.update({'organization_id': 'organizations.name as organization_name'}) %}
{% endif %}
{% if var('using_brands', True) %}
    {% do id_fields_with_names.update({'brand_id': 'brands.name as brand_name'}) %}
{% endif %}

--Columns selected explicitly below, either as part of the grain or by the mapping above.
{% set columns_already_selected = ['source_relation', 'date_day', 'ticket_id', 'status', 'ticket_day_id']
                                  + (id_fields_with_names.keys() | list) %}

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
        {% for col in var('ticket_field_history_columns') if col in id_fields_with_names %} --Standard ID fields where the name can easily be joined from stg model.
            ,{{ id_fields_with_names[col] }}
        {% endfor %}
        {% for column in ticket_field_history_relation_columns if column.name|lower not in columns_already_selected %} --Everything else, including resolved custom field names, passes through as-is.
            ,ticket_field_history.{{ column.name }}
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