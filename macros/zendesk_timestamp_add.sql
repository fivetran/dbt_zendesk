{% macro zendesk_timestamp_add(datepart, interval, from_timestamp) -%}

{{ adapter.dispatch('zendesk_timestamp_add', 'zendesk') (datepart, interval, from_timestamp) }}

{%- endmacro %}


{% macro default__zendesk_timestamp_add(datepart, interval, from_timestamp) %}

    timestampadd(
        {{ datepart }},
        {{ interval }},
        {{ from_timestamp }}
        )

{% endmacro %}


{% macro bigquery__zendesk_timestamp_add(datepart, interval, from_timestamp) %}

        timestamp_add({{ from_timestamp }}, interval  {{ interval }} {{ datepart }})

{% endmacro %}

{% macro redshift__zendesk_timestamp_add(datepart, interval, from_timestamp) %}

        dateadd(
        {{ datepart }},
        {{ interval }},
        {{ from_timestamp }}
        )

{% endmacro %}

{% macro postgres__zendesk_timestamp_add(datepart, interval, from_timestamp) %}

    {{ from_timestamp }} + ((interval '1 {{ datepart }}') * ({{ interval }}))

{% endmacro %}

{% macro spark__zendesk_timestamp_add(datepart, interval, from_timestamp) %}

    {{ dbt.dateadd(datepart, interval, from_timestamp) }}
        
{% endmacro %}

{% macro duckdb__zendesk_timestamp_add(datepart, interval, from_timestamp) %}

    {{ from_timestamp }} + ((interval '1 {{ datepart }}') * ({{ interval }}))

{% endmacro %}