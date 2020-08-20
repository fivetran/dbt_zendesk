{% macro timestamp_add(datepart, interval, from_timestamp) -%}

{{ adapter_macro('zendesk.timestamp_add', datepart, interval, from_timestamp) }}

{%- endmacro %}


{% macro default__timestamp_add(datepart, interval, from_timestamp) %}

    timestampadd(
        {{ datepart }},
        {{ interval }},
        {{ from_timestamp }}
        )

{% endmacro %}


{% macro bigquery__timestamp_add(datepart, interval, from_timestamp) %}

        timestamp_add({{ from_timestamp }}, interval  {{ interval }} {{ datepart }})

{% endmacro %}

{% macro redshift__timestamp_add(datepart, interval, from_timestamp) %}

        dateadd(
        {{ datepart }},
        {{ interval }},
        {{ from_timestamp }}
        )

{% endmacro %}