{% macro duckdb__timestamp_add(datepart, interval, from_timestamp) %}

    {{ from_timestamp }} + ((interval '1 {{ datepart }}') * ({{ interval }}))

{% endmacro %}
