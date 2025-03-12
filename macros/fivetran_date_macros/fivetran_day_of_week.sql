{%- macro fivetran_day_of_week(date, isoweek=true) -%}
{{ adapter.dispatch('fivetran_day_of_week', 'zendesk') (date, isoweek) }}
{%- endmacro %}

{%- macro default__fivetran_day_of_week(date, isoweek) -%}

    {%- set dow = zendesk.fivetran_date_part('dayofweek', date) -%}

    {%- if isoweek -%}
    case
        -- Shift start of week from Sunday (0) to Monday (1)
        when {{ dow }} = 0 then 7
        else {{ dow }}
    end
    {%- else -%}
    {{ dow }} + 1
    {%- endif -%}

{%- endmacro %}

{%- macro snowflake__fivetran_day_of_week(date, isoweek) -%}

    {%- if isoweek -%}
        {%- set dow_part = 'dayofweekiso' -%}
        {{ zendesk.fivetran_date_part(dow_part, date) }}
    {%- else -%}
        {%- set dow_part = 'dayofweek' -%}
        case
            when {{ zendesk.fivetran_date_part(dow_part, date) }} = 7 then 1
            else {{ zendesk.fivetran_date_part(dow_part, date) }} + 1
        end
    {%- endif -%}



{%- endmacro %}

{%- macro bigquery__fivetran_day_of_week(date, isoweek) -%}

    {%- set dow = zendesk.fivetran_date_part('dayofweek', date) -%}

    {%- if isoweek -%}
    case
        -- Shift start of week from Sunday (1) to Monday (2)
        when {{ dow }} = 1 then 7
        else {{ dow }} - 1
    end
    {%- else -%}
    {{ dow }}
    {%- endif -%}

{%- endmacro %}


{%- macro postgres__fivetran_day_of_week(date, isoweek) -%}

    {%- if isoweek -%}
        {%- set dow_part = 'isodow' -%}
        -- Monday(1) to Sunday (7)
        cast({{ zendesk.fivetran_date_part(dow_part, date) }} as {{ dbt.type_int() }})
    {%- else -%}
        {%- set dow_part = 'dow' -%}
        -- Sunday(1) to Saturday (7)
        cast({{ zendesk.fivetran_date_part(dow_part, date) }} + 1 as {{ dbt.type_int() }})
    {%- endif -%}

{%- endmacro %}


{%- macro redshift__fivetran_day_of_week(date, isoweek) -%}

    {%- set dow = zendesk.fivetran_date_part('dayofweek', date) -%}

    {%- if isoweek -%}
    case
        -- Shift start of week from Sunday (0) to Monday (1)
        when {{ dow }} = 0 then 7
        else cast({{ dow }} as {{ dbt.type_bigint() }})
    end
    {%- else -%}
    cast({{ dow }} + 1 as {{ dbt.type_bigint() }})
    {%- endif -%}

{%- endmacro %}

{%- macro duckdb__fivetran_day_of_week(date, isoweek) -%}
{{ return(zendesk.postgres__fivetran_day_of_week(date, isoweek)) }}
{%- endmacro %}


{%- macro spark__fivetran_day_of_week(date, isoweek) -%}

    {%- set dow = "dayofweek_iso" if isoweek else "dayofweek" -%}

    {{ zendesk.fivetran_date_part(dow, date) }}

{%- endmacro %}
