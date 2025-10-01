{%- macro partition_by_source_relation(has_other_partitions='yes',alias=None) -%}

{% set prefix = '' if alias is none else alias ~ '.' %}

    {%- if has_other_partitions == 'no' -%}
        {{ 'partition by ' ~ prefix ~ 'source_relation' if var('zendesk_sources', [])|length > 1 }}
    {%- else -%}
        {{ ', ' ~ prefix ~ 'source_relation' if var('zendesk_sources', [])|length > 1 }}
    {%- endif -%}
{%- endmacro -%}