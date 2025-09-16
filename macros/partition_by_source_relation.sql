{%- macro partition_by_source_relation(has_other_partitions='yes') -%}
    {%- if has_other_partitions == 'no' -%}
        {{ 'partition by source_relation' if var('zendesk_sources', [])|length > 1 }}
    {%- else -%}
        {{ ', source_relation' if var('zendesk_sources', [])|length > 1 }}
    {%- endif -%}
{%- endmacro -%}