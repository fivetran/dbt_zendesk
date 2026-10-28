{% macro zendesk_slugify(string) %}
  {{ return(adapter.dispatch('zendesk_slugify', 'zendesk')(string)) }}
{% endmacro %}

{% macro default__zendesk_slugify(string) %}

{# Same as dbt_utils.slugify, except a value with no Latin letters or numbers (e.g. 購入段階) returns '' instead of failing with "str object has no element 0". #}

{% set slug = (string | string | lower) if string else '' %}
{% set slug = modules.re.sub('[ -]+', '_', slug) %}
{% set slug = modules.re.sub('[^a-z0-9_]+', '', slug) %}
{# Prepends "_" if the slug begins with a number. #}
{% set slug = modules.re.sub('^([0-9])', '_\\1', slug) %}

{{ return(slug) }}

{% endmacro %}
