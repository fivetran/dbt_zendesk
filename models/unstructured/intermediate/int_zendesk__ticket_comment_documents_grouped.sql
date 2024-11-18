{{ config(enabled=var('zendesk__unstructured_enabled', False)) }}

with filtered_comment_documents as (
  select *
  from {{ ref('int_zendesk__ticket_comment_document') }}
),

grouped_comment_documents as (
  select 
    source_relation,
    ticket_id,
    comment_markdown,
    comment_tokens,
    comment_time,
    sum(comment_tokens) over (
      partition by source_relation, ticket_id 
      order by comment_time
      rows between unbounded preceding and current row
    ) as cumulative_length
  from filtered_comment_documents
)

select 
  source_relation,
  ticket_id,
  cast({{ dbt_utils.safe_divide('floor(cumulative_length - 1)', var('zendesk_max_tokens', 5000)) }} as {{ dbt.type_int() }}) as chunk_index,
  {{ dbt.listagg(
    measure="comment_markdown",
    delimiter_text="'\\n\\n---\\n\\n'",
    order_by_clause="order by comment_time"
    ) }} as comments_group_markdown,
  sum(comment_tokens) as chunk_tokens
from grouped_comment_documents
group by 1,2,3