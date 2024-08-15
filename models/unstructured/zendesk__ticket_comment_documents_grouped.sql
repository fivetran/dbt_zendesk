with filtered_comment_documents as (
  select *
  from {{ ref('zendesk__ticket_comment_document') }}
),

grouped_comment_documents as (
  select 
    ticket_id,
    comment_markdown,
    comment_tokens,
    comment_time,
    sum(comment_tokens) over (
      partition by ticket_id 
      order by comment_time
      rows between unbounded preceding and current row
    ) as cumulative_length
  from filtered_comment_documents
)

select 
  ticket_id,
  cast(floor((cumulative_length - 1) / {{ var('max_tokens', 7500) }}) as {{ dbt.type_int() }}) as chunk_index,
  {{ dbt.listagg(
    measure="comment_markdown",
    delimiter_text="'\\n\\n---\\n\\n'",
    order_by_clause="order by comment_time"
    ) }} as comments_group_markdown,
  sum(comment_tokens) as chunk_tokens
from grouped_comment_documents
group by 1,2