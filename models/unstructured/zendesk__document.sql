with ticket_document as (
    select *
    from {{ ref('int_zendesk__ticket_document') }}

), grouped as (
    select *
    from {{ ref('int_zendesk__ticket_comment_documents_grouped') }}

), final as (
    select
        cast(ticket_document.ticket_id as {{ dbt.type_string() }}) as document_id,
        grouped.chunk_index,
        grouped.chunk_tokens as approximate_chunk_tokens,
        {{ dbt.concat([
            "ticket_document.ticket_markdown",
            "'\\n\\n## COMMENTS\\n\\n'",
            "grouped.comments_group_markdown"]) }}
            as chunk,
    from ticket_document
    join grouped
        on grouped.ticket_id = ticket_document.ticket_id
)

select *
from final