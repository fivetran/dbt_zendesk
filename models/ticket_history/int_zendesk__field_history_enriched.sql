with ticket_field_history as (

    select *
    from {{ ref('stg_zendesk__ticket_field_history') }}

), users as (
    select *
    from {{ ref('stg_zendesk__user') }}

), final as (
    select
        ticket_field_history.*,
        case when ticket_field_history.user_id = -1
            then 'zendesk_auto_change'
            else users.name 
                end as user_name

    from ticket_field_history

    left join users
        using(user_id)
)

select *
from final
