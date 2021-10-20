# dbt_zendesk v0.7.0

## 🚨 Breaking Changes
- Fix incremental logic bug introduced in v0.5.0 which caused the `zendesk__ticket_field_history` model to not be properly incrementally updated. ([#44](https://github.com/fivetran/dbt_zendesk/pull/44))
  - The above fix resulted in the removal of the `valid_from` and `valid_to` fields in the final model.

## Bug Fixes
- Incremental bug fix noted in the `Breaking Changes` section of the changelog.
- Updated the logic used to calculate `first_reply_time_calendar_minutes` and `first_reply_time_business_minutes` to include first comments made by agents and find the time difference from the first public agent and the ticket created date. This was updated to better align with Zendesk's [First Reply Time](https://support.zendesk.com/hc/en-us/articles/360022182114#topic_zlf_slp_4y) metric definition. ([#50](https://github.com/fivetran/dbt_zendesk/pull/50)) 
- Fixed the comment metric reference for the `total_agent_replies` within `zendesk__ticket_metrics` to accurately map to the `count_agent_comments` metric (showing all public and non-public comments made by agents) opposed to the `count_internal_comments` (only non-public comments) metric. ([#50](https://github.com/fivetran/dbt_zendesk/pull/50))

## Features
- Add the number of ticket handoffs metric as `count_ticket_handoffs` to the `zendesk__ticket_metrics` model which is a distinct count of all internal users who have touched/commented on the ticket. ([#42](https://github.com/fivetran/dbt_zendesk/pull/42))
- Ticket field history calendar limit variables ([#47](https://github.com/fivetran/dbt_zendesk/pull/47)): 
  - Added `ticket_field_history_timeframe_years` variable to limit the ticket field history model to X number of years (default is 50).
  - Limited by default the last ticket calendar date as it's close date. This highly reduces the query cost of the `zendesk__ticket_field_history` query and takes advantage of the Zendesk functionality of not being able to change a ticket after the close date.
  - Added `ticket_field_history_extension_months` variable to extend field history past Y months from ticket close (for reporting purposes).
  - Refer to the README for more details.

## Under the Hood
- Better Postgres incremental strategy within the `zendesk__ticket_field_history` model to reflect more recent incremental strategies. Similar to the strategy taken in [jira__daily_issue_field_history](https://github.com/fivetran/dbt_jira/blob/master/models/jira__daily_issue_field_history.sql). ([#44](https://github.com/fivetran/dbt_zendesk/pull/44))

## Contributors
- [csaroff](https://github.com/csaroff) ([#47](https://github.com/fivetran/dbt_zendesk/pull/47))
- [jackiexsun](https://github.com/jackiexsun) ([#42](https://github.com/fivetran/dbt_zendesk/pull/42))
- [emiliedecherney](https://github.com/emiliedecherney) ([#50](https://github.com/fivetran/dbt_zendesk/pull/50))
- [gareginordyan](https://github.com/gareginordyan) ([#44](https://github.com/fivetran/dbt_zendesk/pull/44))