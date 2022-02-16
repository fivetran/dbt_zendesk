# dbt_zendesk v0.8.0
## 🚨 Breaking Changes 🚨
- The logic used to generate the `zendesk__ticket_backlog` model was updated to more accurately map backlog changes to tickets. As the underlying `zendesk__ticket_field_history` model is incremental, we recommend a `--full-refresh` after installing this latest version of the package. ([#61](https://github.com/fivetran/dbt_zendesk/pull/61))
## Features
- Addition of the [DECISIONLOG.md](https://github.com/fivetran/dbt_zendesk/blob/main/DECISIONLOG.md). This file contains detailed explanations for the opinionated transformation logic found within this dbt package. ([#59](https://github.com/fivetran/dbt_zendesk/pull/59))
## Bug Fixes
- Added logic required to account for the `first_reply_time` when the first commenter is an internal comment and there are no previous external comments applied to the ticket. ([#59](https://github.com/fivetran/dbt_zendesk/pull/59))
- For those using schedules, incorporates Daylight Savings Time to use the proper timezone offsets for calculating UTC timestamps. Business minute metrics are more accurately calculated, as previously the package did not acknowledge daylight time and only used the standard time offsets ([#62](https://github.com/fivetran/dbt_zendesk/issues/62)).

## Under the Hood
- Updated the incremental logic within `int_zendesk__field_history_scd` to include an additional partition for `ticket_id`. This allows for a more accurate generation of ticket backlog records. ([#61](https://github.com/fivetran/dbt_zendesk/pull/61))
- Corrected the spelling of the partition field within the cte in `int_zendesk__field_history_scd` to be `partition` opposed to `patition`. ([#61](https://github.com/fivetran/dbt_zendesk/pull/61))
# dbt_zendesk v0.8.0-b1
🎉 dbt v1.0.0 Compatibility Pre Release 🎉 An official dbt v1.0.0 compatible version of the package will be released once existing feature/bug PRs are merged.
## 🚨 Breaking Changes 🚨
- Adjusts the `require-dbt-version` to now be within the range [">=1.0.0", "<2.0.0"]. Additionally, the package has been updated for dbt v1.0.0 compatibility. If you are using a dbt version <1.0.0, you will need to upgrade in order to leverage the latest version of the package.
  - For help upgrading your package, I recommend reviewing this GitHub repo's Release Notes on what changes have been implemented since your last upgrade.
  - For help upgrading your dbt project to dbt v1.0.0, I recommend reviewing dbt-labs [upgrading to 1.0.0 docs](https://docs.getdbt.com/docs/guides/migration-guide/upgrading-to-1-0-0) for more details on what changes must be made.
- Upgrades the package dependency to refer to the latest `dbt_zendesk_source`. Additionally, the latest `dbt_zendesk_source` package has a dependency on the latest `dbt_fivetran_utils`. Further, the latest `dbt_fivetran_utils` package also has a dependency on `dbt_utils` [">=0.8.0", "<0.9.0"].
  - Please note, if you are installing a version of `dbt_utils` in your `packages.yml` that is not in the range above then you will encounter a package dependency error.

# dbt_zendesk v0.7.1
## Fixes
- Updated logic within `int_zendesk__sla_policy_applied` to more accurately reflect the `sla_applied_at` time for `first_reply_time` sla's. Per [Zendesk's documentation](https://support.zendesk.com/hc/en-us/articles/4408821871642-Understanding-ticket-reply-time) the `first_reply_time` sla is set at the creation of the ticket, even if the sla is applied after creation. ([#52](https://github.com/fivetran/dbt_zendesk/pull/52))

- It was found that `first_reply_time` Zendesk SLA policies can be modified after they are set if the priority of the ticket changes. As such, this resulted in the package providing multiple `first_reply_time` sla records in the final `zendesk__sla_policies` output model. As such, now **only** the latest `first_reply_time` sla is provided in the final output model. ([#52](https://github.com/fivetran/dbt_zendesk/pull/52))

## Under the Hood
- Redshift recently included `pivot` as a reserved word within the warehouse. As such, the `pivot` CTE within the `int_zendesk__field_history_pivot` model has been changed to `pivots` to avoid the Redshift error. ([#57](https://github.com/fivetran/dbt_zendesk/pull/57/files))

## Contributors
- @jackiexsun ([#52](https://github.com/fivetran/dbt_zendesk/pull/52))

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
