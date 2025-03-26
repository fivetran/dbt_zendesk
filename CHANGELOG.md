# dbt_zendesk version.version

## Documentation
- Added Quickstart model counts to README. ([#183](https://github.com/fivetran/dbt_zendesk/pull/183))
- Corrected references to connectors and connections in the README. ([#183](https://github.com/fivetran/dbt_zendesk/pull/183))

# dbt_zendesk v0.20.0

## [Upstream Under-the-Hood Updates](https://github.com/fivetran/dbt_zendesk_source/releases/tag/v0.15.0)
- (Affects Redshift only) Updates the `union_zendesk_connections` macro to use a limit 1 instead of limit 0 for empty tables.
  - When a table is empty, Redshift ignores explicit data casts and will materialize every column as a `varchar`. Redshift users may experience errors in downstream transformations as a consequence.
  - For each staging model, if the source table is not found, the package will create a empty table with 0 rows for non-Redshift warehouses and a table with 1 all-`null` row for Redshift destinations. The 1 row will ensure that Redshift will respect the package's datatype casts.

## Documentation Update
- Moved badges at top of the README below the H1 header to be consistent with popular README formats.

# dbt_zendesk v0.19.3
[PR #185](https://github.com/fivetran/dbt_zendesk/pull/185) includes the following updates:

## Quickstart Update
- Removed `organization` from `using_organization_tags` configuration to maintain a one-to-one mapping between a Quickstart source table and whether its corresponding variable in the models should be enabled or disabled.

## Under The Hood
- Updated the `packages.yml` to point to the [latest release](https://github.com/fivetran/dbt_zendesk_source/releases/tag/v0.14.2) of the `dbt_zendesk_source` package.

# dbt_zendesk v0.19.2
[PR #181](https://github.com/fivetran/dbt_zendesk/pull/181) includes the following updates:

## New Features
- Introduced new config variables `using_brands` and `using_organizations` to allow  customers to enable and disable `brand` and `organization` source data, resulting in potential downstream impacts:
- End models that could be impacted by these variables include: 
  - `zendesk__ticket_backlog`: `using_brands` and/or `using_organizations` can be set to `false` to disable `brand` and/or `organization` fields.
  - `zendesk__ticket_enriched`: `using_organizations` can be set to `false` to remove `organization` fields from the final model.
- Intermediate models that could be impacted by these variables include:
  - `int_zendesk__organization_aggregates`: `using_organizations` can be utilized to completely disable the model.
  - `int_zendesk__ticket_aggregates`: `using_brands` can be set to `false` to remove `brand` fields from the final model.
  - `int_zendesk__updater_information`: `using_organizations` can be set to `false` to remove `organization` fields from the final model.

## Under the Hood
- Updated `table_variables` in the `quickstart.yml` with the new `brand` and `organization` tables.
- Updated our Buildkite model run script to ensure we test for when `using_brands` and `using_organizations` is set to either true or false. 

## Documentation Updates
- [Updated README](https://github.com/fivetran/dbt_zendesk?tab=readme-ov-file#step-4-enabledisable-models-for-non-existent-sources) with instructions on how to disable `brand` and `organization` sources. 

# dbt_zendesk v0.19.1
[PR #180](https://github.com/fivetran/dbt_zendesk/pull/180) includes the following update:

## Bug Fixes
- In v0.19.0, Snowflake users may have seen `when searching for a relation, dbt found an approximate match` errors when running the `stg_zendesk__group_tmp` model. We have updated the `adapter.get_relation()` [logic](https://github.com/fivetran/dbt_zendesk_source/blob/main/macros/union/union_zendesk_connections.sql) that was causing this error in the `zendesk_source` [package](https://github.com/fivetran/dbt_zendesk_source) (see source package [release](https://github.com/fivetran/dbt_zendesk_source/releases/tag/v0.14.1)).

# dbt_zendesk v0.19.0
[PR #178](https://github.com/fivetran/dbt_zendesk/pull/178) includes the following updates:

## Feature Update: Run Models on Muliple Zendesk Sources
- This release supports running the package on multiple Zendesk sources at once! See the [README](https://github.com/fivetran/dbt_zendesk?tab=readme-ov-file#step-3-define-database-and-schema-variables) for details on how to leverage this feature.

> Please note: This is a **Breaking Change** in that we have a added a new field, `source_relation`, that points to the source connector from which the record originated. This field addition will require a `dbt run --full-refresh`, even if you are not using this new functionality.

## Documentation
- Cleaned up the column-level documentation descriptions for the `zendesk__ticket_enriched` and `zendesk__ticket_metrics` models.

## Under the Hood
- Relevant to package maintainers only:
  - Added a consistency data validation test for each end model.
  - Added `consistency_test_exclude_fields` to ignore in consistency tests. These are largely timestamp fields that can differ slightly due to different runtimes, but `source_relation` is also currently included due to the nature of this update.
  - Filtered out records made or updated today from consistency tests to avoid false positive failures due to different runtimes.
  - Incorporated `source_relation` into each validation test.

# dbt_zendesk v0.18.1
[PR #174](https://github.com/fivetran/dbt_zendesk/pull/174) includes the following changes:

## Bug Fix
- Addressed an issue in which some records in `zendesk__sla_policies` might erroneously have a null `sla_policy_name` due to system-generated millisecond-long gaps in timestamps. The package now compares timestamps to the nearest `second` when selecting valid SLA policy names in [int_zendesk__sla_policy_applied](https://github.com/fivetran/dbt_zendesk/blob/main/models/sla_policy/int_zendesk__sla_policy_applied.sql).

## Under the Hood
- Updated `consistency_sla_policies` and `sla_count_match` data validation tests to account for the above change.

# dbt_zendesk v0.18.0
[PR #171](https://github.com/fivetran/dbt_zendesk/pull/171) includes the following changes:

## Breaking Changes (Full refresh required after upgrading)
### Schedule Change Support
- Support for schedule changes has been added. This feature is disabled by default since most users do not sync the required source `audit_table`. To enable this feature set the variable `using_schedule_histories` to `true` in your `dbt_project.yml`:
```yml
vars:
  using_schedule_histories: true
```
  - Schedule changes can now be extracted directly from the audit log, providing a view of schedule modifications over time.
  - The `int_zendesk__schedule_spine` model is now able to incorporate these schedule changes, making it possible for downstream models to reflect the most up-to-date schedule data.
    - Note this is only in effect when `using_schedule_histories` is true.
  - This improves granularity for Zendesk metrics related to agent availability, SLA tracking, and time-based performance analysis.
#### dbt_zendesk_source changes (see the [Release Notes](https://github.com/fivetran/dbt_zendesk_source/releases/tag/v0.13.0) for more details)
- Introduced the `stg_zendesk__audit_log` table for capturing schedule changes from Zendesk's audit log.
  - This model is disabled by default, to enable it set variable `using_schedule_histories` to `true` in `dbt_project.yml`.

## New Features
- Holiday support: Users can now choose to disable holiday tracking, while continuing to use schedules, by setting variable `using_holidays` to `false` in `dbt_project.yml`.
- New intermediate models have been introduced to streamline both the readability and maintainability:
  - [`int_zendesk__timezone_daylight`](https://github.com/fivetran/dbt_zendesk/blob/main/models/utils/int_zendesk__timezone_daylight.sql): A utility model that maintains a record of daylight savings adjustments for each time zone.
    - materialization: ephemeral
  - [`int_zendesk__schedule_history`](https://github.com/fivetran/dbt_zendesk/blob/main/models/intermediate/int_zendesk__schedule_history.sql): Captures a full history of schedule changes for each `schedule_id`.
    - materialization: table (if enabled)
  - [`int_zendesk__schedule_timezones`](https://github.com/fivetran/dbt_zendesk/blob/main/models/intermediate/int_zendesk__schedule_timezones.sql): Merges schedule history with time zone shifts.
    - materialization: ephemeral
  - [`int_zendesk__schedule_holiday`](https://github.com/fivetran/dbt_zendesk/blob/main/models/intermediate/int_zendesk__schedule_holiday.sql): Identifies and calculates holiday periods for each schedule.
    - materialization: ephemeral
- Rebuilt logic in [`int_zendesk__schedule_spine`](https://github.com/fivetran/dbt_zendesk/blob/main/models/intermediate/int_zendesk__schedule_spine.sql) to consolidate updates from the new intermediate models.
#### dbt_zendesk_source changes (see the [Release Notes](https://github.com/fivetran/dbt_zendesk_source/releases/tag/v0.13.0) for more details)
- Updated the `stg_zendesk__schedule_holidays` model to allow users to disable holiday processing by setting variable `using_holidays` to `false`.

## Bug Fixes
- Resolved a bug in the `int_zendesk__schedule_spine` model where users experienced large gaps in non-holiday periods. The updated logic addresses this issue.

## Decision log
- Added the following [DECISIONLOG](https://github.com/fivetran/dbt_zendesk/blob/main/DECISIONLOG.md) entries:
  - Entry addressing how multiple schedule changes in a single day are handled. Only the last change of the day is captured to align with day-based downstream logic.
  - Entry to clarify backfilling of schedule history. The most recent schedule is sourced from `stg_zendesk__schedule`, while historical changes are managed separately, allowing users to disable the history feature if needed.

## Under the Hood
- Replaced instances of `dbt.date_trunc` with `dbt_date.week_start` to standardize week start dates to Sunday across all warehouses, since our schedule logic relies on consistent weeks.
- Replaced the deprecated `dbt.current_timestamp_backcompat()` function with `dbt.current_timestamp()` to ensure all timestamps are captured in UTC.
- Added seed data for `audit_log` to enhance integration testing capabilities.
- Introduced new helper macros, `clean_data` and `regex_extract`, to process complex text of the schedule changes extracted from audit logs.
- Updated `int_zendesk__calendar_spine` logic to prevent errors during compilation before the first full run, ensuring a smoother development experience.

# dbt_zendesk v0.17.0
## New model ([#161](https://github.com/fivetran/dbt_zendesk/pull/161))
- Addition of the `zendesk__document` model, designed to structure Zendesk textual data for vectorization and integration into NLP workflows. The model outputs a table with:
  - `document_id`: Corresponding to the `ticket_id`
  - `chunk_index`: For text segmentation
  - `chunk`: The text chunk itself
  - `chunk_tokens_approximate`: Approximate token count for each segment
- This model is currently disabled by default. You may enable it by setting the `zendesk__unstructured_enabled` variable as `true` in your `dbt_project.yml`.
  - This model was developed with the limit of chunk sizes to approximately 5000 tokens for use with OpenAI, however you can change this limit by setting the variable `zendesk_max_tokens` in your `dbt_project.yml`.
  - See the README section [Enabling the unstructured document model for NLP](https://github.com/fivetran/dbt_zendesk/blob/main/README.md#enabling-the-unstructured-document-model-for-nlp) for more information.

## Breaking Changes (Full refresh required after upgrading)
- Incremental models running on BigQuery have had the `partition_by` logic adjusted to include a granularity of a month. This change only impacts BigQuery warehouses and was applied to avoid the common `too many partitions` error some users have experienced when partitioning by day. Therefore, adjusting the partition to a month granularity will decrease the number of partitions created and allow for more performant querying and incremental loads. This change was applied to the following models ([#165](https://github.com/fivetran/dbt_zendesk/pull/165)):
  - `int_zendesk__field_calendar_spine`
  - `int_zendesk__field_history_pivot`
  - `zendesk__ticket_field_history`

- In the [dbt_zendesk_source v0.12.0 release](https://github.com/fivetran/dbt_zendesk_source/releases/tag/v0.12.0), the field `_fivetran_deleted` was added to the following models for use in `zendesk__document` model ([#161](https://github.com/fivetran/dbt_zendesk/pull/161)):
  - `stg_zendesk__ticket`
  - `stg_zendesk__ticket_comment`
  - `stg_zendesk__user`
  - If you have already added `_fivetran_deleted` as a passthrough column via the `zendesk__ticket_passthrough_columns` or `zendesk__user_passthrough_columns` variable, you will need to remove or alias this field from the variable to avoid duplicate column errors.

## Bug Fixes
- Fixed an issue in the `zendesk__sla_policies` model where tickets that were opened and solved outside of scheduled hours were not being reported, specifically for the metrics `requester_wait_time` and `agent_work_time`. 
  - Resolved by adjusting the join logic in models `int_zendesk__agent_work_time_business_hours` and `int_zendesk__requester_wait_time_business_hours`. ([#164](https://github.com/fivetran/dbt_zendesk/pull/164), [#156](https://github.com/fivetran/dbt_zendesk/pull/156))
- Fixed an issue in the `zendesk__ticket_metrics` model where certain tickets had miscalculated metrics. 
  - Resolved by adjusting the join logic in models `int_zendesk__ticket_work_time_business`, `int_zendesk__ticket_first_resolution_time_business`, and `int_zendesk__ticket_full_resolution_time_business`. ([#167](https://github.com/fivetran/dbt_zendesk/pull/167))

## Under the hood
- Added integrity validations:
  - Test to ensure `zendesk__sla_policies` and `zendesk__ticket_metrics` models produce consistent time results. ([#164](https://github.com/fivetran/dbt_zendesk/pull/164))
  - Test to ensure `zendesk__ticket_metrics` contains all the tickets found in `stg_zendesk__ticket`. ([#167](https://github.com/fivetran/dbt_zendesk/pull/167))
- Modified the `consistency_sla_policy_count` validation test to group by `ticket_id` for more accurate testing. ([#165](https://github.com/fivetran/dbt_zendesk/pull/165))
- Updated casting in joins from timestamps to dates so that the whole day is considered. This produces more accurate results. ([#164](https://github.com/fivetran/dbt_zendesk/pull/164), [#156](https://github.com/fivetran/dbt_zendesk/pull/156), [#167](https://github.com/fivetran/dbt_zendesk/pull/167))
- Reduced the weeks looking ahead from 208 to 52 to improve performance, as tracking ticket SLAs beyond one year was unnecessary. ([#156](https://github.com/fivetran/dbt_zendesk/pull/156), [#167](https://github.com/fivetran/dbt_zendesk/pull/167))
- Updated seed files to reflect a real world ticket field history update scenario. ([#165](https://github.com/fivetran/dbt_zendesk/pull/165))

# dbt_zendesk v0.16.0
## 🚨 Minor Upgrade 🚨
Although this update is not a breaking change, it will likely impact the output of the `zendesk__sla_policies` and `zendesk__sla_metrics` models. [PR #154](https://github.com/fivetran/dbt_zendesk/pull/154) includes the following changes:

## Bug Fixes
- Addresses the potential issue where the `first_reply_time_business_minutes` metric within the `zendesk__ticket_metrics` model would incorrectly calculate the elapsed time when daylight savings occurred. This change involved adjusting a join to reference the difference of two dates as opposed to timestamps. This more accurately applies a cutoff event during daylight savings.
- Introduction of an additional condition within the `filtered_reply_times` cte of the `int_zendesk__reply_time_combined` model to ensure tickets replied to before any schedule begins and no business minutes have been spent is reserved for **only** the first day the ticket is open. Previously, this condition _could_ be met on days other than the first. This would potentially result in duplicates of `sla_event_id`'s further downstream in the `zendesk__sla_policies` model.

## Under the Hood
- Addition of integrity and consistency validation tests within integration tests for the `zendesk__sla_policies` and `zendesk__ticket_metrics` models.

# dbt_zendesk v0.15.0

## 🚨 Minor Upgrade 🚨
Although this update is not a breaking change, it will significantly impact the output of the `zendesk__sla_policies` model. [PR #146](https://github.com/fivetran/dbt_zendesk/pull/146) includes the following changes:

## Bug Fixes
- Fixes the issue of potential duplicate `sla_event_id`'s occurring in the `zendesk__sla_policies` model.
  - This involved updating the `int_zendesk__schedule_spine` which was previously outputting overlapping schedule windows, to account for when holidays transcended a given schedule week.
  - This also involved updating the `int_zendesk__reply_time_business_hours` model, in which two different versions of a schedule could exist due to daylight savings time.
- Improved performance by adjusting the `int_zendesk__reply_time_business_hours` model to only perform the weeks cartesian join on tickets that require the further look into the future.
    - Previously the `int_zendesk__reply_time_business_hours` would perform a cartesian join on all tickets to calculate weeks into the future. This was required to accurately calculate `sla_elapsed_time` for tickets with first replies far into the future. However, this was only necessary for a handful of tickets. Therefore, this has been adjusted to accurately only calculate the future weeks as far as either the first reply time or first solved time.

## Documentation Updates
- Addition of the reference to the Fivetran prebuilt [Zendesk Streamlit report](https://fivetran-zendesk.streamlit.app/) in the README.
- Updates DECISIONLOG to include a note that the generated time series for ticket SLA policies is limited to a year into the future to maintain performance.

# dbt_zendesk v0.14.0

[PR #136](https://github.com/fivetran/dbt_zendesk/pull/136) includes the following changes:

## Bug Fixes
- Converted the `sla_elapsed_time` metric within the `zendesk__sla_policies` model to be reported in minutes to the second as opposed to just the nearest rounded minute. This ensures more accurate reporting.
- Adjusted the `next_reply_time` SLA elapsed time metric calculation within the `zendesk__sla_policies` model to also take into consideration the ticket solved event as a valid SLA event. Previously if a reply time SLA was attached to a ticket and there was no reply, but the ticket was closed then the SLA would be breached. This update ensures a closed event serves as a route for the SLA to be achieved or breached.
- Updated the `int_zendesk__reply_time_combined` model to additionally account for the following business hour scenarios as they were erroneously being filtered out in previous versions of the package:
   - A ticket is first replied to outside SLA schedules
   - A ticket has not yet received an agent reply
- Overhauled the logic used within the `int_zendesk__reply_time_combined` model to calculate `sla_breach_at` within the `zendesk__sla_policies` and upstream models for reply time SLAs. It was found this field was inconsistent with the actual breach/achieve time of an SLA. The overhaul should now ensure reply time SLA is accurate to either be the time of the SLA breach or achieve event.
  - In particular, for first and next reply time SLAs the `sla_breach_at` will be the time of the breach if the SLA was breached or the time the SLA was achieved if it was not breached.
- Modified the logic that matches schedule weeks within the `int_zendesk__reply_time_combined` model when calculating reply time business metrics. Previously long running SLAs would be excluded from the final model, now all reply time business SLAs regardless of sla elapsed duration will be included in the end `zendesk__sla_policies` model.
- Included additional logic within the `int_zendesk__ticket_schedules` model to more accurately select the **active** default schedule used when calculating the business metrics for the downstream `zendesk__ticket_metrics` and `zendesk__sla_policies` models.
  - Previously the model could possibly select a deleted schedule. This update ensures only an active schedule is selected.
## Documentation Updates
- Updated "Zendesk" references within the README to now refer to "Zendesk Support" in order to more accurately reflect the name of the Fivetran Zendesk Support Connector.
- Added new entries to the DECISIONLOG to highlight nuances and opinionated stances this package uses when calculating business metrics and `first_reply_time` SLAs.

# dbt_zendesk v0.13.1

[PR #128](https://github.com/fivetran/dbt_zendesk/pull/128) includes the following changes:
## Bug Fixes
- The `int_zendesk__schedule_spine` model was updated to properly account for schedules that recognized daylight savings time (DST) at one point in time, and then stopped recognizing it at a later date.
  - For example, the Hong Kong timezone originally recognized DST, but then stopped in 1979. The previous versions of this package only recorded the schedule business hours until 1979. This update addresses this bug.
  - Please note, this update will only effect users leveraging schedules.

## Under the Hood
- Included auto-releaser GitHub Actions workflow to automate future releases.
- Updated the maintainer PR template to resemble the most up to date format.
- Included a `quickstart.yml` file to allow for automated Quickstart data model deployments.

## Contributors
- [@elijamuel](https://github.com/elijamuel) ([Issue #121](https://github.com/fivetran/dbt_zendesk/issues/121))

# dbt_zendesk v0.13.0

## 🚨 Breaking Change (Snowflake users) 🚨
- We have changed our identifier logic in the initial Zendesk source package to account for `group` being both a Snowflake reserved word and a source table. Given `dbt_zendesk_source` is a dependency for this package, Snowflake users will want to execute a `dbt run --full-refresh` before using the new version of the package. [PR #42](https://github.com/fivetran/dbt_zendesk_source/pull/42)

## Bug Fixes
- Updates the `int_zendesk__schedule_spine` model to convert the Holiday schedules into proper UTC values before being used in comparison with the schedule times. This ensures the holidays are properly mapped to schedules regardless of timezones. ([PR #126](https://github.com/fivetran/dbt_zendesk/pull/126))

## 🚀 Feature Updates 🚀
- Added `solve_time_in_calendar_minutes` and `solve_time_in_business_minutes` to our `zendesk__ticket_metrics` model, which calculates calendar and business minutes for when the ticket was in the 'new', 'open', 'hold', or 'pending' status. ([PR #123](https://github.com/fivetran/dbt_zendesk/pull/123))

## 🔎 Under the Hood 🔎 
- Updated the seed files and seed file configurations for the package integration tests to align with changes in  dbt_zendesk_source made in [PR #42](https://github.com/fivetran/dbt_zendesk_source/pull/42) for applying the `dbt_utils.star` macro.
- Corrected the folder structure for the `.github` folder to properly categorize the Community and Maintainer PR templates. ([PR #126](https://github.com/fivetran/dbt_zendesk/pull/126))

## Contributors
- [@fivetran-jessicacherny](https://github.com/fivetran-jessicacherny) ([Issue #119](https://github.com/fivetran/dbt_zendesk/issues/119))
- [@cth84](https://github.com/cth84) ([PR #118](https://github.com/fivetran/dbt_zendesk/pull/118))

# dbt_zendesk v0.12.0

This release includes fixes to issues introduced in v0.11.0-v0.11.1 surrounding the incorporation of schedule holidays.

Special thanks to [@cth84](https://github.com/cth84) and [@nschimmoller](https://github.com/nschimmoller) for working with us to figure out some seriously tricky bugs!

## Bug Fix
- Adjusted the gap-merging logic in `int_zendesk__schedule_spine` to look forward in time instead of backward. This allows the model to take Daylight Savings Time into account when merging gaps. Previously, schedule periods with different `start_time_utc`s (because of DST) were getting merged together ([PR #114](https://github.com/fivetran/dbt_zendesk/pull/114)).
  - Also removed the `double_gap` logic as it was rendered unnecessary by the above change.
- In all of our intermediate business hour models, adjusted the join logic in the `intercepted_periods` CTE, where we associate ticket weekly periods with the appropriate business schedule period. Previously, we did so by comparing the ticket's `status_valid_starting_at` and `status_valid_ending_at` fields to the schedule's `valid_from` and `valid_until` dates. This was causing fanout in certain cases, as we need to take the ticket-status's `week_number` into account because it is part of the grain of the CTE we are joining ([PR #114](https://github.com/fivetran/dbt_zendesk/pull/114)).
- Adjusted the way we calculate the end of holidays in `int_zendesk__schedule_spine`. Previously, we calculated the end of holiday day by adding `24*60*60-1` seconds (making the end the last second of the same day) to the start of the holiday. This previously worked because our downstream joins for calculating business metrics were inclusive (ie `>=` instead of `>`). We've updated these joins to be exclusive (ie `>` or `<`), so we've set the end of the holiday to truly be the end of the day instead of a second prior ([PR #114](https://github.com/fivetran/dbt_zendesk/pull/114)).
- Updated `int_zendesk__requester_wait_time_filtered_statuses` to include the `hold` status, as zendesk updated `on-hold` to just `hold` ([PR #114](https://github.com/fivetran/dbt_zendesk/pull/114)).
- Updates the logic in `int_zendesk__reply_time_combined` to bring through the correct `sla_event_id` records to the end `zendesk__sla_policies` model. ([PR #108](https://github.com/fivetran/dbt_zendesk/issues/108))
   - Originally, duplicate `sla_event_id` records were being persisted because the upstream `filtered_reply_times` CTE did not include for all scenarios. With this update, the CTE will filter for the following scenarios:
       - Ticket is replied to between a schedule window
       - Ticket is replied to before a schedule window and no business minutes have been spent on it
       - Ticket is not replied to and therefore active. But only bring through the active SLA record that is most recent (after the last SLA schedule starts but before the next)
- Updated the ordering within the `int_zendesk__comments_enriched` model logic to also take into account when two comments are posted at the exact same time. Previously, the next comment would be picked arbitrarily. However, we now use the `commenter_role` as the tie breaker giving preference to the `end-user` as they will likely be the first commenter when two comments are posted at the exact same time. ([PR #114](https://github.com/fivetran/dbt_zendesk/pull/114))
- Modified the requester and agent wait time `sla_elapsed_time` metric calculations within the `zendesk__sla_policies` to capture the max `running_total_scheduled_minutes` record as opposed to the cumulative sum. Max more accurately represents the upstream data as it is presented in a rolling sum in the previous intermediate models. ([PR #114](https://github.com/fivetran/dbt_zendesk/pull/114))

## Dependency Updates
- The `dbt-date` dependency has been updated to reflect the recommended latest range, [">=0.9.0", "<1.0.0"]. This will help to avoid upstream dependency conflicts. ([PR #113](https://github.com/fivetran/dbt_zendesk/pull/113))

## Contributors: 
- [@nschimmoller](https://github.com/nschimmoller) ([#108](https://github.com/fivetran/dbt_zendesk/issues/108))
- [@cth84](https://github.com/cth84) ([#107](https://github.com/fivetran/dbt_zendesk/issues/107))

# dbt_zendesk v0.11.2

## Rollback
This [PR #110](https://github.com/fivetran/dbt_zendesk/pull/110/files) is a rollback to v0.10.2. We are seeing issues in business minutes and SLA duplicate records following the v0.11.0 release.

# dbt_zendesk v0.11.1

Tiny release ahead!
## Under the Hood:
- Removes whitespace-escaping from Jinja code in `int_zendesk__field_history_scd`. In different whitepace parsing environments, this can jumble code up with SQL comments ([PR #106](https://github.com/fivetran/dbt_zendesk/pull/106)).

## Contributors: 
- [@bcolbert978](https://github.com/bcolbert978) ([PR #106](https://github.com/fivetran/dbt_zendesk/pull/106))

# dbt_zendesk v0.11.0

## Update: There have been bugs identified in this release and we have rolled back this package to v0.10.2 in the v0.11.2 release.

## Feature Updates:
- Added support of the new `schedule_holiday` table in the `schedule_spine` intermediate model in order to properly capture how holidays impact ticket schedules and their respective SLAs. ([PR #98](https://github.com/fivetran/dbt_zendesk/pull/98))
- Made relevant downstream changes within the following models to capture proper business hour metrics when taking into account holiday schedules: ([PR #98](https://github.com/fivetran/dbt_zendesk/pull/98))
  - `int_zendesk__agent_work_time_business_hours`
  - `int_zendesk__reply_time_business_hours`
  - `int_zendesk__reply_time_combined`
  - `int_zendesk__requester_wait_time_business_hours`
  - `zendesk__sla_policies`
- Added `open_status_duration_in_business_minutes` and `new_status_duration_in_business_minutes` columns to the `int_zendesk__ticket_work_time_business` and `zendesk__ticket_metrics` models. These are counterparts to the already existing `open_status_duration_in_calendar_minutes` and `new_status_duration_in_calendar_minutes` columns. ([PR #97](https://github.com/fivetran/dbt_zendesk/pull/97)) 

## Fixes:
- Added coalesce to `0` statements to the following fields in the `zendesk__ticket_metrics` model. This is necessary as some tickets may have responses entirely outside of business hours which will not count towards business minute metrics. As such, a coalesce to `0` is more representative to the metric as opposed to a `null` record: ([PR #103](https://github.com/fivetran/dbt_zendesk/pull/103))
  - `first_resolution_business_minutes`
  - `full_resolution_business_minutes`
  - `first_reply_time_business_minutes`
  - `agent_wait_time_in_business_minutes`
  - `requester_wait_time_in_business_minutes`
  - `agent_work_time_in_business_minutes`
  - `on_hold_time_in_business_minutes`
- Fixed the `total_agent_replies` field in `zendesk__ticket_metrics` so the value is derived from public agent comments logic, and also ignores ticket creation comments from an agent, matching the Zendesk definition. ([PR #102](https://github.com/fivetran/dbt_zendesk/pull/102))

## Under the Hood:
- Leveraged `dbt_date.week_start` in place of `dbt.date_trunc` for business hour metrics to more consistently capture the start of the week across warehouses. ([PR #98](https://github.com/fivetran/dbt_zendesk/pull/98))
- Start of the week is now consistently set to Sunday. ([PR #98](https://github.com/fivetran/dbt_zendesk/pull/98))
- Incorporated the new `fivetran_utils.drop_schemas_automation` macro into the end of each Buildkite integration test job. ([PR #98](https://github.com/fivetran/dbt_zendesk/pull/98))
- Updated the pull request templates. ([PR #98](https://github.com/fivetran/dbt_zendesk/pull/98))

## Contributors:
- [@Tim-Hoare](https://github.com/Tim-Hoare) ([PR #97](https://github.com/fivetran/dbt_zendesk/pull/97)) 

# dbt_zendesk v0.10.2
[PR #101](https://github.com/fivetran/dbt_zendesk/pull/101) includes the following updates:
## Fixes
- Updated the `group` variable in the `dbt_project.yml` to have properly closed quotes within the variable declaration.
- Adjusted the `in_zendesk__calendar_spine` to set the return result of `dbt.current_timestamp_backcompat()` as a variable. This ensures that when the variable is being called within the model it can properly establish a dependency within the manifest.


# dbt_zendesk v0.10.1
## Bug Fixes
- Modified the `int_zendesk__ticket_schedules` model to have the execute statement reference the source `schedule` table as opposed to the `stg_zendesk__schedule` model so the package may successfully compile before being run for the first time. ([#90](https://github.com/fivetran/dbt_zendesk/pull/90))

# dbt_zendesk v0.10.0

## 🚨 Breaking Changes 🚨:
[PR #81](https://github.com/fivetran/dbt_zendesk/pull/81) includes the following breaking changes:
- Dispatch update for dbt-utils to dbt-core cross-db macros migration. Specifically `{{ dbt_utils.<macro> }}` have been updated to `{{ dbt.<macro> }}` for the below macros:
    - `any_value`
    - `bool_or`
    - `cast_bool_to_text`
    - `concat`
    - `date_trunc`
    - `dateadd`
    - `datediff`
    - `escape_single_quotes`
    - `except`
    - `hash`
    - `intersect`
    - `last_day`
    - `length`
    - `listagg`
    - `position`
    - `replace`
    - `right`
    - `safe_cast`
    - `split_part`
    - `string_literal`
    - `type_bigint`
    - `type_float`
    - `type_int`
    - `type_numeric`
    - `type_string`
    - `type_timestamp`
    - `array_append`
    - `array_concat`
    - `array_construct`
- For `current_timestamp` and `current_timestamp_in_utc` macros, the dispatch AND the macro names have been updated to the below, respectively:
    - `dbt.current_timestamp_backcompat`
    - `dbt.current_timestamp_in_utc_backcompat`
- `dbt_utils.surrogate_key` has also been updated to `dbt_utils.generate_surrogate_key`. Since the method for creating surrogate keys differ, we suggest all users do a `full-refresh` for the most accurate data. For more information, please refer to dbt-utils [release notes](https://github.com/dbt-labs/dbt-utils/releases) for this update.
- Dependencies on `fivetran/fivetran_utils` have been upgraded, previously `[">=0.3.0", "<0.4.0"]` now `[">=0.4.0", "<0.5.0"]`.

# dbt_zendesk v0.9.1
## Bugfix: 
- If doing a _dbt_compile_ prior to _dbt_run_, it fails at `int_zendesk__calendar_spine` because the staging model it references is not built yet. This PR changes the intermediate models to reference source tables instead of staging models. ([#79](https://github.com/fivetran/dbt_zendesk/pull/79))
## Contributors
- [@fbertsch](https://github.com/fbertsch) ([#71](https://github.com/fivetran/dbt_zendesk/issues/71))

# dbt_zendesk v0.9.0
🚨 This includes Breaking Changes! 🚨

## 🎉 Documentation and Feature Updates
- Databricks compatibility 🧱 ([#74](https://github.com/fivetran/dbt_zendesk/pull/74)).
- Updated README documentation updates for easier navigation and setup of the dbt package ([#73](https://github.com/fivetran/dbt_zendesk/pull/73)).
- Added `zendesk_[source_table_name]_identifier` variables to allow for easier flexibility of the package to refer to source tables with different names ([#73](https://github.com/fivetran/dbt_zendesk/pull/73)).
- By default, this package now builds the Zendesk staging models within a schema titled (`<target_schema>` + `_zendesk_source`) in your target database. This was previously `<target_schema>` + `_zendesk_staging`, but we have changed it to maintain consistency with our other packges. See the README for instructions on how to configure the build schema differently. 

## Under the Hood
- Swapped references to the `fivetran_utils.timestamp_diff` macro with `dbt_utils.datediff` macro. The dbt-utils macro previously did not support Redshift.

# dbt_zendesk v0.8.4
## Bug Fix
- Quick fix on missing logic in the case statement for determining multi-touch resolution metrics.
## Contributors
- @tonytusharjr ([#7](https://github.com/fivetran/dbt_zendesk/pull/74)).
# dbt_zendesk v0.8.3
## Features
- This [Zendesk Source package](https://github.com/fivetran/dbt_zendesk_source) now allows for custom fields to be added to the `stg_zendesk__ticket` model. These custom fields will also persist downstream to the `zendesk__ticket_enriched` and `zendesk__ticket_metrics` models. You may now add your own customer fields to these models by leveraging the `zendesk__ticket_passthrough_columns` variable. ([#70](https://github.com/fivetran/dbt_zendesk/pull/70))
# dbt_zendesk v0.8.2
## Fixes
- It was brought to our attention that the `dbt_utils.date_trunc` macro only leverages the default arguments of the date_trunc function in the various warehouses. For example, `date_trunc` in Snowflake for the `week` argument produces the starting Monday, while BigQuery produces the starting Sunday. For this package, we want to leverage the start of the week as Sunday. Therefore, logic within the business metric intermediate models has been adjusted to capture the start of the week as Sunday. This was done by leveraging the `week_start` macro within the `dbt-date` package. ([#68](https://github.com/fivetran/dbt_zendesk/pull/68))
# dbt_zendesk v0.8.1
## Fixes
- The `0.7.1` release of the zendesk package introduced a bug within the `zendesk__sla_policy` model that caused duplicate sla records via a [join condition](https://github.com/fivetran/dbt_zendesk/blob/v0.7.1/models/sla_policy/int_zendesk__sla_policy_applied.sql#L48). This join condition has been modified to leverage the more accurate `sla_policy_applied.valid_starting_at` field instead of the `sla_policy_applied.sla_applied_at` which changes for `first_reply_time` slas. ([#67](https://github.com/fivetran/dbt_zendesk/pull/67))
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
