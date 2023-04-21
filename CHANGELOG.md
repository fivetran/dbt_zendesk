# dbt_zendesk v0.UPDATE.UPDATE

 ## Under the Hood:

- Incorporated the new `fivetran_utils.drop_schemas_automation` macro into the end of each Buildkite integration test job.
- Updated the pull request [templates](/.github).
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
