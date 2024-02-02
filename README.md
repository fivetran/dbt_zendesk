<p align="center">
    <a alt="License"
        href="https://github.com/fivetran/dbt_zendesk/blob/main/LICENSE">
        <img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg" /></a>
    <a alt="dbt-core">
        <img src="https://img.shields.io/badge/dbt_Core™_version->=1.3.0_,<2.0.0-orange.svg" /></a>
    <a alt="Maintained?">
        <img src="https://img.shields.io/badge/Maintained%3F-yes-green.svg" /></a>
    <a alt="PRs">
        <img src="https://img.shields.io/badge/Contributions-welcome-blueviolet" /></a>
    <a alt="Fivetran Quickstart Compatible"
        href="https://fivetran.com/docs/transformations/dbt/quickstart">
        <img src="https://img.shields.io/badge/Fivetran_Quickstart_Compatible%3F-yes-green.svg" /></a>
</p>

# Zendesk Modeling dbt Package ([Docs](https://fivetran.github.io/dbt_zendesk/))
# 📣 What does this dbt package do?
- Produces modeled tables that leverage Zendesk data from [Fivetran's connector](https://fivetran.com/docs/applications/zendesk) in the format described by [this ERD](https://fivetran.com/docs/applications/zendesk#schemainformation) and build off the output of our [zendesk source package](https://github.com/fivetran/dbt_zendesk_source).
- Enables you to better understand the performance of your Support team. It calculates metrics focused on response times, resolution times, and work times for you to analyze. It performs the following actions:
  - Creates an enriched ticket model with relevant resolution, response time, and other metrics
  - Produces a historical ticket field history model to see velocity of your tickets over time
  - Converts metrics to business hours for Zendesk Professional or Enterprise users
  - Calculates SLA policy breaches for Zendesk Professional or Enterprise users
- Generates a comprehensive data dictionary of your source and modeled Zendesk data through the [dbt docs site](https://fivetran.github.io/dbt_zendesk/).

<!--section="zendesk_transformation_model"-->
The following table provides a detailed list of final models materialized within this package by default. 
> TIP: See more details about these models in the package's [dbt docs site](https://fivetran.github.io/dbt_zendesk/#!/overview?g_v=1).

| **model**                    | **description**                                                                                                                                                 |
| ---------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [zendesk__reply_metrics](https://fivetran.github.io/dbt_zendesk/#!/model/model.zendesk.zendesk__reply_metrics)       | Each record represents a communication between an end user and a responding agent, enriched with metrics about reply times.  Calendar and business hours are supported.  |
| [zendesk__ticket_metrics](https://fivetran.github.io/dbt_zendesk/#!/model/model.zendesk.zendesk__ticket_metrics)       | Each record represents a Zendesk ticket, enriched with metrics about reply times, resolution times, and work times.  Calendar and business hours are supported.  |
| [zendesk__ticket_enriched](https://fivetran.github.io/dbt_zendesk/#!/model/model.zendesk.zendesk__ticket_enriched)      | Each record represents a Zendesk ticket, enriched with data about its tags, assignees, requester, submitter, organization, and group.                           |
| [zendesk__ticket_summary](https://fivetran.github.io/dbt_zendesk/#!/model/model.zendesk.zendesk__ticket_summary)           | A single record table containing Zendesk ticket and user summary metrics.                                                              |
| [zendesk__ticket_backlog](https://fivetran.github.io/dbt_zendesk/#!/model/model.zendesk.zendesk__ticket_backlog)           | A daily historical view of the ticket field values defined in the `ticket_field_history_columns` variable for all backlog tickets. Backlog tickets being defined as any ticket not in a 'closed', 'deleted', or 'solved' status.                                                             |
| [zendesk__ticket_field_history](https://fivetran.github.io/dbt_zendesk/#!/model/model.zendesk.zendesk__ticket_field_history) | A daily historical view of the ticket field values defined in the `ticket_field_history_columns` variable and the corresponding updater fields defined in the `ticket_field_history_updater_columns` variable.                                                        |
| [zendesk__sla_policies](https://fivetran.github.io/dbt_zendesk/#!/model/model.zendesk.zendesk__sla_policies)           | Each record represents an SLA policy event and additional sla breach and achievement metrics. Calendar and business hour SLA breaches are supported.    
<!--section-end-->

# 🎯 How do I use the dbt package?

## Step 1: Prerequisites
To use this dbt package, you must have the following:

- At least one Fivetran zendesk connector syncing data into your destination.
- A **BigQuery**, **Snowflake**, **Redshift**, **PostgreSQL**, or **Databricks** destination.

### Databricks Dispatch Configuration
If you are using a Databricks destination with this package you will need to add the below (or a variation of the below) dispatch configuration within your `dbt_project.yml`. This is required in order for the package to accurately search for macros within the `dbt-labs/spark_utils` then the `dbt-labs/dbt_utils` packages respectively.
```yml
dispatch:
  - macro_namespace: dbt_utils
    search_order: ['spark_utils', 'dbt_utils']
```

## Step 2: Install the package
Include the following zendesk package version in your `packages.yml` file:
> TIP: Check [dbt Hub](https://hub.getdbt.com/) for the latest installation instructions or [read the dbt docs](https://docs.getdbt.com/docs/package-management) for more information on installing packages.
```yml
packages:
  - package: fivetran/zendesk
    version: [">=0.13.0", "<0.14.0"]

```
> **Note**: Do not include the Zendesk source package. The Zendesk transform package already has a dependency on the source in its own `packages.yml` file.

## Step 3: Define database and schema variables
By default, this package runs using your destination and the `zendesk` schema. If this is not where your zendesk data is (for example, if your zendesk schema is named `zendesk_fivetran`), update the following variables in your root `dbt_project.yml` file accordingly:

```yml
vars:
    zendesk_database: your_destination_name
    zendesk_schema: your_schema_name 
```

## Step 4: Disable models for non-existent sources
This package takes into consideration that not every Zendesk account utilizes the `schedule`, `schedule_holiday`, `ticket_schedule` `daylight_time`, `time_zone`, `domain_name`, `user_tag`, `organization_tag`, or `ticket_form_history` features, and allows you to disable the corresponding functionality. By default, all variables' values are assumed to be `true`. Add variables for only the tables you want to disable:
```yml
vars:
    using_schedules:            False         #Disable if you are not using schedules
    using_domain_names:         False         #Disable if you are not using domain names
    using_user_tags:            False         #Disable if you are not using user tags
    using_ticket_form_history:  False         #Disable if you are not using ticket form history
    using_organization_tags:    False         #Disable if you are not using organization tags
```

## (Optional) Step 5: Additional configurations

### Adding passthrough columns
This package includes all source columns defined in the staging models. However, the `stg_zendesk__ticket` model allows for additional columns to be added using a pass-through column variable. This is extremely useful if you'd like to include custom fields to the package.
```yml
vars:
  zendesk__ticket_passthrough_columns: [account_custom_field_1, account_custom_field_2]
```

### Mark Former Internal Users as Agents
If a team member leaves your organization and their internal account is deactivated, their `USER.role` will switch from `agent` or `admin` to `end-user`. This will skew historical ticket SLA metrics, as we calculate reply times and other metrics based on `agent` or `admin` activity only.

To persist the integrity of historical ticket SLAs and mark these former team members as agents, provide the `internal_user_criteria` variable with a SQL clause to identify them, based on fields in the `USER` table. This SQL will be wrapped in a `case when` statement in the `stg_zendesk__user` model.

Example usage:
```yml
# dbt_project.yml
vars:
  zendesk_source:
    internal_user_criteria: "lower(email) like '%@fivetran.com' or external_id = '12345' or name in ('Garrett', 'Alfredo')" # can reference any non-custom field in USER
```

### Tracking Ticket Field History Columns
The `zendesk__ticket_field_history` model generates historical data for the columns specified by the `ticket_field_history_columns` variable. By default, the columns tracked are `status`, `priority`, and `assignee_id`.  If you would like to change these columns, add the following configuration to your `dbt_project.yml` file. Additionally, the `zendesk__ticket_field_history` model allows for tracking the specified fields updater information through the use of the `zendesk_ticket_field_history_updater_columns` variable. The values passed through this variable limited to the values shown within the config below. By default, the variable is empty and updater information is not tracked. If you would like to track field history updater information, add any of the below specified values to your `dbt_project.yml` file. After adding the columns to your root `dbt_project.yml` file, run the `dbt run --full-refresh` command to fully refresh any existing models. 

```yml
vars:
    ticket_field_history_columns: ['the','list','of','column','names']
    ticket_field_history_updater_columns: [
                                            'updater_user_id', 'updater_name', 'updater_role', 'updater_email', 'updater_external_id', 'updater_locale', 
                                            'updater_is_active', 'updater_user_tags', 'updater_last_login_at', 'updater_time_zone', 
                                            'updater_organization_id', 'updater_organization_domain_names' , 'updater_organization_organization_tags'
                                            ]
```
*Note: This package only integrates the above ticket_field_history_updater_columns values. If you'd like to include additional updater fields, please create an [issue](https://github.com/fivetran/dbt_zendesk/issues) specifying which ones.*

### Extending and Limiting the Ticket Field History
This package will create a row in `zendesk__ticket_field_history` for each day that a ticket is open, starting at its creation date. A Zendesk ticket cannot be altered after being closed, so its field values will not change after this date. However, you may want to extend a ticket's history past its closure date for easier reporting and visualizing. To do so, add the following configuration to your root `dbt_project.yml` file:

```yml
# dbt_project.yml

...
config-version: 2

vars:
  zendesk:
    ticket_field_history_extension_months: integer_number_of_months # default = 0 
```

Conversely, you may want to only track the past X years of ticket field history. This could be for cost reasons, or because you have a BigQuery destination and have over 4,000 days (10-11 years) of data, leading to a `too many partitions` error in the package's incremental models. To limit the ticket field history to the most recent X years, add the following configuration to your root `dbt_project.yml` file:

```yml
# dbt_project.yml

...
config-version: 2

vars:
  zendesk:
    ticket_field_history_timeframe_years: integer_number_of_years # default = 50 (everything)
```

### Changing the Build Schema
By default this package will build the Zendesk staging models within a schema titled (<target_schema> + `_zendsk_source`), the Zendesk intermediate models within a schema titled (<target_schema> + `_zendesk_intermediate`), and the Zendesk final models within a schema titled (<target_schema> + `_zendesk`) in your target database. If this is not where you would like your modeled Zendesk data to be written to, add the following configuration to your root `dbt_project.yml` file:

```yml
models:
  zendesk:
    +schema: my_new_schema_name # leave blank for just the target_schema
    intermediate:
      +schema: my_new_schema_name # leave blank for just the target_schema
    sla_policy:
      +schema: my_new_schema_name # leave blank for just the target_schema
    ticket_history:
      +schema: my_new_schema_name # leave blank for just the target_schema
  zendesk_source:
    +schema: my_new_schema_name # leave blank for just the target_schema
```
    
### Change the source table references
If an individual source table has a different name than the package expects, add the table name as it appears in your destination to the respective variable:

> IMPORTANT: See this project's [`dbt_project.yml`](https://github.com/fivetran/dbt_zendesk_source/blob/main/dbt_project.yml) variable declarations to see the expected names.

```yml
vars:
    zendesk_<default_source_table_name>_identifier: your_table_name 
```

## (Optional) Step 6: Orchestrate your models with Fivetran Transformations for dbt Core™
<details><summary>Expand for details</summary>
<br>
    
Fivetran offers the ability for you to orchestrate your dbt project through [Fivetran Transformations for dbt Core™](https://fivetran.com/docs/transformations/dbt). Learn how to set up your project for orchestration through Fivetran in our [Transformations for dbt Core setup guides](https://fivetran.com/docs/transformations/dbt#setupguide).
</details>

# 🔍 Does this package have dependencies?
This dbt package is dependent on the following dbt packages. Please be aware that these dependencies are installed by default within this package. For more information on the following packages, refer to the [dbt hub](https://hub.getdbt.com/) site.
> IMPORTANT: If you have any of these dependent packages in your own `packages.yml` file, we highly recommend that you remove them from your root `packages.yml` to avoid package version conflicts.
    
```yml
packages:
    - package: fivetran/zendesk_source
      version: [">=0.10.0", "<0.11.0"]

    - package: fivetran/fivetran_utils
      version: [">=0.4.0", "<0.5.0"]

    - package: dbt-labs/dbt_utils
      version: [">=1.0.0", "<2.0.0"]

    - package: dbt-labs/spark_utils
      version: [">=0.3.0", "<0.4.0"]

    - package: calogica/dbt_date
      version: [">=0.9.0", "<1.0.0"]
```
# 🙌 How is this package maintained and can I contribute?
## Package Maintenance
The Fivetran team maintaining this package _only_ maintains the latest version of the package. We highly recommend you stay consistent with the [latest version](https://hub.getdbt.com/fivetran/zendesk/latest/) of the package and refer to the [CHANGELOG](https://github.com/fivetran/dbt_zendesk/blob/main/CHANGELOG.md) and release notes for more information on changes across versions.

## Contributions
A small team of analytics engineers at Fivetran develops these dbt packages. However, the packages are made better by community contributions! 

We highly encourage and welcome contributions to this package. Check out [this dbt Discourse article](https://discourse.getdbt.com/t/contributing-to-a-dbt-package/657) on the best workflow for contributing to a package!

## Opinionated Modelling Decisions
This dbt package takes an opinionated stance on how business time metrics are calculated. The dbt package takes **all** schedules into account when calculating the business time duration. Whereas, the Zendesk UI logic takes into account **only** the latest schedule assigned to the ticket. If you would like a deeper explanation of the logic used by default in the dbt package you may reference the [DECISIONLOG](https://github.com/fivetran/dbt_zendesk/blob/main/DECISIONLOG.md).

# 🏪 Are there any resources available?
- If you have questions or want to reach out for help, please refer to the [GitHub Issue](https://github.com/fivetran/dbt_zendesk/issues/new/choose) section to find the right avenue of support for you.
- If you would like to provide feedback to the dbt package team at Fivetran or would like to request a new dbt package, fill out our [Feedback Form](https://www.surveymonkey.com/r/DQ7K7WW).
- Have questions or want to be part of the community discourse? Create a post in the [Fivetran community](https://community.fivetran.com/t5/user-group-for-dbt/gh-p/dbt-user-group) and our team along with the community can join in on the discussion!
