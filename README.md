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
    version: [">=0.14.0", "<0.15.0"]

```
> **Note**: Do not include the Zendesk source package. The Zendesk transform package already has a dependency on the source in its own `packages.yml` file.

## Step 3: Define database and schema variables
### Option 1: Single connector 💃
By default, this package runs using your destination and the `zendesk` schema. If this is not where your zendesk data is (for example, if your zendesk schema is named `zendesk_fivetran`), update the following variables in your root `dbt_project.yml` file accordingly:

```yml
vars:
    zendesk_database: your_destination_name
    zendesk_schema: your_schema_name 
```
> **Note**: If you are running the package on one source connector, each model will have a `source_relation` column that is just an empty string.

### Option 2: Union multiple connectors 👯
If you have multiple Zendesk connectors in Fivetran and would like to use this package on all of them simultaneously, we have provided functionality to do so. The package will union all of the data together and pass the unioned table into the transformations. You will be able to see which source it came from in the `source_relation` column of each model. To use this functionality, you will need to set either the `zendesk_union_schemas` OR `zendesk_union_databases` variables (cannot do both, though a more flexible approach is in the works...) in your root `dbt_project.yml` file:

```yml
# dbt_project.yml

vars:
    zendesk_union_schemas: ['zendesk_usa','zendesk_canada'] # use this if the data is in different schemas/datasets of the same database/project
    zendesk_union_databases: ['zendesk_usa','zendesk_canada'] # use this if the data is in different databases/projects but uses the same schema name
```

#### Recommended: Incorporate unioned sources into DAG
By default, this package defines one single-connector source, called `zendesk`, which will be disabled if you are unioning multiple connectors. This means that your DAG will not include your Zendesk sources, though the package will run successfully.

To properly incorporate all of your Zendesk connectors into your project's DAG:
1. Define each of your sources in a `.yml` file in your project. Utilize the following template to leverage our table and column documentation. 

  <details><summary><i>Expand for source configuration template</i></summary><p>

> **Note**: If there are source tables you do not have (see [Step 4](https://github.com/fivetran/dbt_zendesk?tab=readme-ov-file#step-4-disable-models-for-non-existent-sources)), you may still include them, as long as you have set the right variables to `False`. Otherwise, you may remove them from your source definitions.

```yml
sources:
  - name: <name>
    schema: <schema_name>
    database: <database_name>
    loader: fivetran
    loaded_at_field: _fivetran_synced
      
    freshness:
      warn_after: {count: 72, period: hour}
      error_after: {count: 168, period: hour}

    tables: &zendesk_table_defs # <- see https://support.atlassian.com/bitbucket-cloud/docs/yaml-anchors/
      - name: ticket
        description: >
          Tickets are the means through which your end users (customers) communicate with agents in Zendesk Support. Tickets can 
          originate from a number of channels, including email, Help Center, chat, phone call, Twitter, Facebook, or the API.
        columns:
          - name: id
            description: Automatically assigned when the ticket is created
          - name: url
            description: The API url of this ticket
          - name: assignee_id
            description: The agent currently assigned to the ticket
          - name: brand_id
            description: Enterprise only. The id of the brand this ticket is associated with
          - name: created_at
            description: When this record was created
          - name: type
            description: The type of this ticket, possible values are problem, incident, question or task
          - name: subject
            description: The value of the subject field for this ticket
          - name: description
            description: Read-only first comment on the ticket
          - name: priority
            description: The urgency with which the ticket should be addressed, possible values are urgent, high, normal and low
          - name: status
            description: The state of the ticket, possible values are new, open, pending, hold, solved and closed
          - name: recipient
            description: The original recipient e-mail address of the ticket
          - name: requester_id
            description: The user who requested this ticket
          - name: submitter_id
            description: The user who submitted the ticket. The submitter always becomes the author of the first comment on the ticket
          - name: organization_id
            description: The organization of the requester
          - name: group_id
            description: The group this ticket is assigned to
          - name: due_at
            description: If this is a ticket of type "task" it has a due date. Due date format uses ISO 8601 format.
          - name: ticket_form_id
            description: Enterprise only. The id of the ticket form to render for the ticket
          - name: is_public
            description: Is true if any comments are public, false otherwise
          - name: updated_at
            description: When this record last got updated
          - name: via_channel
            description: The channel the ticket was created from
          - name: via_source_from_id
            description: The channel the ticket was created from 
          - name: via_source_from_title
            description: The channel the ticket was created from
          - name: via_source_rel
            description: The rel the ticket was created from 
          - name: via_source_to_address
            description: The address of the source the ticket was created from
          - name: via_source_to_name
            description: The name of the source the ticket was created from    

      - name: brand
        description: >
          Brands are your customer-facing identities. They might represent multiple products or services, or they 
          might literally be multiple brands owned and represented by your company.
        columns:
          - name: id
            description: The ID automatically assigned when the brand is created
          - name: brand_url
            description: The url of the brand
          - name: name
            description: The name of the brand
          - name: subdomain
            description: The subdomain of the brand
          - name: active
            description: If the brand is set as active

      - name: domain_name
        description: Domain names associated with an organization. An organization may have multiple domain names.
        config:
          enabled: "{{ var('using_domain_names', true) }}"
        columns:
          - name: organization_id
            description: Reference to the organization
          - name: domain_name
            description: The name of the domain associated with the organization
          - name: index
            description: Index number of the domain name associated with the organization

      - name: group
        identifier: >
          {% if target.type == 'snowflake' %}"GROUP"{% else %}group{% endif %}
        description: >
          When support requests arrive in Zendesk Support, they can be assigned to a Group. Groups serve as the core
          element of ticket workflow; support agents are organized into Groups and tickets can be assigned to a Group
          only, or to an assigned agent within a Group. A ticket can never be assigned to an agent without also being 
          assigned to a Group.
        freshness: null
        columns:
          - name: id
            description: Automatically assigned when creating groups
          - name: name
            description: The name of the group

      - name: organization_tag
        description: The tags associated with an organization. An organization may have multiple tags.
        config:
          enabled: "{{ var('using_organization_tags', true) }}"
        columns:
          - name: organization_id
            description: Reference to the organization
          - name: tag
            description: Tag associated with the organization

      - name: organization
        description: >
          Just as agents can be segmented into groups in Zendesk Support, your customers (end-users) can be segmented into 
          organizations. You can manually assign customers to an organization or automatically assign them to an organization 
          by their email address domain. Organizations can be used in business rules to route tickets to groups of agents or 
          to send email notifications.
        freshness: null
        columns:
          - name: id
            description: Automatically assigned when the organization is created
          - name: name
            description: A unique name for the organization
          - name: details
            description: Any details obout the organization, such as the address
          - name: url
            description: The API url of this organization
          - name: external_id
            description: A unique external id to associate organizations to an external record
          - name: created_at
            description: The time the organization was created
          - name: updated_at
            description: The time of the last update of the organization
          - name: domain_names
            description: An array of domain names associated with this organization
          - name: details
            description: Any details obout the organization, such as the address
          - name: notes
            description: Any notes you have about the organization
          - name: group_id
            description: New tickets from users in this organization are automatically put in this group
          - name: shared_tickets
            description: End users in this organization are able to see each other's tickets
          - name: shared_comments
            description: End users in this organization are able to see each other's comments on tickets
          - name: tags
            description: The tags of the organization
          - name: organization_fields
            description: Custom fields for this organization

      - name: ticket_comment
        description: Ticket comments represent the conversation between requesters, collaborators, and agents. Comments can be public or private.
        columns:
          - name: id
            description: Automatically assigned when the comment is created
          - name: body
            description: The comment string
          - name: created
            description: The time the comment was created
          - name: public
            description: Boolean field indicating if the comment is public (true), or if it is an internal note (false)
          - name: ticket_id
            description: The ticket id associated with this comment
          - name: user_id
            description: The id of the comment author
          - name: facebook_comment
            description: Boolean field indicating if the comment is a facebook comment
          - name: tweet
            description: Boolean field indicating if the comment is a twitter tweet
          - name: voice_comment
            description: Boolean field indicating if the comment is a voice comment

      - name: user_tag
        description: Table containing all tags associated with a user. Only present if your account has user tagging enabled.
        config:
          enabled: "{{ var('using_user_tags', true) }}"
        columns:
          - name: user_id
            description: Reference to the user
          - name: tag
            description: Tag associated with the user

      - name: user
        description: Zendesk has three types of users, end-users (your customers), agents, and administrators.
        freshness: null
        columns:
          - name: id
            description: Automatically assigned when the user is created
          - name: email
            description: The user's primary email address. *Writeable on create only. On update, a secondary email is added. See Email Address
          - name: name
            description: The user's name
          - name: active
            description: false if the user has been deleted
          - name: created_at
            description: The time the user was created
          - name: organization_id
            description: The id of the user's organization. If the user has more than one organization memberships, the id of the user's default organization
          - name: role
            description: The user's role. Possible values are "end-user", "agent", or "admin"
          - name: time_zone
            description: The user's time zone. See Time Zone
          - name: ticket_restriction
            description: Specifies which tickets the user has access to. Possible values are organization, groups, assigned, requested and null

      - name: schedule
        description: The support schedules created with different business hours and holidays. 
        freshness: null
        config:
          enabled: "{{ var('using_schedules', true) }}"
        columns: 
          - name: id
            description: ID automatically assigned to the schedule upon creation
          - name: name
            description: Name of the schedule
          - name: created_at
            description: Time the schedule was created
          - name: start_time
            description: Start time of the schedule, in the schedule's time zone.
          - name: end_time
            description: End time of the schedule, in the schedule's time zone.
          - name: time_zone
            description: Timezone in which the schedule operates. 
          
      - name: ticket_schedule
        description: The schedules applied to tickets through a trigger.
        freshness: null
        columns: 
          - name: ticket_id
            description: The ID of the ticket assigned to the schedule
          - name: created_at
            description: The time the schedule was assigned to the ticket
          - name: schedule_id
            description: The ID of the schedule applied to the ticket
      
      - name: ticket_form_history
        description: Ticket forms allow an admin to define a subset of ticket fields for display to both agents and end users.
        config:
          enabled: "{{ var('using_ticket_form_history', true) }}"
        columns:
          - name: id
            description: Automatically assigned when creating ticket form
          - name: created_at
            description: The time the ticket form was created
          - name: updated_at
            description: The time of the last update of the ticket form
          - name: display_name
            description: The name of the form that is displayed to an end user
          - name: active
            description: If the form is set as active
          - name: name
            description: The name of the form

      - name: ticket_tag
        description: >
          Tags are words, or combinations of words, you can use to add more context to tickets. The table lists all
          tags currently associated with a ticket.
        freshness: null
        columns: 
          - name: ticket_id
            description: The ID of the ticket associated with the tag
          - name: tags
            description: The tag, or word(s), associated with the ticket

      - name: ticket_field_history
        description: All fields and field values associated with tickets.
        freshness: null
        columns: 
          - name: ticket_id
            description: The ID of the ticket associated with the field
          - name: field_name
            description: The name of the ticket field
          - name: updated
            description: The time the ticket field value was created
          - name: value
            description: The value of the field
          - name: user_id
            description: The id of the user who made the update
      
      - name: daylight_time
        description: >
          Appropriate offsets (from UTC) for timezones that engage or have engaged with Daylight Savings at some point since 1970.
        freshness: null
        columns:
          - name: daylight_end_utc
            description: UTC timestamp of when Daylight Time ended in this year.
          - name: daylight_offset
            description: Number of **hours** added during Daylight Savings Time. 
          - name: daylight_start_utc
            description: UTC timestamp of when Daylight Time began in this year.
          - name: time_zone
            description: Name of the timezone. 
          - name: year
            description: Year in which daylight savings occurred. 

      - name: time_zone
        description: Offsets (from UTC) for each timezone. 
        freshness: null
        columns:
          - name: time_zone
            description: Name of the time zone. 
          - name: standard_offset 
            description: Standard offset of the timezone (non-daylight savings hours). In `+/-hh:mm` format.
      
      - name: schedule_holiday
        description: Information about holidays for each specified schedule.
        freshness: null
        config:
          enabled: "{{ var('using_schedules', true) }}"
        columns:
          - name: end_date
            description: ISO 8601 representation of the holiday end date.
          - name: id
            description: The ID of the scheduled holiday.
          - name: name
            description: Name of the holiday.
          - name: schedule_id
            description: The ID of the schedule.
          - name: start_date
            description: ISO 8601 representation of the holiday start date.
```
  </p></details>

2. Set the `has_defined_sources` variable (scoped to the `zendesk_source` package) to true, like such:
```yml
# dbt_project.yml
vars:
  zendesk_source:
    has_defined_sources: true
```

## Step 4: Disable models for non-existent sources
> _This step is unnecessary (but still available for use) if you are unioning multiple connectors together in the previous step. That is, the `union_data` macro we use will create completely empty staging models for sources that are not found in any of your Zendesk schemas/databases. However, you can still leverage the below variables if you would like to avoid this behavior._

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
<details open><summary>Expand/collapse configurations</summary>

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
    
### Change the source table references (only if using a single connector)
If an individual source table has a different name than the package expects, add the table name as it appears in your destination to the respective variable:

> IMPORTANT: See this project's [`dbt_project.yml`](https://github.com/fivetran/dbt_zendesk_source/blob/main/dbt_project.yml) variable declarations to see the expected names.

```yml
vars:
    zendesk_<default_source_table_name>_identifier: your_table_name 
```
</details>

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
      version: [">=0.11.0", "<0.12.0"]

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
