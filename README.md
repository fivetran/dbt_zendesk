# Zendesk Support

This package models Zendesk Support data from [Fivetran's connector](https://fivetran.com/docs/applications/zendesk). It uses data in the format described by [this ERD](https://fivetran.com/docs/applications/zendesk#schemainformation).

This package enables you to better understand the performance of your Support team. It calculates metrics focused on response times, resolution times, and work times for you to analyze. 

### Optional features (for Zendesk Professional or Enterprise users)
- Package converts metrics to business hours
- Package calculates SLA policy breaches

## Models

This package contains transformation models, designed to work simultaneously with our [Zendesk Support source package](https://github.com/fivetran/dbt_zendesk_source). A dependency on the source package is declared in this package's `packages.yml` file, so it will automatically download when you run `dbt deps`. The primary outputs of this package are described below. Intermediate models are used to create these output models.

| **model**                    | **description**                                                                                                                                                 |
| ---------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| zendesk_ticket_metrics       | Each record represents a Zendesk ticket, enhriched with metrics about reply times, resolution times, and work times.  Calendar and business hours are supported.  |
| zendesk_ticket_enhanced      | Each record represents a Zendesk ticket, enriched with data about its tags, assignees, requester, submitter, organization, and group.                           |
| zendesk_ticket_field_history | A daily historical view of the ticket field values defined in the `ticket_field_history_columns` variable .                                                        |
| zendesk_sla_breach           | Each record represents an SLA breach event. Calendar and business hour SLA breaches are supported.                                                              |

## Installation Instructions
Check [dbt Hub](https://hub.getdbt.com/) for the latest installation instructions, or [read the docs](https://docs.getdbt.com/docs/package-management) for more information on installing packages.

## Configuration
By default, this package looks for your Zendesk Support data in the `zendesk` schema of your [target database](https://docs.getdbt.com/docs/running-a-dbt-project/using-the-command-line-interface/configure-your-profile). If this is not where your Zendesk Support data is, add the following configuration to your `dbt_project.yml` file:

```yml
# dbt_project.yml

...
config-version: 2

vars:
  zendesk_source:
    zendesk_database: your_database_name
    zendesk_schema: your_schema_name 
```

The `zendesk_ticket_field_history` model generates historical data for the columns specified by the `ticket_field_history_columns` variable. By default, the columns tracked are `status`, `priority`, and `assignee_id`.  If you would like to change these columns, add the following configuration to your `dbt_project.yml` file.  After adding the columns to your `dbt_project.yml` file, run the `dbt run --full-refresh` command to fully refresh any existing models.

```yml
# dbt_project.yml

...
config-version: 2

vars:
  zendesk:
    ticket_field_history_columns: ['the','list','of','column','names']
```

### Disabling models

It's possible that your Zendesk Support connector does not sync every table that this package expects. If your syncs exclude certain tables, it is because you either don't use that functionality in Zendesk Support or actively excluded some tables from your syncs. To disable the corresponding functionality in the package, you must add the relevant variables. By default, all variables are assumed to be `true`. Add variables for only the tables you would like to disable:  

```yml
# dbt_project.yml

...
config-version: 2

vars:
  zendesk_source:
    using_schedules: false        # Disable if you do not have the schedule and ticket_schedule tables, or if you do not want metrics reported in business hours
    using_sla_policy: false       # Disable if you are not using SLAs 
  zendesk:
    using_schedules: false
    using_sla_policy: false
```

## Contributions

Additional contributions to this package are very welcome! Please create issues
or open PRs against `master`. Check out 
[this post](https://discourse.getdbt.com/t/contributing-to-a-dbt-package/657) 
on the best workflow for contributing to a package.

## Resources:
- Find all of Fivetran's pre-built dbt packages in our [dbt hub](https://hub.getdbt.com/fivetran/)
- Provide [feedback](https://www.surveymonkey.com/r/DQ7K7WW) on our existing dbt packages or what you'd like to see next
- Learn more about Fivetran [here](https://fivetran.com/docs)
- Check out [Fivetran's blog](https://fivetran.com/blog)
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](http://slack.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
