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
| [zendesk__ticket_metrics](https://github.com/fivetran/dbt_zendesk/blob/master/models/zendesk__ticket_metrics.sql)       | Each record represents a Zendesk ticket, enriched with metrics about reply times, resolution times, and work times.  Calendar and business hours are supported.  |
| [zendesk__ticket_enriched](https://github.com/fivetran/dbt_zendesk/blob/master/models/zendesk__ticket_enriched.sql)      | Each record represents a Zendesk ticket, enriched with data about its tags, assignees, requester, submitter, organization, and group.                           |
| [zendesk__ticket_summary](https://github.com/fivetran/dbt_zendesk/blob/master/models/zendesk__ticket_summary.sql)           | A single record table containing Zendesk ticket and user summary metrics.                                                              |
| [zendesk__ticket_backlog](https://github.com/fivetran/dbt_zendesk/blob/master/models/zendesk__ticket_backlog.sql)           | A daily historical view of the ticket field values defined in the `ticket_field_history_columns` variable for all backlog tickets. Backlog tickets being defined as any ticket not in a 'closed', 'deleted', or 'solved' status.                                                             |
| [zendesk__ticket_field_history](https://github.com/fivetran/dbt_zendesk/blob/master/models/zendesk__ticket_field_history.sql) | A daily historical view of the ticket field values defined in the `ticket_field_history_columns` variable and the corresponding updater fields defined in the `ticket_field_history_updater_columns` variable.                                                        |
| [zendesk__sla_breach](https://github.com/fivetran/dbt_zendesk/blob/master/models/zendesk__sla_breach.sql)           | Each record represents an SLA breach event. Calendar and business hour SLA breaches are supported.                                                              |

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

The `zendesk__ticket_field_history` model generates historical data for the columns specified by the `ticket_field_history_columns` variable. By default, the columns tracked are `status`, `priority`, and `assignee_id`.  If you would like to change these columns, add the following configuration to your `dbt_project.yml` file. Additionally, the `zendesk__ticket_field_history` model allows for tracking the specified fields updater information through the use of the `zendesk_ticket_field_history_updater_columns` variable. The values passed through this variable limited to the values shown within the config below. By default, the variable is empty and updater information is not tracked. If you would like to track field history updater information, add any of the below specified values to your `dbt_project.yml` file. After adding the columns to your `dbt_project.yml` file, run the `dbt run --full-refresh` command to fully refresh any existing models. 

```yml
# dbt_project.yml

...
config-version: 2

vars:
  zendesk:
    ticket_field_history_columns: ['the','list','of','column','names']
    ticket_field_history_updater_columns: [
                                            'updater_user_id', 'updater_name', 'updater_role', 'updater_email', 'updater_external_id', 'updater_locale', 
                                            'updater_is_active', 'updater_user_tags', 'updater_last_login_at', 'updater_time_zone', 
                                            'updater_organization_id', 'updater_organization_domain_names' , 'updater_organization_organization_tags'
                                          ]
```
*Note: This package only integrates the above ticket_field_history_updater_columns values. If you'd like to include additional updater fields, please create an [issue](https://github.com/fivetran/dbt_zendesk/issues) specifying which ones.*

### Changing the Build Schema
By default this package will build the Zendesk Support intermediate models within a schema titled (<target_schema> + `_zendesk_intermediate`)  and the Zendesk Support final models within your <target_schema> in your target database. If this is not where you would like you Zendesk Support intermediate and final models to be written to, add the following configuration to your `dbt_project.yml` file:

```yml
# dbt_project.yml

...
models:
  zendesk:
    +schema: my_new_final_models_schema
    intermediate:
      +schema: my_new_intermediate_models_schema
    sla_policy:
      +schema: my_new_intermediate_models_schema
    ticket_history:
      +schema: my_new_intermediate_models_schema

```

### Disabling models

This package takes into consideration that not every Zendesk account utilizes the `schedule`, `domain_name`, `user_tag`, `organization_tag`, or `ticket_form_history` features, and allows you to disable the corresponding functionality. By default, all variables' values are assumed to be `true`. Add variables for only the tables you want to disable:

```yml
# dbt_project.yml

...
config-version: 2

vars:
  using_schedules:            False         #Disable if you are not using schedules
  using_domain_names:         False         #Disable if you are not using domain names
  using_user_tags:            False         #Disable if you are not using user tags
  using_ticket_form_history:  False         #Disable if you are not using ticket form history
  using_organization_tags:    False         #Disable if you are not using organization tags
```
*Note: This package only integrates the above variables. If you'd like to disable other models, please create an [issue](https://github.com/fivetran/dbt_zendesk/issues) specifying which ones.*
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
