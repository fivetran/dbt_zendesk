# Zendesk

This package models Zendesk data from [Fivetran's connector](https://fivetran.com/docs/applications/zendesk). It uses data in the format described by [this ERD](https://docs.google.com/presentation/d/1AQv77L9WlDXqRS0gkdQTmg1HSUo-Znlcoq7CHg0JrP8).

The main focus of this package is to enable you to better understand the performance of your Support team. Metrics focused on response times, resolution times and work time are calculated for you. Optionally, these metrics can be converted to business times if you use Zendesk's scheduling feature.  SLA policy breaches are also calculated.

## Models

This package contains transformation models, designed to work simultaneously with our [Zendesk source package](https://github.com/fivetran/dbt_zendesk_source). A depenedency on the source package is declared in this package's `packages.yml` file, so it will automatically download when you run `dbt deps`. The primary outputs of this package are described below. Intermediate models are used to create these output models.

| **model**                  | **description**                                                                                                                                               |
| -------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Zendesk\_issues             | Each record represents a Zendesk issue, enriched with data about it's assignees, milestones, and time comparisons.                                             |
| Zendesk\_pull\_requests     | Each record represents a Zendesk pull request, enriched with data about it's repository, reviewers, and durations between review requests, merges and reviews. |
| Zendesk\_daily\_metrics     | Each record represents a single day, enriched with metrics about PRs and issues that were created and closed during that period.                              |
| Zendesk\_weekly\_metrics    | Each record represents a single week, enriched with metrics about PRs and issues that were created and closed during that period.                             |
| Zendesk\_monthly\_metrics   | Each record represents a single month, enriched with metrics about PRs and issues that were created and closed during that period.                            |
| Zendesk\_quarterly\_metrics | Each record represents a single quarter, enriched with metrics about PRs and issues that were created and closed during that period.                          |


## Installation Instructions
Check [dbt Hub](https://hub.getdbt.com/) for the latest installation instructions, or [read the docs](https://docs.getdbt.com/docs/package-management) for more information on installing packages.

## Configuration
By default this package will look for your Zendesk data in the `Zendesk` schema of your [target database](https://docs.getdbt.com/docs/running-a-dbt-project/using-the-command-line-interface/configure-your-profile). If this is not where your Zendesk data is (perhaps your Zendesk schema is `Zendesk_fivetran`), please add the following configuration to your `dbt_project.yml` file:

```yml
# dbt_project.yml

...
config-version: 2

vars:
  Zendesk_source:
    Zendesk_database: your_database_name
    Zendesk_schema: your_schema_name 
```


The `marketo__lead_history` model generates historical data for the columns specified by the `lead_history_columns` variable. By default, the columns tracked are `lead_status`, `urgency`, `priority`, `relative_score`, `relative_urgency`, `demographic_score_marketing` and `behavior_score_marketing`.  If you would like to change these columns, add the following configuration to your `dbt_project.yml` file.  After adding the columns to your `dbt_project.yml` file, run the `dbt run --full-refresh` command to fully refresh any existing models.

```yml
# dbt_project.yml

...
config-version: 2

vars:
  marketo:
    lead_history_columns: ['the','list','of','column','names']
```

## Contributions

Additional contributions to this package are very welcome! Please create issues
or open PRs against `master`. Check out 
[this post](https://discourse.getdbt.com/t/contributing-to-a-dbt-package/657) 
on the best workflow for contributing to a package.

## Resources:
- Learn more about Fivetran [here](https://fivetran.com/docs)
- Check out [Fivetran's blog](https://fivetran.com/blog)
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](http://slack.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
