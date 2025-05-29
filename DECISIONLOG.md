# Decision Log

## User Role History
### Broadened Definition of Internal Roles
When not using the user role history, internal roles were limited to `'admin'`, `'agent'`, and `internal_user_criteria` custom definitions when identifying internal users. However, when using audit logs to reconstruct role history, this approach proved insufficient—particularly in orgs that leverage custom roles (e.g., “Light Agent”), which are stored as `'agent'` in the `users` table but appear differently in audit log `change_description`.

When the `using_audit_log` variable is enabled, internal roles are now defined as any role *not equal to* `'end-user'` or `'not set'`. This better reflects actual internal users in environments with custom roles and aligns role history more closely with how roles behave operationally, however there is the chance that users are now over-included as users. 

In the `zendesk__ticket_enriched` model, the `is_agent_submitted` field will now evaluate to `true` if the submitter's role is determined as `is_internal_role = true` in the role history. If audit logs are not enabled, only `agent` or `admin` roles will evaluate to `true`.

If you encounter a scenario where this logic doesn't align with your expectations, please consider opening a [feature request](https://github.com/fivetran/dbt_zendesk/issues/new/choose) so we can evaluate it further.

Future Considerations:
- Investigating the `custom_role` table may allow finer control in distinguishing between support-enabled and limited-access roles (e.g., Light Agent vs Contributor).
- For now, the broader internal role logic provides a reasonable balance between simplicity and accuracy.

## Schedule History
### Handling Multiple Schedule Changes in a Day
While integrating schedule changes from the audit_log source, we observed that multiple changes can occur on the same day, often when users are still finalizing a schedule. To maintain clarity and align with our day-based downstream logic, we decided to capture only the last change made on any given day. If this approach proves insufficient for your use case, please submit a [feature request](https://github.com/fivetran/dbt_zendesk/issues/new/choose) for enabling support for multiple changes within a single day.

### Backfilling the Schedule History
Although the schedule history extracted from the audit log includes the most recent schedule, we exclude it in the `int_zendesk__schedule_history` model. Instead, we rely on the schedule from `stg_zendesk__schedule`, since it represents the live schedule. This approach also allows users who are not using schedule histories to easily disable the history feature. We join the live schedule with the schedule history model and bridge the valid_from and valid_until dates to maintain consistency.

## Tracking Ticket SLA Policies Into the Future
In our models we generate a future time series for ticket SLA policies. This is limited to a year to maintain performance. 

## Zendesk Support First Reply Time SLA Opinionated Logic
The logic for `first_reply_time` breach/achievement metrics within the `zendesk__ticket_metrics` and `zendesk__sla_policies` models are structured on the Zendesk Support definition of [first reply time SLA events](https://support.zendesk.com/hc/en-us/articles/4408821871642-Understanding-ticket-reply-time?page=2#topic_jvw_nqd_1hb). For example, this data model calculates first reply time to be the duration of time (business or calendar) between the creation of the ticket and the first public comment from either an `agent` or `admin`. This holds true regardless of when the first reply time SLA was applied to the ticket.

This means if a ticket has been opened for a number of days and then a `first_reply_time` SLA is applied to the ticket, this data model will still calculate the `first_reply_time` metric as the duration of time from the creation of the ticket and the first public comment, **not** from when the SLA was applied. 

We have found that some reports of `sla_breach_at`, `sla_elapsed_time`, and the `first_reply_time_*` metrics in the aforementioned models do not match the metrics provided in the Zendesk Support UI. This is due to certain reports in Zendesk Support calculating the `first_reply_time` as the first public `agent` or `admin` reply following the SLA being applied to the ticket. We are taking the stance in this data model that this is not reflective of the `first_reply_time` metric and will continue to report the `first_reply_time` as mentioned above. As a result, some of your `first_reply_time` metrics may potentially not match exactly what you see reported in the Zendesk Support UI reports.

## Zendesk Support Backlog Tickets
- You may find some discrepancies between what Zendesk Support reports and our model the total number of backlog tickets on a given day. After investigating this we have realized this is due to Zendesk Support taking a snapshot of each day sometime in the 23rd hour as stated in their [article](https://support.zendesk.com/hc/en-us/articles/4408819342490-Why-does-the-Backlog-dataset-only-show-the-Backlog-recorded-Hour-as-23-).

```
Because backlog data is captured on a per-day basis, it cannot be segmented hourly. The Backlog recorded - Hour is listed as 23 because data is captured daily between 11 pm, 12 am, or 1 am depending on factors like Daylight Saving Time (DST). 
For more information, see the article: Analyzing your ticket backlog history with Explore.
```

- While Zendesk Support doesn't segment their backlog data per hour, on the other hand we always try to model our data starting at a greater granularity. This means we start by taking the _hour_ from the timestamp field from the Zendesk Support source tables then bringing it to _day_. Therefore there will be edge cases where tickets updated near the end of day may fall into different statuses, depending on whether you're looking at the Zendesk Support Backlog dashboard or our model outputs.

## Business Time Metrics
When developing this package we noticed Zendesk Support reported ticket response times in business minutes based on the last schedule which is applied to the ticket. However, we felt this is not an accurate representation of the true ticket elapsed time in business minutes. Therefore, we took the opinionated decision to apply logic within our transformations to calculate the cumulative elapsed time in business minutes of a ticket across **all** schedules which the ticket was assigned during it's lifetime.

Below is a quick explanation of how this is calculated within the dbt package for **first_reply_time_business_minutes** as well as how this differs from Zendesk Support's logic:
> Note: While this is an example of `first_reply_time_business_minutes`, the logic is the same for other business minute metrics.

- A ticket (`941606`) is created on `2020-09-29 17:01:38 UTC` and first solved at `2020-10-01 15:03:44 UTC`.
- When the ticket was created it was assigned the schedule `Level 1 Chicago`
  - The schedule intervals are expressed as the number of minutes since the start of the week.
  - Sunday is considered the start of the week.
- The `Level 1 Chicago` schedule can be interpreted as the following:

| **start_time_utc** | **end_time_utc**  | 
| ------------------ | ----------------- |
| 720  | 1560  |
| 2160 | 3000  |
| 3600 | 4440  |
| 5040 | 5880  |
| 6480 | 7320  |
| 7920 | 8760  |
| 9360 | 10200 |

- Looking closer into the ticket, we also see another schedule `Level 2 San Francisco` was assigned to the ticket on `2020-09-30 19:01:25 UTC`
- The `Level 2 San Francisco` schedule can be interpreted as the following:

| **start_time_utc** | **end_time_utc**  | 
| ------------------ | ----------------- |
| 2340 | 2910 |
| 3780 | 4350 |
| 5220 | 5790 |
| 6660 | 7230 |
| 8100 | 8670 |

- Now that we know the ticket had two schedules, let's see the comments exchanged within this ticket to capture when the `first_reply_time` was recorded.

| **ticket_id** | **field_name** | **is_public** | **commenter_role** | **valid_starting_at** |
| ------------- | -------------- | ------------- | ------------------ | --------------------- |
| 941606 | comment | TRUE | external_comment | 2020-09-29 17:01:38 UTC |
| 941606 | comment | FALSE | internal_comment | 2020-09-30 19:01:25 UTC |
| 941606 | comment | TRUE | internal_comment | 2020-09-30 19:01:46 UTC |
| 941606 | comment | TRUE | internal_comment | 2020-10-01 15:03:44 UTC |

- Seeing the comments made to the ticket, we understand that the customer commented on the ticket at `2020-09-29 17:01:38 UTC` and the first **public** internal comment was made at `2020-09-30 19:01:46 UTC`.
- In comparison of the two schedules associated with this ticket, we can see that the `Level 1 Chicago` schedule was set for almost the entire duration of the ticket before the first reply. Whereas, the `Level 2 San Francisco` schedule was only set for 21 seconds.
  - Regardless, we will be using both schedules in the calculation of the `first_reply_time_business_minutes`.
- Now that we have the schedules, the schedule intervals, and the first_reply_time we can calculate the total elapsed `first_reply_time_business_minutes`. But, let's first convert the UTC timestamps to the Zendesk Support-esque intervals expressed within the schedules:
> The `Interval Results` are calculate via: `(Full Days From Sunday * 24 * 60) + (Hours * 60) + Minutes`

| **Action** | **Timestamp** | **Full Days from Sunday** | **Hours** | **Minutes** | **Interval Result** |
| ---------- | ------------- | ------------------------- | --------- | ----------- | ------------------- |
| Ticket Created and Schedule set to Level 1 Chicago | `Tuesday, September 29, 2020 at 5:01:38 PM` | 2 | 17 | 2 | 3902 |
| Schedule changed to Level 2 San Francisco | `Wednesday, September 30, 2020 at 7:01:25 PM` | 3 | 19 | 1.25 | 5461.25 |
| First Public Internal Comment | `Wednesday, September 30, 2020 at 7:01:46 PM` | 3 | 19 | 1.46 | 5461.46 |

- With the Interval Results obtained above, we can see where these overlap within the schedules.

**Level 1 Chicago**
> Overlap was from 3902 to 5461.25 and falls within two intervals

| **start_time_utc** | **end_time_utc**  | 
| ------------------ | ----------------- |
| 720  | 1560  |
| 2160 | 3000  |
| >**3600**<  | >**4440**<  |
| >**5040**< | >**5880**<  |
| 6480 | 7320  |
| 7920 | 8760  |
| 9360 | 10200 |

**Level 2 San Francisco**
> Only overlap was from 5461.25 to 5461.46 and falls within one interval

| **start_time_utc** | **end_time_utc**  | 
| ------------------ | ----------------- |
| 2340 | 2910 |
| 3780 | 4350 |
| >**5220**< | >**5790**< |
| 6660 | 7230 |
| 8100 | 8670 |

- Now let's figure out the overlapping duration

| **Schedule** | **Schedule start_time_utc** | **Schedule end_time_utc**  | **Ticket Start** | **Ticket End** | **Difference** | 
|----| ------------------ | -----------------| ------------------ | -----------------| ------------------ |
| `Level 1 Chicago` | 3600 | >**4440**< | >**3902**< | 5461.25 | 538 |
| `Level 1 Chicago` | >**5040**< | 5880 | 3902 | >**5461.25**< | 421.25 |
| `Level 2 San Francisco` | >**5220**< (We use **5461.25** to account for overlap) | 5790 | 5462 | >**5461.46**< | .21 |

- Adding the differences above we arrive at a total `first_reply_time_business_minutes` of 959.46 minutes.

- So how does Zendesk Support calculate this?
  - Instead of taking into account the various schedules used by the ticket, Zendesk Support will instead use the **last** schedule applied to the ticket to record the duration in business minutes.
- Therefore, in the example above Zendesk Support will **only** use the `Level 2 San Francisco` schedule when calculating the `first_reply_time_business_minutes` for ticket `941606`.
  - Below is an example of how Zendesk Support calculates this:

| **Schedule** | **Schedule start_time_utc** | **Schedule end_time_utc**  | **Ticket Start** | **Ticket End** | **Difference** | 
|----| ------------------ | -----------------| ------------------ | -----------------| ------------------ |
| `Level 2 San Francisco` | 3780 | >**4350**<  | 3902 | 5461.46 | 448 |
| `Level 2 San Francisco` | >**5220**<  | 5790 | 3902 | 5461.46 | 241.46 |

- Adding the differences above we arrive at a total `first_reply_time_business_minutes` of 689.46 minutes.