# Decision Log

## Business Time Metrics
When developing this package we noticed Zendesk reported ticket response times in business minutes based on the last schedule which is applied to the ticket. However, we felt this is not an accurate representation of the true ticket elapsed time in business minutes. Therefore, we took the opinionated decision to apply logic within our transformations to calculate the cumulative elapsed time in business minutes of a ticket across **all** schedules which the ticket was assigned during it's lifetime.

Below is a quick explanation of how this is calculated within the dbt package for **first_reply_time_business_minutes** as well as how this differs from Zendesk's logic:
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
- Now that we have the schedules, the schedule intervals, and the first_reply_time we can calculate the total elapsed `first_reply_time_business_minutes`. But, let's first convert the UTC timestamps to the Zendesk-esque intervals expressed within the schedules:
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

- So how does Zendesk calculate this?
  - Instead of taking into account the various schedules used by the ticket, Zendesk will instead use the **last** schedule applied to the ticket to record the duration in business minutes.
- Therefore, in the example above Zendesk will **only** use the `Level 2 San Francisco` schedule when calculating the `first_reply_time_business_minutes` for ticket `941606`.
  - Below is an example of how Zendesk calculates this:

| **Schedule** | **Schedule start_time_utc** | **Schedule end_time_utc**  | **Ticket Start** | **Ticket End** | **Difference** | 
|----| ------------------ | -----------------| ------------------ | -----------------| ------------------ |
| `Level 2 San Francisco` | 3780 | >**4350**<  | 3902 | 5461.46 | 448 |
| `Level 2 San Francisco` | >**5220**<  | 5790 | 3902 | 5461.46 | 241.46 |

- Adding the differences above we arrive at a total `first_reply_time_business_minutes` of 689.46 minutes.