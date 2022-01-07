### Zendesk Backlog Tickets

- You may find some discrepancies between what Zendesk reports and our model the total number of backlog tickets on a given day. After investigating this we have realized this is due to Zendesk taking a snapshot of each day sometime in the 23rd hour as stated in their [article.](https://support.zendesk.com/hc/en-us/articles/4408819342490-Why-does-the-Backlog-dataset-only-show-the-Backlog-recorded-Hour-as-23-).
 
```
Because backlog data is captured on a per-day basis, it cannot be segmented hourly. The Backlog recorded - Hour is listed as 23 because data is captured daily between 11 pm, 12 am, or 1 am depending on factors like Daylight Saving Time (DST). 
For more information, see the article: Analyzing your ticket backlog history with Explore.
```

- While Zendesk doesn't segment their backlog data per hour, on the other hand we always try to model our data starting at a greater granularity. This means we start by taking the _hour_ from the timestamp field from the Zendesk source tables then bringing it to _day_. Therefore there will be edge cases where tickets updated near the end of day may fall into different statuses, depending on whether you're looking at the Zendesk Backlog dashboard or our model outputs.
