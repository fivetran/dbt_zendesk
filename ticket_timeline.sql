 with ticket_status_history as (
        select
          ticket_id as ticket_id,
          updated as valid_starting,
          value as status,
        from `digital-arbor-400`.zendesk_new.ticket_field_history
        where lower(field_name) = 'status'
      ), ticket_status_timeline as (
        select
          ticket_id,
          valid_starting,
          coalesce(
            lead(valid_starting) over (partition by ticket_id order by valid_starting),
            '2999-12-31 23:59:59 UTC')
            as valid_until,
          status,
        from ticket_status_history
      ), generate_timeline as (
        select
          timeline_date as timeline_date_start,
          lead(timeline_date, 1) over (order by timeline_date) as timeline_date_end -- this is NON-INCLUSIVE
        from unnest(
          generate_date_array(
            date_trunc('2020-02-10', {% parameter time_period %}), -- ZD launch date
            date_add(current_date(), interval 1 {% parameter time_period %}), -- end 1 period after current_date
            interval 1 {% parameter time_period %}) -- specify period length
          ) as timeline_date
      )
      select
        ticket_status_timeline.ticket_id,
        -- the DISPLAYED period should always show 1 day before the start of the new_period
        -- since in the timeline table end date is NON-INCLUSIVE and we would like to show an INCLUSIVE period_end
        -- we just subtract 1 day from the EXCLUSIVE timeline end date to get an INCLUSIVE timeline end date
        -- example (monthly), if monthly is selected then for march we should show 2019-03-31, which is one day before 2019-04-01 (the exclusive end date)
        -- example (daily), if daily is selected then for 2019-03-15, we should show the same date (one day before 2019-03-16 which would be the exclusive end date)
        date_add(generate_timeline.timeline_date_end, interval -1 day) as period_ending_date,
        array_agg(ticket_status_timeline.status order by valid_starting desc)[offset(0)] as last_status_in_period,
      from generate_timeline
      join ticket_status_timeline as ticket_status_timeline
        on timestamp(generate_timeline.timeline_date_start) <= ticket_status_timeline.valid_until
          and timestamp(generate_timeline.timeline_date_end) > ticket_status_timeline.valid_starting
      where status not in ('solved','closed','deleted')
      group by 1,2
      ;;