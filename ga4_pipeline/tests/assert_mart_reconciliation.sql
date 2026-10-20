-- Independently aggregate event grain and compare it to the daily mart.
with src as (
  select event_date,
         count(*) as event_count,
         sum(case when event_name='purchase' then 1 else 0 end) as purchase_events,
         sum(case when event_name='purchase' then coalesce(revenue_cents,0) else 0 end) as reported_revenue_cents
  from {{ ref('fct_ga4_events') }}
  group by event_date
)
select s.event_date
from src s
left join {{ ref('mart_ga4_daily') }} d on s.event_date=d.event_date
where d.event_date is null
   or s.event_count <> d.event_count
   or s.purchase_events <> d.purchase_events
   or s.reported_revenue_cents <> d.reported_revenue_cents
