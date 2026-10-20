select e.event_date, count(*) as event_count,
       sum(case when e.event_name = 'purchase' then 1 else 0 end) as purchase_events,
       sum(coalesce(p.item_rows, 0)) as purchase_item_rows,
       sum(coalesce(p.revenue_cents, 0)) as reported_revenue_cents,
       sum(coalesce(p.item_value_cents, 0)) as item_value_cents,
       sum(coalesce(p.transaction_id_finding, 0)) as transaction_id_findings,
       sum(coalesce(p.revenue_finding, 0)) as revenue_findings,
       sum(case when e.user_pseudo_id is null or e.session_id is null then 1 else 0 end)
         as identity_findings
from {{ ref('fct_ga4_events') }} e
left join {{ ref('mart_ga4_purchase_quality') }} p on e.event_key = p.event_key
group by e.event_date
