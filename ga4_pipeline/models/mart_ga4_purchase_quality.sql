with item_totals as (
  select event_key, count(*) as item_rows, sum(extended_cents) as item_value_cents
  from {{ ref('fct_ga4_items') }}
  group by event_key
),
purchase_events as (
  select *,
         count(*) over (partition by transaction_id) as transaction_occurrences
  from {{ ref('fct_ga4_events') }}
  where event_name = 'purchase'
)
select p.event_key, p.event_date, p.arrival_batch, p.transaction_id,
       p.user_pseudo_id, p.session_id, p.revenue_cents,
       coalesce(i.item_rows, 0) as item_rows,
       coalesce(i.item_value_cents, 0) as item_value_cents,
       p.revenue_cents - coalesce(i.item_value_cents, 0) as revenue_variance_cents,
       case when p.transaction_id is null or p.transaction_occurrences > 1
         then 1 else 0 end as transaction_id_finding,
       case when p.user_pseudo_id is null or p.session_id is null
         then 1 else 0 end as identity_finding,
       case when p.revenue_cents is null or i.item_value_cents is null
             or abs(p.revenue_cents - i.item_value_cents) > 1
         then 1 else 0 end as revenue_finding
from purchase_events p
left join item_totals i on p.event_key = i.event_key
