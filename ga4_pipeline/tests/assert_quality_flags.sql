-- Duplicate transaction IDs and revenue discrepancies are disclosed, not deduped.
with expected as (
  select event_key, transaction_id,
         count(*) over (partition by transaction_id) as tx_count
  from {{ ref('fct_ga4_events') }}
  where event_name='purchase'
)
select p.event_key
from {{ ref('mart_ga4_purchase_quality') }} p
join expected e on e.event_key=p.event_key
where p.transaction_id_finding <>
      case when e.transaction_id is null or e.tx_count>1 then 1 else 0 end
   or p.revenue_finding <>
      case when p.revenue_cents is null or p.item_rows=0
                or abs(p.revenue_variance_cents)>1 then 1 else 0 end
