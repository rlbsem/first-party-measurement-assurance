select e.tenant,e.legal_entity,e.source,e.business_id,e.amount_minor
from {{ ref('fct_stream_events') }} e
left join (
  select tenant,legal_entity,source,business_id,sum(price_minor*quantity) item_minor
  from {{ ref('fct_stream_items') }} group by 1,2,3,4
) i using(tenant,legal_entity,source,business_id)
where e.value_basis='payment' and (i.item_minor is null or i.item_minor<>e.amount_minor)
