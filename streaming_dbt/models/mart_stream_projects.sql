-- Signed commitments and cash are separate measures; never add the two as revenue.
select tenant,legal_entity,project_id,currency,
       countif(value_basis='contract') signed_contracts,
       sum(if(value_basis='contract',amount_minor,0)) signed_minor,
       countif(value_basis='payment') payment_transactions,
       sum(if(value_basis='payment',amount_minor,0)) payment_minor
from {{ ref('fct_stream_events') }}
where value_basis in ('contract','payment')
group by tenant,legal_entity,project_id,currency
