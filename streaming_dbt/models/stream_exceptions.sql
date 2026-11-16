select bound_tenant tenant,bound_source source,business_id,'invalid_contract' reason
from {{ ref('stg_stream') }} where not valid
union all
select tenant,source,business_id,'conflicting_revision' reason
from {{ ref('stg_stream') }} where valid
group by tenant,source,business_id,event_type,revision
having count(distinct meaning_hash)>1
union all
select tenant,source,business_id,'duplicate_transaction_identity' reason
from {{ ref('fct_stream_events') }} where transaction_id is not null
qualify count(*) over(partition by tenant,legal_entity,transaction_id)>1
union all
select p.tenant,p.source,p.business_id,'payment_without_sufficient_contract' reason
from {{ ref('fct_stream_events') }} p
left join (select tenant,legal_entity,project_id,sum(amount_minor) signed_minor,count(*) n
           from {{ ref('fct_stream_events') }} where value_basis='contract' group by 1,2,3) c
using(tenant,legal_entity,project_id)
where p.value_basis='payment'
qualify c.n is null or c.n<>1 or sum(p.amount_minor) over(partition by p.tenant,p.legal_entity,p.project_id)>c.signed_minor
