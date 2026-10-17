with truth as (
  select project_id,
    sum(case when stage='proposal' then value_cents else 0 end)::bigint as proposal_cents,
    sum(case when stage='signed_contract' then value_cents else 0 end)::bigint as signed_contract_cents,
    sum(case when stage='payment' then value_cents else 0 end)::bigint as payment_cents
  from {{ ref('source_facts') }} group by project_id
), measured as (
  select project_id,
    sum(case when value_basis='contract' then value_cents else 0 end)::bigint as measured_contract_cents,
    sum(case when value_basis='payment' then value_cents else 0 end)::bigint as measured_payment_cents
  from {{ ref('fct_conversions') }} group by project_id
)
select p.project_id,p.project_type,p.opportunity_cents,t.proposal_cents,t.signed_contract_cents,t.payment_cents,
       coalesce(m.measured_contract_cents,0) as measured_contract_cents,
       coalesce(m.measured_payment_cents,0) as measured_payment_cents
from {{ ref('projects') }} p left join truth t using(project_id) left join measured m using(project_id)
