with expected as (
  select *, case stage when 'inquiry' then 'generate_lead' when 'qualified' then 'qualify_lead'
    when 'working' then 'working_lead' when 'disqualified' then 'disqualify_lead'
    when 'signed_contract' then 'close_convert_lead' when 'payment' then 'purchase' end as event_name
  from {{ ref('source_facts') }} where stage <> 'proposal'
), keyed as (select *, event_name || '|' || source_ref as conversion_key from expected)
select coalesce(s.conversion_key,m.conversion_key) as conversion_key,
       coalesce(s.project_id,m.project_id) as project_id, coalesce(s.source_ref,m.source_ref) as source_ref,
       coalesce(s.event_name,m.event_name) as event_name,
       case coalesce(s.event_name,m.event_name) when 'purchase' then 'payment'
            when 'close_convert_lead' then 'contract' else 'unvalued' end as value_basis,
       cast(s.value_cents as bigint) as expected_cents, m.value_cents as measured_cents,
       coalesce(m.value_cents,0)-coalesce(cast(s.value_cents as bigint),0) as delta_cents,
       case when s.conversion_key is null then 'unexpected'
            when s.measurement_eligible=0 and m.conversion_key is null then 'not_eligible'
            when m.conversion_key is null then 'missing'
            when s.value_cents<>m.value_cents or s.currency<>m.currency then 'value_mismatch'
            else 'matched' end as status
from keyed s full outer join {{ ref('fct_conversions') }} m using(conversion_key)
