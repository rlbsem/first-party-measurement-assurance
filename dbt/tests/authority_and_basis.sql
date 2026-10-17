select e.event_id from {{ ref('fct_events') }} e
join {{ ref('source_facts') }} s using(source_ref)
where (e.event_name='generate_lead' and e.origin<>'browser')
   or (e.event_name<>'generate_lead' and e.origin<>'offline')
   or (e.event_name='purchase' and (e.value_basis<>'payment' or s.stage<>'payment'))
   or (e.event_name='close_convert_lead' and (e.value_basis<>'contract' or s.stage<>'signed_contract'))
   or (e.value_basis='unvalued' and e.value_cents<>0)
   or e.currency<>'USD'
