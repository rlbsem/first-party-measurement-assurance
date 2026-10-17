select e.event_id from {{ ref('fct_events') }} e
left join {{ ref('fct_items') }} i using(event_id)
where e.event_name='purchase'
group by e.event_id,e.value_cents
having sum(i.extended_cents) is null or sum(i.extended_cents)<>e.value_cents
