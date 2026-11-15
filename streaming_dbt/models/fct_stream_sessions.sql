-- Session grain is distinct from business events, transactions and signed value.
select tenant,legal_entity,session_id,
       min(event_time) first_event_time,max(event_time) last_event_time,
       count(*) canonical_events,
       countif(event_type='generate_lead' and eligible) eligible_inquiries
from {{ ref('fct_stream_events') }}
where session_id is not null
group by tenant,legal_entity,session_id
