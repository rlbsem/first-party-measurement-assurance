select tenant,legal_entity,source,event_type,business_id
from {{ ref('fct_stream_events') }}
group by tenant,legal_entity,source,event_type,business_id
having count(*)<>1
