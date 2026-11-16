select e.tenant,e.legal_entity,e.source,e.business_id,e.revision,i.*
from {{ ref('fct_stream_events') }} e cross join unnest(e.items) i
