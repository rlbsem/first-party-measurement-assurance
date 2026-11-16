select tenant,legal_entity,session_id
from {{ ref('fct_stream_sessions') }}
group by tenant,legal_entity,session_id
having count(*)<>1
