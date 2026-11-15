select tenant,legal_entity,date(event_time) day,currency,value_basis,event_type,
       count(*) events,sum(amount_minor) amount_minor
from {{ ref('fct_stream_events') }} group by 1,2,3,4,5,6
