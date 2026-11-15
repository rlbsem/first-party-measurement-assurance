select schema_version,tenant,legal_entity,source,business_id,revision,event_type,event_time,
       project_id,session_id,transaction_id,currency,value_basis,amount_minor,items,eligible,policy_version
from {{ ref('stg_stream') }}
where valid
qualify row_number() over(partition by tenant,legal_entity,source,event_type,business_id order by revision desc,publish_time desc,message_id desc)=1
