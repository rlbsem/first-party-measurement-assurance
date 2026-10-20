-- Reject incomplete normalized rows; NULL item amounts must never disappear
-- inside SUM(). Missing user/session identity remains a disclosed finding.
select event_key
from {{ ref('stg_event_versions') }}
where arrival_batch <= {{ var('active_batch', 2) }}
  and (event_key is null or trim(event_key)='' or event_date is null
       or event_name is null or arrival_batch < 1 or source_version < 1)
union all
select event_key
from {{ ref('stg_item_versions') }}
where event_key is null or source_version is null or source_version < 1
   or item_index is null or item_index < 0 or price_cents is null or price_cents < 0
   or quantity is null or quantity <= 0
