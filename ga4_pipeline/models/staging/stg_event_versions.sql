select
  cast(event_key as string) as event_key,
  cast(event_date as date) as event_date,
  cast(arrival_batch as {{ ga4_integer() }}) as arrival_batch,
  cast(source_version as {{ ga4_integer() }}) as source_version,
  cast(event_name as string) as event_name,
  nullif(trim(cast(user_pseudo_id as string)), '') as user_pseudo_id,
  nullif(trim(cast(session_id as string)), '') as session_id,
  nullif(trim(cast(transaction_id as string)), '') as transaction_id,
  cast(revenue_cents as {{ ga4_integer() }}) as revenue_cents
from {{ event_versions_relation() }}
