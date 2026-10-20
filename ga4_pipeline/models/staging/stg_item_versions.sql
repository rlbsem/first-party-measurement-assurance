select
  cast(event_key as string) as event_key,
  cast(source_version as {{ ga4_integer() }}) as source_version,
  cast(item_index as {{ ga4_integer() }}) as item_index,
  cast(item_id as string) as item_id,
  cast(price_cents as {{ ga4_integer() }}) as price_cents,
  cast(quantity as {{ ga4_integer() }}) as quantity
from {{ item_versions_relation() }}
