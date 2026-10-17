select event_name || '|' || source_ref as conversion_key, event_id, source_ref, project_id,
       event_name, event_date, origin, value_basis, currency, value_cents, campaign, is_late
from {{ ref('fct_events') }}
