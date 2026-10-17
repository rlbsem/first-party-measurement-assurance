select event_id, transaction_id, event_date, session_id, currency, value_cents, campaign
from {{ ref('fct_events') }} where event_name = 'purchase'
