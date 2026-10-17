select campaign, currency, value_basis, event_name, count(*) as conversions,
       sum(value_cents) as measured_cents
from {{ ref('fct_conversions') }} group by campaign,currency,value_basis,event_name
-- Descriptive conversion context only; not causal or cross-device attribution.
