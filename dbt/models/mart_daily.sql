select event_date, currency, value_basis, event_name, count(*) as accepted_events,
       sum(value_cents) as measured_cents, sum(case when is_late then 1 else 0 end) as late_events
from {{ ref('fct_events') }} group by event_date,currency,value_basis,event_name
-- Value basis is part of the grain: never combine contract and payment values.
