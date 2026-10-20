-- Session grain is pseudo-user plus session identifier, not session_id alone.
select user_pseudo_id, session_id,
       min(event_date) as first_event_date, max(event_date) as last_event_date,
       count(*) as event_count,
       sum(case when event_name = 'purchase' then 1 else 0 end) as purchase_events
from {{ ref('fct_ga4_events') }}
where user_pseudo_id is not null and session_id is not null
group by user_pseudo_id, session_id
