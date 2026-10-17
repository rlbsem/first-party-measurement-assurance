{{ config(materialized='incremental', unique_key='event_id', incremental_strategy='delete+insert') }}
select event_id, session_id, event_name, origin, source_ref, project_id, project_type, value_basis,
       cast(occurred_at as timestamp) as occurred_at, cast(received_at as timestamp) as received_at,
       cast(occurred_at as date) as event_date, cast(value_cents as bigint) as value_cents,
       currency, campaign, transaction_id, cast(late as boolean) as is_late, payload_hash
from {{ ref('accepted_events') }}
-- Re-read the bounded immutable ledger, including late events behind event-time watermarks.
