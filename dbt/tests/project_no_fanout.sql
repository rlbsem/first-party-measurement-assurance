select 1 as violation where
(select sum(payment_cents) from {{ ref('mart_project_values') }}) <>
(select sum(value_cents) from {{ ref('source_facts') }} where stage='payment')
or (select sum(signed_contract_cents) from {{ ref('mart_project_values') }}) <>
(select sum(value_cents) from {{ ref('source_facts') }} where stage='signed_contract')
