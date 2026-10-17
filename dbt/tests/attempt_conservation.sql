select 1 as violation
where (select count(*) from {{ ref('collection_decisions') }}) <>
      (select coalesce(sum(attempts),0) from {{ ref('mart_health') }})
or (select count(*) from {{ ref('fct_events') }}) <>
   (select count(*) from {{ ref('collection_decisions') }} where status in ('accepted','accepted_late'))
