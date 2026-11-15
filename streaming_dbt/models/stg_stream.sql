-- Source identity is bound to an owned subscription, not JSON assertions.
with bindings as (
{% for b in var('bindings') %}
  select '{{ b.subscription }}' subscription_name, '{{ b.source }}' bound_source,
         '{{ b.tenant }}' bound_tenant, '{{ b.legal_entity }}' bound_entity,
         '{{ b.kind }}' source_kind, '{{ b.policy_version }}' bound_policy
  {% if not loop.last %}union all{% endif %}
{% endfor %}
), raw as (
  select r.*, safe.parse_json(data) j
  from `{{ var('project') }}.{{ var('raw_dataset') }}.{{ var('lane') }}` r
  where json_value(safe.parse_json(attributes),'$.cohort')='{{ var("cohort") }}'
), parsed as (
  select r.*, b.* except(subscription_name),
    json_value(j,'$.schema_version') schema_version,
    json_value(j,'$.tenant') tenant, json_value(j,'$.legal_entity') legal_entity,
    json_value(j,'$.source') source, json_value(j,'$.business_id') business_id,
    safe_cast(json_value(j,'$.revision') as int64) revision,
    json_value(j,'$.event_id') event_id, json_value(j,'$.event_type') event_type,
    safe_cast(json_value(j,'$.event_time') as timestamp) event_time,
    json_value(j,'$.project_id') project_id, json_value(j,'$.session_id') session_id,
    json_value(j,'$.transaction_id') transaction_id, json_value(j,'$.currency') currency,
    json_value(j,'$.value_basis') value_basis,
    safe_cast(json_value(j,'$.amount_minor') as int64) amount_minor,
    safe_cast(json_value(j,'$.eligible') as bool) eligible,
    json_value(j,'$.policy_version') policy_version,
    array(select as struct safe_cast(json_value(i,'$.index') as int64) as index,
          json_value(i,'$.item_id') item_id, safe_cast(json_value(i,'$.price_minor') as int64) price_minor,
          safe_cast(json_value(i,'$.quantity') as int64) quantity
          from unnest(json_query_array(j,'$.items')) i) items
  from raw r left join bindings b using(subscription_name)
), checked as (
 select *,
  coalesce(schema_version='measurement-stream.v1'
   and array_to_string(array(select k from unnest(json_keys(j,1)) k order by k),',')=
     'amount_minor,business_id,currency,eligible,event_id,event_time,event_type,items,legal_entity,policy_version,project_id,revision,schema_version,session_id,source,tenant,transaction_id,value_basis'
   and source=bound_source and tenant=bound_tenant and legal_entity=bound_entity and policy_version=bound_policy
   and ((source_kind='web' and event_type='generate_lead') or (source_kind='commerce' and event_type in ('qualify_lead','working_lead','disqualify_lead','close_convert_lead','purchase')))
   and json_type(json_query(j,'$.revision'))='number'
   and json_type(json_query(j,'$.amount_minor'))='number'
   and json_type(json_query(j,'$.items'))='array'
   and revision between 1 and 1000000 and regexp_contains(json_value(j,'$.revision'),r'^[0-9]+$')
   and amount_minor between 0 and 1000000000000 and regexp_contains(json_value(j,'$.amount_minor'),r'^[0-9]+$')
   and json_type(json_query(j,'$.eligible'))='boolean'
   and regexp_contains(json_value(j,'$.event_time'),r'(Z|[+-][0-9]{2}:[0-9]{2})$')
   and event_time<=timestamp_add(publish_time,interval 120 second)
   and value_basis=case event_type when 'purchase' then 'payment' when 'close_convert_lead' then 'contract' else 'unvalued' end
   and ((value_basis='unvalued' and currency is null and amount_minor=0 and array_length(items)=0 and transaction_id is null)
     or (value_basis='contract' and currency='USD' and amount_minor>0 and array_length(items)=0 and transaction_id is null)
     or (value_basis='payment' and currency='USD' and amount_minor>0 and length(transaction_id) between 1 and 100
       and array_length(items) between 1 and 100
       and (select sum(safe_multiply(i.price_minor,i.quantity)) from unnest(items) i)=amount_minor
       and not exists(select 1 from unnest(items) i where i.index not between 0 and 99 or i.price_minor not between 0 and 1000000000000 or i.quantity not between 1 and 1000000 or i.index is null or i.price_minor is null or i.quantity is null or not regexp_contains(i.item_id,r'^[A-Za-z0-9_.:-]{1,100}$') or i.item_id is null)
       and not exists(select 1 from unnest(json_query_array(j,'$.items')) i where array_to_string(array(select k from unnest(json_keys(i,1)) k order by k),',')!='index,item_id,price_minor,quantity' or json_type(json_query(i,'$.index'))!='number' or json_type(json_query(i,'$.price_minor'))!='number' or json_type(json_query(i,'$.quantity'))!='number' or not regexp_contains(json_value(i,'$.index'),r'^[0-9]+$') or not regexp_contains(json_value(i,'$.price_minor'),r'^[0-9]+$') or not regexp_contains(json_value(i,'$.quantity'),r'^[0-9]+$'))
       and (select count(distinct i.index) from unnest(items) i)=array_length(items)))
   and (json_type(json_query(j,'$.tenant'))='string' and regexp_contains(json_value(j,'$.tenant'),r'^[A-Za-z0-9_.:-]{1,100}$')) and (json_type(json_query(j,'$.legal_entity'))='string' and regexp_contains(json_value(j,'$.legal_entity'),r'^[A-Za-z0-9_.:-]{1,100}$')) and (json_type(json_query(j,'$.source'))='string' and regexp_contains(json_value(j,'$.source'),r'^[A-Za-z0-9_.:-]{1,100}$')) and (json_type(json_query(j,'$.business_id'))='string' and regexp_contains(json_value(j,'$.business_id'),r'^[A-Za-z0-9_.:-]{1,100}$')) and (json_type(json_query(j,'$.event_id'))='string' and regexp_contains(json_value(j,'$.event_id'),r'^[A-Za-z0-9_.:-]{1,100}$')) and (json_type(json_query(j,'$.project_id'))='string' and regexp_contains(json_value(j,'$.project_id'),r'^[A-Za-z0-9_.:-]{1,100}$')) and (json_type(json_query(j,'$.policy_version'))='string' and regexp_contains(json_value(j,'$.policy_version'),r'^[A-Za-z0-9_.:-]{1,100}$'))
   and (session_id is null or (json_type(json_query(j,'$.session_id'))='string' and regexp_contains(session_id,r'^[A-Za-z0-9_.:-]{1,100}$')))
   and (transaction_id is null or (json_type(json_query(j,'$.transaction_id'))='string' and regexp_contains(transaction_id,r'^[A-Za-z0-9_.:-]{1,100}$'))),
   false) valid
 from parsed
)
select *, to_hex(sha256(to_json_string(struct(tenant,legal_entity,source,business_id,revision,event_type,event_time,project_id,session_id,transaction_id,currency,value_basis,amount_minor,items,eligible,policy_version)))) meaning_hash
from checked
