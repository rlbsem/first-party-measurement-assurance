"""Offline render of existing dbt SELECT bodies and a separate raw-source oracle.

This is Jinja rendering, NOT dbt compilation, materialization or MERGE execution.
Only SELECT statements are produced. No Google clients or credentials are used.
"""
import argparse
import datetime as dt
import hashlib
import json
import re
from pathlib import Path
from types import SimpleNamespace

from jinja2 import Environment, StrictUndefined

ROOT = Path(__file__).resolve().parents[1]
MODELS = ('stg_event_versions', 'stg_item_versions', 'fct_ga4_events', 'fct_ga4_items',
          'mart_ga4_purchase_quality', 'mart_ga4_sessions', 'mart_ga4_daily')
EVENT_COLUMNS = 'event_date arrival_batch source_version event_name user_pseudo_id session_id transaction_id revenue_cents'
ITEM_COLUMNS = ('event_date event_name user_pseudo_id session_id transaction_id revenue_cents '
                'item_index item_id price_cents quantity extended_cents source_version')
PURCHASE_COLUMNS = ('event_date arrival_batch transaction_id user_pseudo_id session_id revenue_cents '
                    'item_rows item_value_cents revenue_variance_cents transaction_id_finding identity_finding revenue_finding')
SESSION_COLUMNS = 'user_pseudo_id session_id first_event_date last_event_date event_count purchase_events'
DAILY_COLUMNS = ('event_date event_count purchase_events purchase_item_rows reported_revenue_cents '
                 'item_value_cents transaction_id_findings revenue_findings identity_findings')


def source(shard):
    if not re.fullmatch(r'[0-9]{8}', shard):
        raise ValueError('One literal YYYYMMDD shard required')
    parsed = dt.datetime.strptime(shard, '%Y%m%d').date()
    if not dt.date(2020, 11, 1) <= parsed <= dt.date(2021, 1, 31):
        raise ValueError('Shard outside official public sample')
    return f"""-- Read-only public GA4 sample; ONE SHARD and purchase-user cohort, not the full export.
-- USD values use the provider's *_in_usd fields. Unknowns remain NULL.
with raw_day as (
  select event_date, event_timestamp, event_name, user_pseudo_id,
         event_bundle_sequence_id, event_server_timestamp_offset,
         (select p.value.int_value from unnest(event_params) p where p.key='ga_session_id') as source_session_id,
         ecommerce.transaction_id as source_transaction_id,
         ecommerce.purchase_revenue_in_usd as revenue_usd, items
  from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  where _TABLE_SUFFIX = '{shard}'
),
cohort as (
  select * from raw_day
  where event_name='purchase'
     or user_pseudo_id in (select user_pseudo_id from raw_day where event_name='purchase')
)
"""


def model_bodies():
    env = Environment(undefined=StrictUndefined)
    env.globals.update(target=SimpleNamespace(type='bigquery'), ref=lambda name: name,
                       source=lambda group, name: 'landing_' + ('events' if name == 'event_versions' else 'items'),
                       config=lambda **kwargs: '', var=lambda name, default=None: 1,
                       is_incremental=lambda: False)
    macros = env.from_string((ROOT / 'ga4_pipeline/macros/source_relations.sql').read_text()).make_module()
    for name in ('event_versions_relation', 'item_versions_relation', 'ga4_integer'):
        env.globals[name] = getattr(macros, name)
    result = []
    for name in MODELS:
        path = ROOT / 'ga4_pipeline/models' / ('staging' if name.startswith('stg_') else '') / (name + '.sql')
        result.append(f'{name} as (\n{env.from_string(path.read_text()).render().strip()}\n)')
    return result


def signatures(relations):
    parts = []
    for model, relation, columns in relations:
        fields = ', '.join('r.' + name for name in columns.split())
        parts.append(f"""select '{model}' as model, count(*) as row_count,
  to_hex(sha256(coalesce(string_agg(row_hash, '\\n' order by row_hash), ''))) as multiset_sha256
from (select to_hex(sha256(to_json_string(struct({fields})))) as row_hash from {relation} r)""")
    return '\nunion all\n'.join(parts) + '\norder by model\n'


def queries(shard='20210101'):
    common = source(shard)
    snapshot = """, snapshot_payloads as (
  select c.*, to_hex(sha256(to_json_string(c))) as content_hash from cohort c
), snapshot_rows as (
  -- Preserve identical/repeated rows. These are snapshot-local keys, NOT canonical GA4 event IDs.
  select *, concat(content_hash, ':', cast(row_number() over (partition by content_hash order by event_timestamp) as string)) as event_key
  from snapshot_payloads
), landing_events as (
  select event_key, parse_date('%Y%m%d',event_date) as event_date, 1 as arrival_batch,
         1 as source_version, event_name, user_pseudo_id,
         cast(source_session_id as string) as session_id,
         nullif(source_transaction_id,'') as transaction_id,
         case when event_name='purchase' then cast(round(revenue_usd*100) as int64) else 0 end as revenue_cents
  from snapshot_rows
), landing_items as (
  select e.event_key, 1 as source_version, item_offset as item_index, i.item_id,
         cast(round(i.price_in_usd*100) as int64) as price_cents, i.quantity
  from snapshot_rows e cross join unnest(e.items) i with offset as item_offset
),
"""
    item_view = """preview_items as (
 select e.event_date,e.event_name,e.user_pseudo_id,e.session_id,e.transaction_id,e.revenue_cents,
        i.item_index,i.item_id,i.price_cents,i.quantity,i.extended_cents,i.source_version
 from fct_ga4_items i join fct_ga4_events e on i.event_key=e.event_key
)"""
    model_query = common + snapshot + ',\n'.join(model_bodies() + [item_view]) + '\n'
    model_query += signatures([
        ('events', 'fct_ga4_events', EVENT_COLUMNS), ('items', 'preview_items', ITEM_COLUMNS),
        ('purchases', 'mart_ga4_purchase_quality', PURCHASE_COLUMNS),
        ('sessions', 'mart_ga4_sessions', SESSION_COLUMNS), ('daily', 'mart_ga4_daily', DAILY_COLUMNS)])
    # Independent source logic never reads any modeled relation or rendered dbt body.
    reference = """, source_events as (
  select parse_date('%Y%m%d',event_date) as event_date, 1 as arrival_batch, 1 as source_version,
         event_name, nullif(trim(user_pseudo_id),'') as user_pseudo_id,
         nullif(trim(cast(source_session_id as string)),'') as session_id,
         nullif(trim(source_transaction_id),'') as transaction_id,
         case when event_name='purchase' then cast(round(revenue_usd*100) as int64) else 0 end as revenue_cents,
         items
  from cohort
), source_items as (
  select e.event_date,e.event_name,e.user_pseudo_id,e.session_id,e.transaction_id,e.revenue_cents,
         item_offset as item_index, i.item_id, cast(round(i.price_in_usd*100) as int64) as price_cents,
         i.quantity, cast(round(i.price_in_usd*100) as int64)*i.quantity as extended_cents, 1 as source_version
  from source_events e cross join unnest(e.items) i with offset as item_offset
), raw_purchases as (
  select *, (select count(*) from unnest(items)) as item_rows,
         (select sum(cast(round(i.price_in_usd*100) as int64)*i.quantity) from unnest(items) i) as source_item_value,
         count(*) over (partition by transaction_id) as tx_rows
  from source_events where event_name='purchase'
), source_purchases as (
  select event_date,arrival_batch,transaction_id,user_pseudo_id,session_id,revenue_cents,item_rows,
         coalesce(source_item_value,0) as item_value_cents,
         revenue_cents-coalesce(source_item_value,0) as revenue_variance_cents,
         case when transaction_id is null or tx_rows>1 then 1 else 0 end as transaction_id_finding,
         case when user_pseudo_id is null or session_id is null then 1 else 0 end as identity_finding,
         case when revenue_cents is null or source_item_value is null
                   or abs(revenue_cents-source_item_value)>1 then 1 else 0 end as revenue_finding
  from raw_purchases
), source_sessions as (
  select user_pseudo_id,session_id,min(event_date) as first_event_date,max(event_date) as last_event_date,
         count(*) as event_count,countif(event_name='purchase') as purchase_events
  from source_events where user_pseudo_id is not null and session_id is not null
  group by user_pseudo_id,session_id
), source_daily as (
  select event_date,count(*) as event_count,countif(event_name='purchase') as purchase_events,
         sum(if(event_name='purchase',array_length(items),0)) as purchase_item_rows,
         sum(if(event_name='purchase',coalesce(revenue_cents,0),0)) as reported_revenue_cents,
         sum(if(event_name='purchase',coalesce((select sum(cast(round(i.price_in_usd*100) as int64)*i.quantity) from unnest(items) i),0),0)) as item_value_cents,
         sum(case when event_name='purchase' and (transaction_id is null or transaction_id in
              (select transaction_id from source_events where event_name='purchase' group by transaction_id having count(*)>1)) then 1 else 0 end) as transaction_id_findings,
         sum(case when event_name='purchase' and (revenue_cents is null or array_length(items)=0
              or (select sum(cast(round(i.price_in_usd*100) as int64)*i.quantity) from unnest(items) i) is null
              or abs(revenue_cents-(select sum(cast(round(i.price_in_usd*100) as int64)*i.quantity) from unnest(items) i))>1) then 1 else 0 end) as revenue_findings,
         countif(user_pseudo_id is null or session_id is null) as identity_findings
  from source_events group by event_date
)
"""
    reference_query = common + reference + signatures([
        ('events', 'source_events', EVENT_COLUMNS), ('items', 'source_items', ITEM_COLUMNS),
        ('purchases', 'source_purchases', PURCHASE_COLUMNS), ('sessions', 'source_sessions', SESSION_COLUMNS),
        ('daily', 'source_daily', DAILY_COLUMNS)])
    return {'warehouse_models_preview': model_query, 'warehouse_source_oracle': reference_query}


def write_queries(output, shard='20210101'):
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    record = {'shard': shard, 'scope': 'purchase-user cohort; read-only SQL SELECT bodies, not dbt execution', 'sha256': {}}
    for name, sql in queries(shard).items():
        path = output / (name + '.sql')
        path.write_text(sql, encoding='utf-8', newline='\n')
        record['sha256'][path.name] = hashlib.sha256(path.read_bytes()).hexdigest()
    return record


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=ROOT / '.local/warehouse-preview')
    parser.add_argument('--shard', default='20210101')
    args = parser.parse_args()
    print(json.dumps(write_queries(args.output, args.shard)))
