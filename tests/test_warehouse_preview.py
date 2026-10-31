"""Offline rendering is kept distinct from real dbt or Google execution."""
import sys

import duckdb
import pytest

from scripts.bigquery_oracle import compare, fixture_rows
from scripts.verify_bigquery import EVENT_FIELDS, ITEM_FIELDS, landing_rows
from scripts.warehouse_preview import MODELS, ROOT, model_bodies, queries


def test_actual_model_bodies_match_complete_independent_oracle():
    with duckdb.connect() as db:
        for name, fields, rows in [('landing_events', EVENT_FIELDS, landing_rows(1)['event_versions']),
                                   ('landing_items', ITEM_FIELDS, landing_rows(1)['item_versions'])]:
            schema = ','.join(f'{k} {"VARCHAR" if t == "STRING" else t}' for k, t in fields)
            db.execute(f'create table {name} ({schema})')
            db.executemany(f'insert into {name} values ({",".join("?" for _ in fields)})',
                           [[r[k] for k, _ in fields] for r in rows])
        for name, body in zip(MODELS, model_bodies()):
            sql = body[len(name) + 5:-1].strip()
            db.execute(f'create view {name} as {sql}')
        actual = {}
        for name in fixture_rows(1):
            cursor = db.execute(f'select * from {name}')
            actual[name] = [dict(zip([c[0] for c in cursor.description], row)) for row in cursor.fetchall()]
    compare(actual, fixture_rows(1))


@pytest.mark.parametrize('shard', ['20210101 OR 1=1', '2021011', '20210201', '20201031', '20211301'])
def test_only_one_valid_public_shard_is_rendered(shard):
    with pytest.raises(ValueError):
        queries(shard)


def test_preview_never_authenticates_and_preserves_duplicate_multiplicity(monkeypatch):
    monkeypatch.setitem(sys.modules, 'google.auth', None)
    result = queries()
    assert len(result) == 2
    model, oracle = result.values()
    assert 'row_number() over (partition by content_hash' in model
    assert 'distinct' not in model.lower().split('snapshot_payloads as')[1].split('landing_events as')[0]
    assert 'source_purchases' in oracle and 'fct_ga4_events' not in oracle
    for query in result.values():
        assert "_TABLE_SUFFIX = '20210101'" in query
        assert 'revenue_in_usd' in query and 'price_in_usd' in query
        assert 'use_legacy_sql' not in query
        assert 'string_agg(row_hash' in query and 'order by row_hash' in query


def test_checked_sql_matches_actual_sources():
    for name, query in queries().items():
        assert (ROOT / 'bigquery' / (name + '.sql')).read_text() == query


def test_sessions_cross_midnight_users_and_repeated_purchase_events():
    # Same session ID across people is two sessions; crossing midnight is not a new session.
    events = [
        ['a1','2026-09-30',1,1,'page_view','a','123',None,0],
        ['a2','2026-10-01',1,1,'purchase','a','123','repeated',1000],
        ['a3','2026-10-01',1,1,'purchase','a','123','repeated',1000],
        ['b1','2026-10-01',1,1,'purchase','b','123','other',2000],
    ]
    items = [['a2',1,0,'x',1000,1], ['a3',1,0,'x',1000,1], ['b1',1,0,'y',2000,1]]
    with duckdb.connect() as db:
        for table, fields, rows in [('landing_events', EVENT_FIELDS, events), ('landing_items', ITEM_FIELDS, items)]:
            schema = ','.join(f'{k} {"VARCHAR" if t == "STRING" else t}' for k, t in fields)
            db.execute(f'create table {table} ({schema})')
            db.executemany(f'insert into {table} values ({",".join("?" for _ in fields)})', rows)
        for name, body in zip(MODELS, model_bodies()):
            db.execute(f'create view {name} as {body[len(name)+5:-1].strip()}')
        assert db.execute('select count(*) from fct_ga4_events').fetchone()[0] == 4
        sessions = db.execute('select user_pseudo_id,event_count,cast(first_event_date as varchar),'
                              'cast(last_event_date as varchar) from mart_ga4_sessions order by 1').fetchall()
        assert sessions == [('a',3,'2026-09-30','2026-10-01'), ('b',1,'2026-10-01','2026-10-01')]
        assert db.execute('select count(*),sum(revenue_cents),sum(transaction_id_finding) '
                          'from mart_ga4_purchase_quality').fetchone() == (3,4000,2)
