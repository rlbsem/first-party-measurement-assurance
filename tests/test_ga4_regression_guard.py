"""Execute the actual singular-test SQL against independently altered warehouse facts."""
from copy import deepcopy

import duckdb
import pytest
from jinja2 import StrictUndefined, Template

from scripts.bigquery_oracle import fixture_rows
from scripts.verify_bigquery import EVENT_FIELDS, ROOT, landing_rows


@pytest.mark.parametrize('field,value', [
    ('event_name', 'page_view'), ('user_pseudo_id', 'another-user'),
    ('session_id', 'another-session'), ('transaction_id', 'another-transaction'),
    ('event_date', '2020-01-01'), ('arrival_batch', 1), ('source_version', 99),
    ('revenue_cents', 1), ('event_key', 'unknown-event'),
    ('user_pseudo_id', None), ('transaction_id', None), ('session_id', None),
    ('extra_future_event', None), ('missing_event', None), ('none', None),
])
def test_full_event_slice_guard(field, value):
    facts = deepcopy(fixture_rows(2)['fct_ga4_events'])
    target = next(r for r in facts if r['event_key'] == 'ev03')
    if field == 'extra_future_event':
        facts.append({**target, 'event_key': 'future-admission', 'arrival_batch': 3})
    elif field == 'missing_event':
        facts.remove(target)
    elif field != 'none':
        target[field] = value
    with duckdb.connect() as db:
        schema = ','.join(f'{k} {"VARCHAR" if t == "STRING" else t}' for k, t in EVENT_FIELDS)
        for table, rows in [('stg_event_versions', landing_rows(1)['event_versions'] + landing_rows(2)['event_versions']),
                            ('fct_ga4_events', facts)]:
            db.execute(f'create table {table} ({schema})')
            db.executemany(f'insert into {table} values ({",".join("?" for _ in EVENT_FIELDS)})',
                           [[r[k] for k, _ in EVENT_FIELDS] for r in rows])
        query = Template((ROOT / 'ga4_pipeline/tests/assert_no_regression.sql').read_text(),
                         undefined=StrictUndefined).render(ref=lambda name: name, var=lambda *args: 2)
        findings = db.execute(query).fetchall()
    assert bool(findings) == (field != 'none')
