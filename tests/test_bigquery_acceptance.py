"""Offline safety/contract tests. No Google credentials or native jobs are used."""
from copy import deepcopy
import datetime as dt
from types import SimpleNamespace

import pytest

from scripts import bigquery_oracle as oracle
from scripts import verify_bigquery as native
from scripts import package


NOW = dt.datetime(2026, 10, 20, tzinfo=dt.timezone.utc)


def approved_plan():
    return native.plan('example-approved-project', 'offline_check', NOW)


def test_offline_plan_does_not_import_or_authenticate_google(monkeypatch):
    import sys
    monkeypatch.setitem(sys.modules, 'google.auth', None)
    value = approved_plan()
    native.validate_approval(value, native.digest(value), NOW)
    assert value['maximum_bytes_billed'] == 100_000_000
    assert value['requires_billing_enabled'] is True
    assert len(value['datasets']) == 3


@pytest.mark.parametrize('field,value', [
    ('maximum_bytes_billed', 0), ('threads', 8), ('dbt_job_retries', 1),
    ('requires_billing_enabled', False),
    ('datasets', {'landing': 'existing_data'}), ('source_sha256', {}),
])
def test_edited_or_stale_plan_rejected_even_with_new_digest(field, value):
    plan = approved_plan()
    plan[field] = value
    with pytest.raises(ValueError):
        native.validate_approval(plan, native.digest(plan), NOW)


def test_wrong_approval_digest_and_expiration_rejected():
    value = approved_plan()
    with pytest.raises(ValueError, match='digest'):
        native.validate_approval(value, '0' * 64, NOW)
    with pytest.raises(ValueError, match='expired'):
        native.validate_approval(value, native.digest(value), NOW + dt.timedelta(days=1))
    changed = {**value, 'project': 'different-approved-project'}
    with pytest.raises(ValueError, match='digest'):
        native.validate_approval(changed, native.digest(value), NOW)


@pytest.mark.parametrize('project,run', [
    ('Display Name', 'safe_run'), ('abc` DROP', 'safe_run'),
    ('example-project', 'bad-name'), ('example-project', '../../data'),
])
def test_sql_identifiers_are_pinned(project, run):
    with pytest.raises(ValueError):
        native.plan(project, run, NOW)


def test_runtime_output_cannot_target_public_evidence():
    with pytest.raises(ValueError):
        native.private_path(native.ROOT / 'docs/evidence/unreviewed.json')


def test_physical_admission_preserves_batches_and_superseded_items():
    first, second = native.landing_rows(1), native.landing_rows(2)
    assert len(first['event_versions']) == 5 and len(second['event_versions']) == 6
    assert len(first['item_versions']) == 4 and len(second['item_versions']) == 5
    assert next(r for r in second['event_versions'] if r['event_key'] == 'ev03')['source_version'] == 2
    assert any(r['event_date'] == '2026-09-26' for r in second['event_versions'])
    assert next(r for r in first['event_versions'] if r['event_key'] == 'ev01')['transaction_id'] is None


@pytest.mark.parametrize('model', native.MODELS)
def test_complete_oracle_detects_duplicate_rows(model):
    correct = oracle.fixture_rows(2)
    altered = deepcopy(correct)
    altered[model].append(deepcopy(altered[model][0]))
    with pytest.raises(AssertionError, match=model):
        oracle.compare(altered, correct)


@pytest.mark.parametrize('model,field,value', [
    ('fct_ga4_events', 'session_id', 'wrong-session'),
    ('fct_ga4_items', 'item_id', 'wrong-item'),
    ('mart_ga4_sessions', 'last_event_date', '2099-01-01'),
    ('mart_ga4_purchase_quality', 'revenue_variance_cents', -500),
    ('mart_ga4_daily', 'purchase_item_rows', 999),
])
def test_complete_oracle_detects_field_corruption(model, field, value):
    correct = oracle.fixture_rows(2)
    altered = deepcopy(correct)
    altered[model][0][field] = value
    with pytest.raises(AssertionError, match=model):
        oracle.compare(altered, correct)


def test_dataset_delete_requires_ownership_and_location():
    labels = {'assurance_run': 'safe_run', 'plan_sha': 'a' * 32}
    native.assert_owned(SimpleNamespace(labels=labels, location='US'), labels, 'US')
    for observed, location in (({}, 'US'), (labels, 'EU'), ({**labels, 'plan_sha': 'b' * 32}, 'US')):
        with pytest.raises(RuntimeError, match='prohibited'):
            native.assert_owned(SimpleNamespace(labels=observed, location=location), labels, 'US')


@pytest.mark.parametrize('info', [{}, {'billingEnabled': False}, {'billingEnabled': 'true'}])
def test_sandbox_or_unknown_billing_cannot_execute_merge(info):
    with pytest.raises(RuntimeError, match='cannot execute'):
        native.require_billing(info)


def test_read_only_billing_check_does_not_upgrade_project():
    native.require_billing({'billingEnabled': True})


def test_collision_preflight_never_marks_existing_dataset_for_cleanup(tmp_path):
    run = native.NativeRun.__new__(native.NativeRun)
    run.plan, run.created = approved_plan(), []
    class NotFound(Exception):
        pass
    run.not_found = NotFound
    run.client = SimpleNamespace(get_dataset=lambda *args, **kwargs: object())
    with pytest.raises(RuntimeError, match='collision'):
        run.create()
    assert run.created == []


def test_uncertain_job_receipt_prohibits_cleanup(monkeypatch):
    run = native.NativeRun.__new__(native.NativeRun)
    run.created = ['landing']
    def unavailable():
        raise TimeoutError('job metadata unavailable')
    monkeypatch.setattr(run, 'collect_jobs', unavailable)
    deleted = []
    run.client = SimpleNamespace(delete_dataset=lambda *a, **k: deleted.append(a))
    with pytest.raises(TimeoutError):
        run.cleanup()
    assert not deleted


def test_package_includes_entire_ga4_module_and_optional_lock():
    packaged = {p.relative_to(package.ROOT).as_posix() for p in package.files()}
    for path in (package.ROOT / 'ga4_pipeline').rglob('*'):
        if (path.is_file() and path.name != '.user.yml' and
                not any(part in ('target', 'logs', '__pycache__') for part in path.parts)):
            assert path.relative_to(package.ROOT).as_posix() in packaged
    assert 'requirements-bigquery.lock' in packaged


def test_public_projection_rejects_ambiguous_parameter_and_preserves_unknowns():
    events = (native.ROOT / 'bigquery/ga4_event_projection.sql').read_text()
    items = (native.ROOT / 'bigquery/ga4_item_projection.sql').read_text()
    assert "where ep.key='ga_session_id')" in events and 'limit 1' not in events.lower()
    assert 'coalesce(i.price' not in items and 'coalesce(i.quantity' not in items
    assert 'coalesce(ecommerce.purchase_revenue' not in events
    assert '_TABLE_SUFFIX = @shard' in events and '_TABLE_SUFFIX = @shard' in items


@pytest.mark.parametrize('fault,test_name', [
    ('none', 'assert_landing_values'),
    ('missing_price', 'assert_landing_values'),
    ('missing_quantity', 'assert_landing_values'),
    ('negative_position', 'assert_landing_values'),
    ('missing_event_date', 'assert_landing_values'),
    ('empty_event_key', 'assert_landing_values'),
    ('orphan_revision', 'assert_item_version_identity'),
    ('duplicate_position', 'assert_item_version_identity'),
])
def test_actual_landing_sql_rejects_faults_locally(fault, test_name):
    # Execute the real portable singular-test SQL in DuckDB. This is deliberately
    # local SQL evidence, not a substitute for the future native negative probe.
    import duckdb
    from jinja2 import Template
    all_rows = {key: native.landing_rows(1)[key] + native.landing_rows(2)[key]
                for key in ('event_versions', 'item_versions')}
    if fault == 'missing_price':
        all_rows['item_versions'][0]['price_cents'] = None
    elif fault == 'missing_quantity':
        all_rows['item_versions'][0]['quantity'] = None
    elif fault == 'negative_position':
        all_rows['item_versions'][0]['item_index'] = -1
    elif fault == 'missing_event_date':
        all_rows['event_versions'][0]['event_date'] = None
    elif fault == 'empty_event_key':
        all_rows['event_versions'][0]['event_key'] = ''
    elif fault == 'orphan_revision':
        all_rows['item_versions'][0]['source_version'] = 999
    elif fault == 'duplicate_position':
        all_rows['item_versions'].append(deepcopy(all_rows['item_versions'][0]))
    with duckdb.connect() as db:
        for name, schema in (('event_versions', native.EVENT_FIELDS), ('item_versions', native.ITEM_FIELDS)):
            columns = ','.join(f'{k} {"VARCHAR" if t == "STRING" else t}' for k, t in schema)
            db.execute(f'CREATE TABLE stg_{name} ({columns})')
            db.executemany(f'INSERT INTO stg_{name} VALUES ({",".join("?" for _ in schema)})',
                           [[row[k] for k, _ in schema] for row in all_rows[name]])
        sql = Template((native.ROOT / f'ga4_pipeline/tests/{test_name}.sql').read_text()).render(
            ref=lambda name: name, var=lambda *args: 2)
        findings = db.execute(sql).fetchall()
    assert bool(findings) == (fault != 'none')
