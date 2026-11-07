"""Adversarial offline revalidation of genuine recorded native results."""
import json
from copy import deepcopy

import pytest

from scripts import check_bigquery_execution as evidence


def recorded():
    directory = evidence.ROOT / evidence.RECORD
    return {n: json.loads((directory / (n + '.json')).read_text()) for n in evidence.NAMES}


def test_recorded_native_acceptance_and_independent_cleanup():
    report = evidence.check()
    assert report['successful_builds'] == 4
    assert report['models_per_build'] == 7 and report['tests_per_build'] == 30
    assert report['expected_negative_failures'] == 1
    assert report['billing_clearance'] is False


@pytest.mark.parametrize('fault', [
    'failed_acceptance', 'source_revision', 'source_hash', 'production_claim',
    'missing_phase', 'wrong_corrected_value', 'duplicate_fact', 'missing_null_field',
    'missing_dbt_test', 'failed_dbt_model', 'incorrect_negative_probe',
    'failed_job', 'missing_independent_job', 'duplicate_job', 'missing_query_cap',
    'changed_sql', 'cleanup_ack_only', 'independent_cleanup_missing',
    'changed_resource_total', 'billing_clearance_claim',
    'replay_changes_rows', 'non_merge_statement', 'unbound_dbt_job',
])
def test_recorded_evidence_rejects_faults(fault):
    value = deepcopy(recorded())
    if fault == 'failed_acceptance':
        value['acceptance']['status'] = 'failed'
    elif fault == 'source_revision':
        value['provenance']['source_revision'] = '0' * 40
    elif fault == 'source_hash':
        value['provenance']['source_sha256']['scripts/verify_bigquery.py'] = '0' * 64
    elif fault == 'production_claim':
        value['provenance']['boundaries']['production_ga4_export'] = True
    elif fault == 'missing_phase':
        del value['rowsets']['replay']
    elif fault == 'wrong_corrected_value':
        next(r for r in value['rowsets']['incremental']['fct_ga4_events']
             if r['event_key'] == 'ev03')['revenue_cents'] += 1
    elif fault == 'duplicate_fact':
        rows = value['rowsets']['incremental']['fct_ga4_events']
        rows.append(deepcopy(rows[0]))
    elif fault == 'missing_null_field':
        row = next(r for r in value['rowsets']['incremental']['fct_ga4_events']
                   if r['user_pseudo_id'] is None)
        del row['user_pseudo_id']
    elif fault == 'missing_dbt_test':
        value['dbt-results']['replay']['results'].pop()
    elif fault == 'failed_dbt_model':
        next(r for r in value['dbt-results']['initial']['results']
             if r['unique_id'].startswith('model.'))['status'] = 'error'
    elif fault == 'incorrect_negative_probe':
        value['dbt-results']['negative-duplicate']['results'][0]['failures'] = 0
    elif fault == 'failed_job':
        value['jobs'][0]['state'] = 'RUNNING'
    elif fault == 'missing_independent_job':
        value['independent-audit']['jobs'].pop()
    elif fault == 'duplicate_job':
        value['jobs'].append(deepcopy(value['jobs'][0]))
    elif fault == 'missing_query_cap':
        next(j for j in value['jobs'] if j['job_type'] == 'query')['maximum_bytes_billed'] = None
    elif fault == 'changed_sql':
        next(j for j in value['jobs'] if j['job_type'] == 'query')['sql'] += '\n-- changed'
    elif fault == 'cleanup_ack_only':
        value['acceptance']['cleanup']['landing'] = 'deletion_requested'
    elif fault == 'independent_cleanup_missing':
        value['independent-audit']['remaining_run_datasets'] = ['landing']
    elif fault == 'changed_resource_total':
        value['independent-audit']['resource_observations']['query_bytes_billed'] += 1
    elif fault == 'billing_clearance_claim':
        value['independent-audit']['resource_observations']['billing_clearance'] = True
    elif fault == 'replay_changes_rows':
        next(r for r in value['dbt-results']['replay']['results']
             if r['unique_id'].endswith('.fct_ga4_events'))['adapter_response']['rows_affected'] = 1
    elif fault == 'non_merge_statement':
        for row in value['jobs'] + value['independent-audit']['jobs']:
            if row['statement_type'] == 'MERGE':
                row['statement_type'] = 'SELECT'
    elif fault == 'unbound_dbt_job':
        value['dbt-results']['initial']['results'][0]['adapter_response']['job_id'] = 'not-recorded'
    with pytest.raises((ValueError, AssertionError)):
        evidence.check(value)


def test_export_refuses_to_overwrite_even_without_source_access(tmp_path):
    from scripts.export_bigquery_execution import export
    with pytest.raises(FileExistsError):
        export(tmp_path / 'missing-source', tmp_path / 'missing-archive', tmp_path)
