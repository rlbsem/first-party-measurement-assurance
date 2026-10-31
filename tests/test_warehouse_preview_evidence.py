"""Adversarial checks of genuine recorded observations, never stub vendor execution."""
import json
from copy import deepcopy

import pytest

from scripts.check_warehouse_preview_evidence import RECORD, ROOT, check


def test_genuine_native_record_matches_source_and_independent_queries():
    report = check()
    assert report['data_jobs'] == 2 and report['native_dbt_build'] is False
    assert report['bytes_processed'] == 19631768
    assert report['row_counts'] == {'daily': 1, 'events': 1856, 'items': 4828, 'purchases': 14, 'sessions': 12}


@pytest.mark.parametrize('fault', ['count', 'hash', 'missing', 'duplicate'])
def test_changed_result_cannot_pass(fault):
    directory = ROOT / RECORD
    rows = [json.loads((directory.parent / name).read_text()) for name in ('model-results.json', 'source-results.json')]
    if fault == 'count':
        rows[0][0]['row_count'] = '2'
    elif fault == 'hash':
        rows[0][0]['multiset_sha256'] = '0' * 64
    elif fault == 'missing':
        rows[0].pop()
    else:
        rows[0].append(deepcopy(rows[0][0]))
    with pytest.raises(ValueError):
        check(result_rows=rows)


@pytest.mark.parametrize('field,value', [('state', 'RUNNING'), ('cache_hit', 'true'),
                                       ('completed_without_error', 'false'), ('statement_type', 'MERGE'),
                                       ('total_bytes_processed', '100000001'), ('sql_sha256', '0'*64)])
def test_invalid_provider_metadata_cannot_pass(field, value):
    metadata = json.loads((ROOT / RECORD).with_name('job-statistics-results.json').read_text())
    metadata[0][field] = value
    with pytest.raises(ValueError):
        check(provider_rows=metadata)


@pytest.mark.parametrize('boundary', ['native_dbt_compile', 'native_dbt_build', 'native_incremental_merge', 'production_ga4_export'])
def test_native_select_cannot_be_promoted_to_unexecuted_acceptance(boundary):
    value = json.loads((ROOT / RECORD).read_text())
    value['evidence_boundaries'][boundary] = True
    with pytest.raises(ValueError, match='promoted'):
        check(value)
