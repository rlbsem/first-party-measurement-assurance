"""Offline checks of recorded GoogleSQL observations; never authenticates or submits jobs."""
import hashlib
import json
import re
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.warehouse_preview import MODELS, queries

RECORD = 'docs/evidence/warehouse-review/native-readonly/acceptance.json'


def require(condition, message):
    if not condition:
        raise ValueError(message)


def check(value=None, root=ROOT, result_rows=None, provider_rows=None):
    directory = root / Path(RECORD).parent
    value = value if value is not None else json.loads((root / RECORD).read_text())
    require(value['status'] == 'executed_read_only_sql', 'Not executed read-only evidence')
    boundaries = value['evidence_boundaries']
    require(boundaries['native_google_sql'] is True and boundaries['jinja_rendered_existing_model_select_bodies'] is True,
            'Native SELECT boundary missing')
    require(all(boundaries[k] is False for k in ('native_dbt_compile', 'native_dbt_build',
                                                'native_incremental_merge', 'production_ga4_export')),
            'Read-only SQL must not be promoted to native dbt/production acceptance')
    require(value['observed_project_mode'] == 'billing-disabled sandbox banner', 'Sandbox observation missing')
    require(value['settings']['maximum_bytes_billed_per_job'] is None
            and value['settings']['job_timeout_ms'] is None, 'Do not invent configured job limits')
    require(value['rowset_comparison']['all_five_match'] is True, 'Comparison not established')
    expected_sources = {f'ga4_pipeline/models/{"staging/" if n.startswith("stg_") else ""}{n}.sql' for n in MODELS}
    expected_sources |= {'ga4_pipeline/macros/source_relations.sql', 'scripts/warehouse_preview.py'}
    require(set(value['source_sha256']) == expected_sources, 'Incomplete source provenance')
    for name, sha in value['source_sha256'].items():
        require(hashlib.sha256((root / name).read_bytes()).hexdigest() == sha, 'Rendered model source changed: ' + name)
    provider_rows = provider_rows if provider_rows is not None else json.loads((directory / 'job-statistics-results.json').read_text())
    require(len(provider_rows) == 2, 'Provider metadata must contain exactly two jobs')
    provider = {row['job_id']: row for row in provider_rows}
    require(len(provider) == 2, 'Duplicate job identity')
    require(set(value['queries']) == set(queries()), 'Query inventory differs')
    generated = queries()
    processed = billed = slots = 0
    for name, record in value['queries'].items():
        path = 'bigquery/' + name + '.sql'
        require(record['source_path'] == path, 'Unexpected SQL source')
        sql = (root / path).read_bytes()
        require(sql.decode() == generated[name], 'Recorded SELECT bodies differ from actual model rendering')
        require(hashlib.sha256(sql).hexdigest() == record['source_sha256'], 'SQL file hash changed')
        require(record['executed_line_endings'] == 'CRLF', 'Editor encoding differs')
        job = provider[record['job_id']]
        require(hashlib.sha256(sql.replace(b'\n', b'\r\n')).hexdigest() == job['sql_sha256'],
                'Provider SQL hash does not match exact tracked query')
        require(job['state'] == 'DONE' and job['completed_without_error'] == 'true', 'Incomplete/failed native job')
        require(job['job_type'] == 'QUERY' and job['statement_type'] == 'SELECT', 'Not read-only SELECT')
        require(job['cache_hit'] == 'false', 'Cached result cannot establish fresh execution')
        start, end = [datetime.fromisoformat(job[k].replace(' UTC', '+00:00')) for k in ('start_time', 'end_time')]
        require(start <= end, 'Invalid job timing')
        require(0 < int(job['total_bytes_processed']) <= value['settings']['estimated_scan_gate_bytes'],
                'Actual scan exceeds reviewed size')
        require(0 < int(job['total_bytes_billed']) <= 100000000 and int(job['total_slot_ms']) >= 0,
                'Invalid resource counters')
        processed += int(job['total_bytes_processed'])
        billed += int(job['total_bytes_billed'])
        slots += int(job['total_slot_ms'])
    result_rows = result_rows if result_rows is not None else [json.loads((directory / name).read_text())
                                                             for name in ('model-results.json', 'source-results.json')]
    require(len(result_rows) == 2, 'Two independent rowset observations required')
    normalized = []
    for rows in result_rows:
        require(len(rows) == 5 and {r['model'] for r in rows} == {'events', 'items', 'purchases', 'sessions', 'daily'},
                'Incomplete/duplicate model comparison')
        require(all(int(r['row_count']) > 0 and re.fullmatch('[a-f0-9]{64}', r['multiset_sha256']) for r in rows),
                'Malformed results')
        normalized.append(sorted((r['model'], int(r['row_count']), r['multiset_sha256']) for r in rows))
    require(normalized[0] == normalized[1], 'Native multiset comparison differs')
    meta = value['metadata_query']
    require(meta['source_path'] == 'bigquery/warehouse_job_metadata.sql', 'Unexpected metadata query source')
    require(hashlib.sha256((root / meta['source_path']).read_bytes()).hexdigest() == meta['source_sha256'],
            'Metadata source hash changed')
    require(set(provider) == {r['job_id'] for r in value['queries'].values()}, 'Job/source inventory differs')
    return {'status': 'verified_recorded_google_sql', 'row_counts': {r[0]: r[1] for r in normalized[0]},
            'data_jobs': 2, 'bytes_processed': processed, 'job_reported_bytes_billed': billed,
            'slot_millis': slots, 'native_dbt_build': False, 'native_incremental_merge': False,
            'boundary': 'Recorded provider observations and local integrity checks; no new Google requests or billing assertion.'}


if __name__ == '__main__':
    print(json.dumps(check()))
