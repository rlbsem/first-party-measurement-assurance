"""Check recorded native acceptance offline; never authenticate or submit jobs."""
import hashlib
import json
import re
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.bigquery_oracle import compare, fixture_rows
from scripts.verify_ga4_pipeline import verify_snapshot
from scripts.verify_bigquery import inputs

RECORD = 'docs/evidence/native-dbt-bigquery/2026-11-07'
PHASES = ('initial', 'incremental', 'replay', 'independent-full')
NAMES = ('acceptance', 'dbt-results', 'rowsets', 'jobs', 'independent-audit', 'provenance')


def require(condition, message):
    if not condition:
        raise ValueError(message)


def check(values=None, root=ROOT):
    directory = root / RECORD
    if values is None:
        hashes = json.loads((directory / 'artifact-sha256.json').read_text())
        require(set(hashes) == {n + '.json' for n in NAMES}, 'Evidence file inventory differs')
        for name, sha in hashes.items():
            require(hashlib.sha256((directory / name).read_bytes()).hexdigest() == sha,
                    'Recorded artifact changed: ' + name)
        values = {n: json.loads((directory / (n + '.json')).read_text()) for n in NAMES}
    report, builds, rows, jobs, audit, provenance = (values[n] for n in NAMES)
    require(report['status'] == 'passed', 'Native acceptance did not pass')
    require(all(report[k] is True for k in ('incremental_equals_full_refresh',
                                           'replay_unchanged', 'native_merge_captured')),
            'Native acceptance assertions incomplete')
    require(report['negative_duplicate_probe'] == 'expected_dbt_failure_detected',
            'Negative probe missing')
    require(provenance['source_revision'] == 'a1438e3eef101e23d61f0fd7889272098e50f9f6',
            'Unexpected executed source revision')
    require(provenance['plan_sha256'] == report['plan_sha256'] == audit['plan_sha256'],
            'Plan binding differs')
    require(provenance['boundaries'] == {
        'native_dbt_build': True, 'native_incremental_merge': True,
        'synthetic_landing': True, 'production_ga4_export': False,
        'public_sample_used_in_this_run': False, 'billing_clearance': False},
        'Execution boundary changed')
    require(len(provenance['source_sha256']) == 27
            and set(provenance['source_sha256']) == set(inputs()), 'Source inventory incomplete')
    for name, sha in provenance['source_sha256'].items():
        path = (root / name).resolve()
        require(path.is_relative_to(root), 'Unsafe source path')
        # Execution inputs stay exact. The module README may gain a current-status
        # link; its original hash remains pinned to the executed Git revision.
        if path.suffix != '.md':
            require(hashlib.sha256(path.read_bytes()).hexdigest() == sha,
                    'Executed input changed: ' + name)
    require(set(rows) == set(PHASES), 'Rowset phase coverage differs')
    require([p['phase'] for p in report['phases']] == list(PHASES), 'Acceptance phase coverage differs')
    for phase in PHASES:
        batch = 1 if phase == 'initial' else 2
        compare(rows[phase], fixture_rows(batch))
        expected = dict(phase=phase, complete_python_oracle=True, **verify_snapshot(rows[phase], batch))
        require(expected == report['phases'][PHASES.index(phase)], 'Phase summary differs from rows')
    compare(rows['incremental'], rows['replay'])
    compare(rows['incremental'], rows['independent-full'])
    require(set(builds) == set(PHASES) | {'negative-duplicate'}, 'dbt phase coverage differs')
    expected_nodes = set(provenance['dbt_nodes'])
    model_names = {'model.ga4_governed_warehouse.' + p.stem
                   for p in (root / 'ga4_pipeline/models').rglob('*.sql')}
    require({n for n in expected_nodes if n.startswith('model.')} == model_names
            and len(model_names) == 7
            and len([n for n in expected_nodes if n.startswith('test.')]) == 30,
            'Expected seven models and thirty tests')
    for phase in PHASES:
        result = builds[phase]['results']
        require(len(result) == len(expected_nodes) == 37
                and {r['unique_id'] for r in result} == expected_nodes
                and all(r['status'] in ('success', 'pass') for r in result),
                'Incomplete or failed dbt coverage: ' + phase)
    negative = builds['negative-duplicate']['results']
    require(len(negative) == 1 and negative[0]['status'] == 'fail'
            and negative[0]['failures'] == 1
            and negative[0]['unique_id'].startswith('test.ga4_governed_warehouse.unique_fct_ga4_events_event_key.'),
            'Expected exact uniqueness failure absent')
    provider = {j['job_id']: j for j in audit['jobs']}
    require(len(provider) == len(audit['jobs']) == len(jobs)
            and len({j['job_id'] for j in jobs}) == len(jobs), 'Duplicate or missing native jobs')
    for job in jobs:
        observed = provider.get(job['job_id'])
        require(observed is not None, 'Job missing independent metadata')
        require(job['state'] == observed['state'] == 'DONE'
                and not job['errors'] and not observed['errors'], 'Native job incomplete or failed')
        for key in ('job_type', 'location', 'statement_type', 'bytes_processed',
                    'bytes_billed', 'slot_millis', 'maximum_bytes_billed', 'labels'):
            require(job[key] == observed[key], 'Independent job metadata differs: ' + key)
        require(job['location'] == 'US' and job['labels']['assurance_run'] == report['run_id'],
                'Job scope differs')
        require(datetime.fromisoformat(observed['created']) <= datetime.fromisoformat(observed['started'])
                <= datetime.fromisoformat(observed['ended']) <= datetime.fromisoformat(audit['checked_at']),
                'Invalid job/audit timing')
        if job['job_type'] == 'query':
            require(job['maximum_bytes_billed'] == 100_000_000, 'Missing query cap')
            require(hashlib.sha256(job['sql'].encode()).hexdigest() == job['sanitized_sql_sha256'],
                    'Sanitized SQL changed')
            require(re.fullmatch('[a-f0-9]{64}', job['sql_sha256']) is not None,
                    'Original executed SQL hash missing')
    merges = [j['job_id'] for j in jobs if j['statement_type'] == 'MERGE'
              and j['sql'] and re.search(r'\bmerge\s+into\b', j['sql'], re.I)]
    require(len(merges) >= 2, 'Incremental and replay MERGE evidence absent')
    for phase, affected in (('incremental', 6), ('replay', 0)):
        result = next(r for r in builds[phase]['results']
                      if r['unique_id'] == 'model.ga4_governed_warehouse.fct_ga4_events')
        response = result['adapter_response']
        require(response['job_id'] in merges and response['code'] == 'MERGE'
                and response['rows_affected'] == affected, 'MERGE outcome not established: ' + phase)
    job_ids = set(provider)
    require(all(r['adapter_response']['job_id'] in job_ids
                for build in builds.values() for r in build['results']),
            'dbt result not linked to a recorded native job')
    require(all(report['cleanup'][n] == audit['dataset_absence'][n] == 'confirmed_not_found'
                for n in ('landing', 'incremental', 'full'))
            and audit['remaining_run_datasets'] == [] and audit['all_recorded_jobs_terminal'] is True,
            'Independent cleanup not established')
    top = [j for j in audit['jobs'] if j['job_type'] == 'query' and j['parent_job_id'] is None]
    measured = audit['resource_observations']
    require(measured['top_level_query_jobs'] == len(top)
            and measured['load_jobs'] == sum(j['job_type'] == 'load' for j in jobs)
            and measured['query_bytes_processed'] == sum(j['bytes_processed'] or 0 for j in top)
            and measured['query_bytes_billed'] == sum(j['bytes_billed'] or 0 for j in top)
            and measured['query_slot_millis'] == sum(j['slot_millis'] or 0 for j in top)
            and measured['billing_clearance'] is False, 'Resource observations or billing boundary differ')
    return dict(status='verified_recorded_native_dbt', models_per_build=7, tests_per_build=30,
                successful_builds=4, expected_negative_failures=1, merge_jobs=merges,
                final_row_counts={k: len(v) for k, v in rows['incremental'].items()},
                **measured, boundary='Offline revalidation of recorded native execution; no new cloud requests.')


if __name__ == '__main__':
    print(json.dumps(check()))
