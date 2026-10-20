"""Approval-gated native synthetic dbt acceptance. Planning is entirely offline.

Runtime artifacts contain project/job identity and SQL: keep them in ignored .local.
No interactive login, billing activation, public GA4 scans or IAM changes occur here.
"""
import argparse
import datetime as dt
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.bigquery_oracle import compare, fixture_rows
from scripts.verify_ga4_pipeline import csv_records, MODELS, verify_snapshot

MAX_BYTES = 100_000_000
PINNED = {'dbt-bigquery': '1.9.2', 'dbt-core': '1.9.8', 'google-cloud-bigquery': '3.46.2'}
EVENT_FIELDS = [('event_key', 'STRING'), ('event_date', 'DATE'), ('arrival_batch', 'INTEGER'),
                ('source_version', 'INTEGER'), ('event_name', 'STRING'),
                ('user_pseudo_id', 'STRING'), ('session_id', 'STRING'),
                ('transaction_id', 'STRING'), ('revenue_cents', 'INTEGER')]
ITEM_FIELDS = [('event_key', 'STRING'), ('source_version', 'INTEGER'), ('item_index', 'INTEGER'),
               ('item_id', 'STRING'), ('price_cents', 'INTEGER'), ('quantity', 'INTEGER')]


def utcnow():
    return dt.datetime.now(dt.timezone.utc)


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, default=str) + '\n', encoding='utf-8', newline='\n')


def private_path(path):
    path = Path(path).resolve()
    if not path.is_relative_to((ROOT / '.local').resolve()):
        raise ValueError('Use an ignored repository .local path for account-specific artifacts')
    return path


def inputs():
    paths = list((ROOT / 'ga4_pipeline').rglob('*'))
    paths += [ROOT / 'scripts' / n for n in
              ('verify_bigquery.py', 'bigquery_oracle.py', 'verify_ga4_pipeline.py')]
    paths += [ROOT / 'requirements-bigquery.lock']
    return {p.relative_to(ROOT).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(paths) if p.is_file() and p.name != '.user.yml' and
            not any(x in p.parts for x in ('target', 'logs', 'dbt_packages'))}


def plan(project, run_id, now=None):
    if not re.fullmatch(r'[a-z][a-z0-9-]{4,28}[a-z0-9]', project):
        raise ValueError('Pin a valid Google project ID, not a display name')
    if not re.fullmatch(r'[a-z][a-z0-9_]{5,23}', run_id):
        raise ValueError('Run ID must be 6-24 lowercase letters/digits/underscores')
    # Validate the exact existing fixture before any cloud access is possible.
    fixture_rows(1)
    fixture_rows(2)
    now = now or utcnow()
    return dict(version=1, project=project, run_id=run_id, location='US',
                expires_at=(now + dt.timedelta(hours=24)).isoformat(),
                datasets={name: f'fpa_{run_id}_{name}' for name in ('landing', 'incremental', 'full')},
                maximum_bytes_billed=MAX_BYTES, table_expiration_hours=24,
                threads=1, dbt_job_retries=0, query_timeout_seconds=120,
                planning_query_jobs=250, planning_query_bytes=250 * MAX_BYTES,
                planning_allowance_usd=0.25, aggregate_cost_cap=False,
                negative_probe='one duplicate event in disposable full-build fact; uniqueness must fail',
                requires_billing_enabled=True, source_sha256=inputs())


def validate_approval(value, approved_sha, now=None):
    if digest(value) != approved_sha:
        raise ValueError('Approved plan digest does not match')
    expiration = dt.datetime.fromisoformat(value['expires_at'])
    now = now or utcnow()
    if expiration <= now or expiration > now + dt.timedelta(hours=24, minutes=1):
        raise ValueError('Plan expired or invalid lifetime')
    # Rebuild every immutable field; edited limits, targets and source changes fail closed.
    expected = plan(value['project'], value['run_id'], expiration - dt.timedelta(hours=24))
    if value != expected:
        raise ValueError('Plan no longer matches pinned code, resources or limits')


def landing_rows(batch):
    events = []
    revisions = set()
    for row in csv_records('raw_ga4_events.csv'):
        if int(row['arrival_batch']) != batch:
            continue
        row = {k: (int(v) if kind == 'INTEGER' else v or None) for k, kind in EVENT_FIELDS
               for v in [row[k]]}
        events.append(row)
        revisions.add((row['event_key'], row['source_version']))
    items = [{k: (int(row[k]) if kind == 'INTEGER' else row[k]) for k, kind in ITEM_FIELDS}
             for row in csv_records('raw_ga4_items.csv')
             if (row['event_key'], int(row['source_version'])) in revisions]
    return {'event_versions': events, 'item_versions': items}


def assert_owned(dataset, labels, location):
    if (dataset.location.upper() != location or
            any(dataset.labels.get(k) != v for k, v in labels.items())):
        raise RuntimeError('Dataset ownership/location mismatch; deletion prohibited')


def require_billing(info):
    if info.get('billingEnabled') is not True:
        raise RuntimeError('Billing-disabled sandbox cannot execute incremental MERGE acceptance')


class NativeRun:
    def __init__(self, value, output):
        # Called only AFTER plan approval and lifetime/source validation.
        import google.auth
        from google.auth.transport.requests import AuthorizedSession
        from google.cloud import bigquery
        from google.api_core.exceptions import NotFound
        for name, version in PINNED.items():
            if importlib.metadata.version(name) != version:
                raise RuntimeError(f'Install the optional lock: {name} must be {version}')
        self.bq, self.not_found = bigquery, NotFound
        self.plan, self.output = value, output
        self.labels = {'assurance_run': value['run_id'], 'plan_sha': digest(value)[:32]}
        credentials, _ = google.auth.default(scopes=['https://www.googleapis.com/auth/cloud-platform'])
        # Metadata only; never enable APIs or billing. Owner UI alone is insufficient.
        response = AuthorizedSession(credentials).get(
            f'https://cloudbilling.googleapis.com/v1/projects/{value["project"]}/billingInfo', timeout=30)
        response.raise_for_status()
        require_billing(response.json())
        self.client = bigquery.Client(project=value['project'], credentials=credentials, location='US')
        self.started = utcnow()
        self.created, self.jobs, self.job_intents = [], {}, set()
        self.report = dict(scope='native BigQuery synthetic landing/dbt; not production GA4',
                           status='started', plan_sha256=digest(value),
                           project=value['project'], run_id=value['run_id'], phases=[], cleanup={})

    def collect_jobs(self):
        # An uncertain submission must not become permission to delete its inputs.
        for job_id in self.job_intents:
            self.jobs[job_id] = self.client.get_job(job_id, location='US', retry=None, timeout=30)
        # Same principal's jobs only, exact run label and start time. Never query billing tables.
        for job in self.client.list_jobs(min_creation_time=self.started, all_users=False,
                                         retry=None, timeout=30):
            if (job.labels or {}).get('assurance_run') != self.plan['run_id']:
                continue
            self.jobs[job.job_id] = self.client.get_job(job.job_id, location=job.location,
                                                       retry=None, timeout=30)
        records = []
        for job in self.jobs.values():
            sql = getattr(job, 'query', None)
            records.append(dict(job_id=job.job_id, location=job.location, state=job.state,
                job_type=job.job_type, statement_type=getattr(job, 'statement_type', None),
                errors=job.errors, sql=sql,
                sql_sha256=hashlib.sha256(sql.encode()).hexdigest() if sql else None,
                bytes_processed=getattr(job, 'total_bytes_processed', None),
                bytes_billed=getattr(job, 'total_bytes_billed', None),
                maximum_bytes_billed=getattr(job, 'maximum_bytes_billed', None),
                slot_millis=getattr(job, 'slot_millis', None),
                cache_hit=getattr(job, 'cache_hit', None), labels=job.labels))
        write(self.output / 'jobs.json', records)
        return records

    def query(self, sql):
        config = self.bq.QueryJobConfig(maximum_bytes_billed=MAX_BYTES, use_query_cache=False,
                                        job_timeout_ms=120_000,
                                        labels=self.labels)
        job_id = f'{self.plan["run_id"]}_{uuid.uuid4().hex}'
        self.job_intents.add(job_id)
        write(self.output / 'job-intents.json', sorted(self.job_intents))
        job = self.client.query(sql, job_config=config, location='US', job_id=job_id,
                                retry=None, job_retry=None, timeout=30)
        self.jobs[job.job_id] = job
        return [dict(row) for row in job.result(timeout=120, retry=None, job_retry=None)]

    def dataset_name(self, name):
        return f'{self.plan["project"]}.{self.plan["datasets"][name]}'

    def create(self):
        # Inventory ALL names first; a collision can never become permission to delete.
        for name in self.plan['datasets']:
            try:
                self.client.get_dataset(self.dataset_name(name), retry=None, timeout=30)
            except self.not_found:
                continue
            raise RuntimeError('Dataset collision: no resource may be reused or overwritten')
        for name in self.plan['datasets']:
            dataset = self.bq.Dataset(self.dataset_name(name))
            dataset.location, dataset.labels = 'US', self.labels
            dataset.default_table_expiration_ms = 24 * 60 * 60 * 1000
            # Record intent before request; an acknowledgement may be lost.
            self.created.append(name)
            write(self.output / 'dataset-registry.json', self.created)
            self.client.create_dataset(dataset, retry=None, timeout=30)

    def load(self, batch):
        for table, rows in landing_rows(batch).items():
            fields = EVENT_FIELDS if table == 'event_versions' else ITEM_FIELDS
            config = self.bq.LoadJobConfig(schema=[self.bq.SchemaField(k, t) for k, t in fields],
                write_disposition='WRITE_APPEND', labels=self.labels, job_timeout_ms=120_000)
            job_id = f'{self.plan["run_id"]}_batch{batch}_{table}'
            self.job_intents.add(job_id)
            write(self.output / 'job-intents.json', sorted(self.job_intents))
            job = self.client.load_table_from_json(rows, f'{self.dataset_name("landing")}.{table}',
                job_config=config, location='US', num_retries=0, timeout=30,
                job_id=job_id)
            self.jobs[job.job_id] = job
            job.result(timeout=120, retry=None)
        # Exact readback detects duplicate appends, missing fields and ambiguous loads.
        for table in ('event_versions', 'item_versions'):
            actual = self.query(f'SELECT * FROM `{self.dataset_name("landing")}.{table}`')
            expected = sum((landing_rows(b)[table] for b in range(1, batch + 1)), [])
            compare({table: actual}, {table: expected})

    def dbt(self, phase, batch, full=False):
        directory = self.output / phase
        directory.mkdir()
        env = dict(os.environ, DBT_SEND_ANONYMOUS_USAGE_STATS='false',
                   GA4_BQ_PROJECT=self.plan['project'], GA4_BQ_RUN=self.plan['run_id'],
                   GA4_BQ_LANDING=self.plan['datasets']['landing'],
                   GA4_BQ_DATASET=self.plan['datasets']['full' if full else 'incremental'],
                   DBT_TARGET_PATH=str(directory / 'target'), DBT_LOG_PATH=str(directory / 'logs'))
        args = [sys.executable, '-m', 'dbt.cli.main', 'build', '--project-dir', str(ROOT / 'ga4_pipeline'),
                '--profiles-dir', str(ROOT / 'ga4_pipeline'), '--target', 'bigquery',
                '--no-partial-parse', '--exclude', 'resource_type:seed',
                '--vars', json.dumps({'active_batch': batch})]
        if full:
            args.append('--full-refresh')
        try:
            with (directory / 'console.log').open('w', encoding='utf-8') as log:
                process = subprocess.run(args, cwd=ROOT, env=env, stdout=log,
                                         stderr=subprocess.STDOUT, timeout=600)
            if process.returncode:
                raise RuntimeError(f'dbt failed in {phase}; inspect private log')
            result = json.loads((directory / 'target/run_results.json').read_text())
            manifest = json.loads((directory / 'target/manifest.json').read_text())
            expected = {key for key, node in manifest['nodes'].items()
                        if node['resource_type'] in ('model', 'test')}
            observed = {row['unique_id'] for row in result['results']}
            if (observed != expected or len([k for k in observed if k.startswith('model.')]) != 7
                    or any(r['status'] not in ('success', 'pass') for r in result['results'])):
                raise AssertionError('dbt model/test coverage incomplete or unsuccessful')
        finally:
            self.collect_jobs()
        if len(self.jobs) > self.plan['planning_query_jobs']:
            raise RuntimeError('Job count exceeds planning envelope; stop before next phase')

    def snapshot(self, phase, batch, full=False):
        dataset = self.dataset_name('full' if full else 'incremental')
        rows = {name: self.query(f'SELECT * FROM `{dataset}.{name}`') for name in MODELS}
        compare(rows, fixture_rows(batch))
        outcome = verify_snapshot(rows, batch)
        write(self.output / f'{phase}-rowsets.json', rows)
        self.report['phases'].append(dict(phase=phase, complete_python_oracle=True, **outcome))
        return rows

    def negative_probe(self):
        # Deliberately corrupt ONLY the disposable independent full-build table,
        # AFTER parity capture. No source row or incremental result is modified.
        table = f'{self.dataset_name("full")}.fct_ga4_events'
        self.query(f"INSERT INTO `{table}` SELECT * FROM `{table}` WHERE event_key='ev03'")
        directory = self.output / 'negative-duplicate'
        directory.mkdir()
        env = dict(os.environ, DBT_SEND_ANONYMOUS_USAGE_STATS='false',
                   GA4_BQ_PROJECT=self.plan['project'], GA4_BQ_RUN=self.plan['run_id'],
                   GA4_BQ_LANDING=self.plan['datasets']['landing'],
                   GA4_BQ_DATASET=self.plan['datasets']['full'],
                   DBT_TARGET_PATH=str(directory / 'target'), DBT_LOG_PATH=str(directory / 'logs'))
        with (directory / 'console.log').open('w', encoding='utf-8') as log:
            proc = subprocess.run([sys.executable, '-m', 'dbt.cli.main', 'test',
                '--project-dir', str(ROOT / 'ga4_pipeline'), '--profiles-dir', str(ROOT / 'ga4_pipeline'),
                '--target', 'bigquery', '--no-partial-parse', '--vars', '{"active_batch":2}',
                '--select', 'unique_fct_ga4_events_event_key'], cwd=ROOT, env=env,
                stdout=log, stderr=subprocess.STDOUT, timeout=180)
        result = json.loads((directory / 'target/run_results.json').read_text())['results']
        if (proc.returncode != 1 or len(result) != 1 or result[0]['status'] != 'fail'
                or result[0]['failures'] != 1
                or not result[0]['unique_id'].startswith('test.ga4_governed_warehouse.unique_fct_ga4_events_event_key.')):
            raise AssertionError('Expected native duplicate uniqueness failure not established')
        self.report['negative_duplicate_probe'] = 'expected_dbt_failure_detected'

    def cleanup(self):
        # Stop/cancel THIS run's jobs, and verify terminal status before dataset removal.
        records = self.collect_jobs()
        for job in list(self.jobs.values()):
            if job.state != 'DONE':
                self.client.cancel_job(job.job_id, location=job.location, retry=None, timeout=30)
        records = self.collect_jobs()
        if any(j['state'] != 'DONE' for j in records):
            raise RuntimeError('Run jobs still active/uncertain; cleanup requires operator follow-up')
        for name in reversed(self.created):
            identifier = self.dataset_name(name)
            try:
                dataset = self.client.get_dataset(identifier, retry=None, timeout=30)
            except self.not_found:
                self.report['cleanup'][name] = 'absent'
                continue
            assert_owned(dataset, self.labels, 'US')
            self.client.delete_dataset(identifier, delete_contents=True, retry=None, timeout=30)
            try:
                self.client.get_dataset(identifier, retry=None, timeout=30)
            except self.not_found:
                self.report['cleanup'][name] = 'confirmed_not_found'
            else:
                raise RuntimeError('Dataset deletion not independently confirmed')

    def run(self):
        try:
            self.create()
            self.load(1)
            self.dbt('initial', 1)
            self.snapshot('initial', 1)
            self.load(2)
            self.dbt('incremental', 2)
            incremental = self.snapshot('incremental', 2)
            self.dbt('replay', 2)
            compare(self.snapshot('replay', 2), incremental)
            self.dbt('independent-full', 2, full=True)
            compare(self.snapshot('independent-full', 2, full=True), incremental)
            self.negative_probe()
            records = self.collect_jobs()
            if not any(j['sql'] and re.search(r'\bmerge\s+into\b', j['sql'], re.I)
                       and j['state'] == 'DONE' and not j['errors'] for j in records):
                raise AssertionError('No successful native MERGE job captured')
            if any(j['job_type'] == 'query' and j['maximum_bytes_billed'] != MAX_BYTES
                   for j in records):
                raise AssertionError('Query job limit evidence differs from approved limit')
            self.report.update(status='passed', incremental_equals_full_refresh=True,
                               replay_unchanged=True, native_merge_captured=True)
        except BaseException as error:
            self.report.update(status='failed', error_type=type(error).__name__)
            raise
        finally:
            try:
                self.cleanup()
            except BaseException as error:
                self.report.update(status='cleanup_incomplete', cleanup_error_type=type(error).__name__)
                raise
            finally:
                self.report['completed_at'] = utcnow().isoformat()
                write(self.output / 'acceptance.json', self.report)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    prepare = commands.add_parser('plan', help='Offline; creates no Google client or jobs')
    prepare.add_argument('--project', required=True)
    prepare.add_argument('--run-id', required=True)
    prepare.add_argument('--output', type=private_path, required=True)
    execute = commands.add_parser('execute', help='Only after explicit owner cloud approval')
    execute.add_argument('--plan', type=private_path, required=True)
    execute.add_argument('--approved-sha256', required=True)
    execute.add_argument('--output', type=private_path, required=True)
    args = parser.parse_args()
    if args.command == 'plan':
        value = plan(args.project, args.run_id)
        write(args.output, value)
        print(json.dumps({'scope': 'offline plan only', 'sha256': digest(value)}))
        return
    value = json.loads(args.plan.read_text())
    validate_approval(value, args.approved_sha256)
    # Never restart an ambiguous execution. A failed run requires inventory/review,
    # not a new blind append or automatic rerun of a chargeable workload.
    args.output.mkdir(parents=True, exist_ok=False)
    write(args.output / 'approved-plan.json', value)
    NativeRun(value, args.output).run()


if __name__ == '__main__':
    main()
