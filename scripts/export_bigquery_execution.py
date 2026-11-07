"""Export a small, sanitized native record locally; never overwrite an existing record."""
import argparse
import hashlib
import json
from pathlib import Path

PHASES = ('initial', 'incremental', 'replay', 'independent-full')


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def export(source, archive, output):
    source, archive, output = Path(source), Path(archive), Path(output)
    if output.exists():
        raise FileExistsError('Recorded evidence is immutable; choose a new output directory')
    report = json.loads((source / 'execution/acceptance.json').read_text())
    plan = json.loads((source / 'plan.json').read_text())
    if report['status'] != 'passed':
        raise ValueError('Preserve failed execution separately; do not export a passing record')
    pinned = json.loads((source / 'private-artifact-sha256.json').read_text())
    for name, expected in pinned.items():
        path = (source / name).resolve()
        if not path.is_relative_to(source.resolve()) or sha(path) != expected:
            raise ValueError('Private original artifact changed: ' + name)
    if (hashlib.sha256(json.dumps(plan, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
            != report['plan_sha256'] or load_preflight(source)['plan_unchanged'] is not True):
        raise ValueError('Executed plan binding differs')
    project = plan['project']
    def load(name):
        return json.loads((source / name).read_text())
    def sanitize(value):
        if isinstance(value, str):
            return value.replace(project, '<execution-project>')
        if isinstance(value, dict):
            return {k: sanitize(v) for k, v in value.items()}
        if isinstance(value, list):
            return [sanitize(v) for v in value]
        return value
    builds, original_hashes = {}, {}
    for phase in (*PHASES, 'negative-duplicate'):
        name = f'execution/{phase}/target/run_results.json'
        result = load(name)
        builds[phase] = dict(metadata={k: result['metadata'][k] for k in ('dbt_version', 'dbt_schema_version', 'generated_at')},
                            elapsed_time=result['elapsed_time'], results=[
                                {k: r[k] for k in ('unique_id', 'status', 'failures', 'execution_time', 'adapter_response')}
                                for r in result['results']])
        original_hashes[name] = sha(source / name)
        manifest_name = f'execution/{phase}/target/manifest.json'
        original_hashes[manifest_name] = sha(source / manifest_name)
    manifest = load('execution/initial/target/manifest.json')
    jobs = load('execution/jobs.json')
    for job in jobs:
        if job['sql']:
            if hashlib.sha256(job['sql'].encode()).hexdigest() != job['sql_sha256']:
                raise ValueError('Original executed SQL hash differs')
            job['sanitized_sql_sha256'] = hashlib.sha256(sanitize(job['sql']).encode()).hexdigest()
    values = {
        'acceptance': report, 'dbt-results': builds,
        'rowsets': {p: load(f'execution/{p}-rowsets.json') for p in PHASES},
        'jobs': jobs, 'independent-audit': load('independent-audit.json'),
        'provenance': dict(source_revision=load('preflight.json')['source_revision'],
                          plan_sha256=report['plan_sha256'], source_sha256=plan['source_sha256'],
                          dbt_nodes=sorted(k for k, n in manifest['nodes'].items()
                                           if n['resource_type'] in ('model', 'test')),
                          native_versions={k: manifest['metadata'][k] for k in ('dbt_version', 'adapter_type')},
                          private_archive_sha256=sha(archive),
                          private_artifact_count=len(load('private-artifact-sha256.json')),
                          original_dbt_artifact_sha256=original_hashes,
                          redaction='Project identity replaced consistently; actual job IDs retained. Original and sanitized SQL hashes are distinct. Private logs, account identities and credentials are not copied.',
                          source_documentation_note='Execution inputs remain exact; the module README may gain a current-status link. Its original hash refers to the pinned executed revision.',
                          boundaries=dict(native_dbt_build=True, native_incremental_merge=True,
                                          synthetic_landing=True, production_ga4_export=False,
                                          public_sample_used_in_this_run=False, billing_clearance=False))}
    output.mkdir(parents=True, exist_ok=False)
    for name, value in values.items():
        text = json.dumps(sanitize(value), indent=2) + '\n'
        if project in text or '@' in text:
            raise ValueError('Private project identity or email marker remains')
        (output / (name + '.json')).write_text(text, encoding='utf-8', newline='\n')
    hashes = {p.name: sha(p) for p in sorted(output.glob('*.json'))}
    (output / 'artifact-sha256.json').write_text(json.dumps(hashes, indent=2) + '\n', encoding='utf-8', newline='\n')
    return hashes


def load_preflight(source):
    return json.loads((source / 'approved-preflight.json').read_text())


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', type=Path, required=True)
    parser.add_argument('--archive', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(export(args.input, args.archive, args.output)))
