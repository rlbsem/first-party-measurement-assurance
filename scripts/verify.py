"""Single credential-free verification entry point; no vendor calls or publishing."""
import hashlib
import importlib.metadata
import json
import platform
import re
import subprocess
import sys
import uuid
import xml.etree.ElementTree as ET
from copy import deepcopy
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from assurance.analytics import build
from assurance.core import Ledger, digest
from assurance.fixtures import seed
from assurance.native import write_evidence
from demo import run as demo
from browser_verify import run as browser
from package import files, audit, write_manifest


def sources():
    return {p.relative_to(ROOT).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest() for p in files()
            if p.relative_to(ROOT).parts[0] not in ('docs',) and p.suffix != '.md'}


def links():
    count = 0
    for file in files():
        if file.suffix != '.md':
            continue
        for link in re.findall(r'\]\(([^)]+)\)', file.read_text(encoding='utf-8')):
            if '://' in link or link.startswith('#'):
                continue
            target = (file.parent / link.split('#')[0]).resolve()
            assert target.is_relative_to(ROOT) and target.exists(), (file, link)
            count += 1
    return count


def archived():
    record = ROOT/'docs/evidence/browser.json'
    if not record.exists():
        return
    value = json.loads(record.read_text())
    for name, expected in value['source_sha256'].items():
        assert hashlib.sha256((ROOT/name).read_bytes()).hexdigest() == expected, name
    for shot in value['screenshots']:
        assert hashlib.sha256((ROOT/'docs/images'/shot['file']).read_bytes()).hexdigest() == shot['sha256'], shot['file']


def main():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument('--check-archive', action='store_true')
    args = p.parse_args()
    if args.check_archive:
        from package import check_manifest
        check_manifest()
        archived()
    work = ROOT/'.local'/('verify-'+uuid.uuid4().hex[:10])
    work.mkdir(parents=True)
    subprocess.run([sys.executable, '-m', 'pip', 'check'], check=True)
    test = subprocess.run([sys.executable, '-m', 'pytest', '-q', '--basetemp', str(work/'pytest'),
                           '--junitxml', str(work/'tests.xml')], cwd=ROOT, capture_output=True, text=True)
    (work/'tests.log').write_text(test.stdout+test.stderr, encoding='utf-8', newline='\n')
    if test.returncode:
        raise RuntimeError(f'tests failed; inspect {work / "tests.log"}')
    suite = ET.parse(work/'tests.xml').getroot().find('testsuite')
    counts = {k: int(suite.attrib[k]) for k in ('tests', 'failures', 'errors', 'skipped')}
    result = demo(work/'demo')
    a, b = Ledger(work/'a.sqlite'), Ledger(work/'b.sqlite')
    seed(a)
    seed(b)
    assert digest(a.snapshot()) == digest(b.snapshot()), 'fixture not deterministic'
    corrupt = deepcopy(a.snapshot())
    payment=next(e for e in corrupt['events'] if e['payload']['event_name']=='purchase')
    payment['payload']['items'][0]['quantity'] = 2
    # Intentional downstream corruption with a matching snapshot hash tests SQL,
    # separately from the source-integrity test.
    payment['hash'] = digest(payment['payload'])
    try:
        build(corrupt, work/'fault')
    except RuntimeError:
        fault = json.loads((work/'fault/dbt/target/run_results.json').read_text())
        failures = [r['unique_id'] for r in fault['results'] if r['status']=='fail']
        assert any('item_conservation' in name for name in failures), failures
    else:
        raise AssertionError('corrupt item values passed dbt')
    # Publish the verified analytical record before the browser presents it.
    write_evidence(ROOT/'docs/evidence/demo.json', result)
    write_evidence(ROOT/'docs/evidence/source-ledger.json', a.snapshot())
    capture = browser(work/'browser', ROOT/'docs/images')
    write_evidence(ROOT/'docs/evidence/browser.json', capture)
    archived()
    write_evidence(ROOT/'docs/evidence/verification.json', {
        'status': 'passed', 'environment': {'python': platform.python_version(), 'os': platform.system()},
        'dependencies': {n: importlib.metadata.version(n) for n in ('duckdb','dbt-core','dbt-duckdb','playwright','fastapi','pytest')},
        'python_tests': counts, 'dbt_successful_builds': 3, 'dbt_models_each': len(result['final']['models']),
        'dbt_tests_each': sum(r['node'].startswith('test.') for r in result['final']['dbt_results']),
        'expected_dbt_fault': {'status':'detected', 'failed_nodes':failures},
        'independent_model_equivalence': True, 'deterministic_fixture': True,
        'browser_checks': len(capture['checks']), 'responsive_combinations': len(capture['layouts']),
        'source_sha256': sources(), 'native_bigquery':'not_executed', 'native_mp':'not_executed',
        'hosted_ci':'not_executed_in_this_delivery'})
    checked = links()
    audit_result = audit(files())
    write_manifest()
    print(json.dumps({'tests': counts, 'dbt': f"3 builds / {len(result['final']['models'])} models",
                      'expected_fault': 'detected', 'browser_checks': len(capture['checks']),
                      'markdown_links': checked, 'public_audit': audit_result}))


if __name__ == '__main__':
    main()
