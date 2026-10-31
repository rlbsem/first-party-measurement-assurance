"""Add complete Python rowset checks to the unchanged local incremental/full-build runner."""
import argparse
import hashlib
import json
import subprocess
import sys
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.bigquery_oracle import compare, fixture_rows
from scripts.verify_ga4_pipeline import sql_rows


def fingerprints():
    paths = list((ROOT / 'ga4_pipeline').rglob('*.sql')) + list((ROOT / 'ga4_pipeline/seeds').glob('*.csv'))
    paths += [ROOT / 'scripts' / name for name in ('verify_ga4_pipeline.py', 'bigquery_oracle.py', 'verify_ga4_current.py')]
    return {p.relative_to(ROOT).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(paths)
            if not any(part in ('target', 'logs', 'dbt_packages') for part in p.parts)}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    if not output.is_relative_to(ROOT / '.local') or output.exists():
        parser.error('Use a fresh private .local output file')
    before = fingerprints()
    work = ROOT / '.local' / ('ga4-complete-' + uuid.uuid4().hex[:10])
    work.mkdir(parents=True)
    subprocess.run([sys.executable, 'scripts/verify_ga4_pipeline.py', '--output', str(work / 'pipeline.json')],
                   cwd=ROOT, check=True)
    record = json.loads((work / 'pipeline.json').read_text())
    assert record['status'] == 'passed'
    database_root = ROOT / '.local' / ('ga4-verification-' + record['run_id'])
    for name in ('incremental', 'full'):
        compare(sql_rows(database_root / (name + '.duckdb')), fixture_rows(2))
    assert before == fingerprints(), 'Source changed during execution'
    record['complete_python_oracle'] = {'models': 5, 'all_fields_and_multiplicities': True,
                                        'incremental_matches': True, 'independent_full_matches': True}
    record['source_sha256'] = before
    record['source_revision'] = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(record, indent=2)+'\n', encoding='utf-8', newline='\n')
    print('Five complete Python-oracle rowsets match both actual dbt databases; acceptance sealed:', output)


if __name__ == '__main__':
    main()
