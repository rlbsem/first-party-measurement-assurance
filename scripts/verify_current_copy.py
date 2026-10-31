"""Run the unchanged application verifier in a fresh private copy, preserving committed evidence.

No vendor requests. Generated current screenshots/results stay under .local and
are distinguishable from immutable historical records at the repository root.
"""
import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.check_warehouse_preservation import check as preserved
from scripts.package import check_manifest, files


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    if not output.is_relative_to(ROOT / '.local'):
        parser.error('Use a private .local output directory')
    output.mkdir(parents=True, exist_ok=False)
    check_manifest()
    preservation = preserved()
    before = {p.relative_to(ROOT).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest() for p in files()}
    checkout = output / 'source'
    for name in list(before) + ['package-manifest.json']:
        target = checkout / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / name, target)
    # The original verifier sees a complete source checkout but writes only to this fresh copy.
    environment = dict(os.environ)
    environment.pop('PYTHONPATH', None)
    result = subprocess.run([sys.executable, 'scripts/verify.py', '--check-archive'], cwd=checkout,
                            env=environment, timeout=900)
    assert before == {p.relative_to(ROOT).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest() for p in files()}, \
        'Root evidence/source changed during isolated reproduction'
    if result.returncode:
        raise RuntimeError('Application verifier failed; fresh-copy outputs retained')
    preserved()
    revision = (subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip()
                if (ROOT / '.git').exists() else None)
    record = {'status': 'passed', 'source_revision': revision, 'source_sha256': before,
              'preservation': preservation,
              'source_revision_origin': 'git' if (ROOT / '.git').exists() else 'unversioned_source_copy',
              'scope': 'Credential-free application reproduction in a fresh source copy; no native requests.'}
    (output / 'copy-verification.json').write_text(json.dumps(record, indent=2)+'\n', encoding='utf-8', newline='\n')
    print('Current application verified in fresh copy; historical root evidence unchanged')


if __name__ == '__main__':
    main()
