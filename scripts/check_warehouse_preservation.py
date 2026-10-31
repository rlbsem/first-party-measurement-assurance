"""Verify original application, source contracts and historical observations remain unchanged."""
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def check():
    manifest = json.loads((ROOT / 'docs/evidence/warehouse-review/preserved-baseline.json').read_text())
    for name, sha in manifest['sha256'].items():
        path = (ROOT / name).resolve()
        if not path.is_relative_to(ROOT) or hashlib.sha256(path.read_bytes()).hexdigest() != sha:
            raise ValueError('Original implementation/evidence changed: ' + name)
    return {'preserved_files': len(manifest['sha256']), 'baseline_revision': manifest['baseline_revision']}


if __name__ == '__main__':
    print(json.dumps(check()))
