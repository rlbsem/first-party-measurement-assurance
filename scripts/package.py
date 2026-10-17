"""Allowlist packaging, targeted public-content audit, and exact ZIP verification."""
import argparse
import hashlib
import json
import re
import os
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DIRECTORIES = {'assurance', 'contracts', 'dbt', 'web', 'bigquery', 'scripts', 'tests', 'docs', '.github'}
ROOT_FILES = {'README.md', 'LICENSE', '.gitignore', '.gitattributes', 'pyproject.toml', 'requirements.txt', 'requirements.lock'}
EXTENSIONS = {'.py', '.json', '.sql', '.yml', '.yaml', '.html', '.css', '.js', '.md', '.png', '.txt', '.lock', '.toml'}


def files():
    result = []
    candidates = [ROOT / name for name in ROOT_FILES if (ROOT/name).is_file()]
    for folder in DIRECTORIES:
        for base, directories, names in os.walk(ROOT/folder):
            directories[:] = [d for d in directories if d not in ('__pycache__','target','logs','dbt_packages')]
            candidates.extend(Path(base)/n for n in names)
    for path in candidates:
        rel = path.relative_to(ROOT)
        if not path.is_file() or any(p in ('__pycache__', 'target', 'logs', 'dbt_packages') for p in rel.parts):
            continue
        if path.is_symlink():
            raise ValueError(f'symlink not allowed: {rel}')
        if len(rel.parts) == 1 and path.name in ROOT_FILES:
            result.append(path)
        elif rel.parts[0] in DIRECTORIES and path.suffix in EXTENSIONS:
            result.append(path)
    return sorted(result)


def audit(paths):
    patterns = [r'-----BEGIN ' + r'(?:RSA |EC |OPENSSH )?PRIVATE KEY-----',
                r'\b(?:ghp|github_pat)_[A-Za-z0-9_]{20,}',
                r'\bAKIA[A-Z0-9]{16}\b', r'[A-Z]:\\' + r'(?:Users|csa)\\',
                r'(?i)(?:api_secret|access_token)\s*[:=]\s*[\"\x27][A-Za-z0-9_-]{20,}']
    for path in paths:
        if path.suffix == '.png':
            continue
        text = path.read_text(encoding='utf-8')
        if any(re.search(pattern, text) for pattern in patterns):
            raise ValueError(f'public content audit finding: {path.relative_to(ROOT)}')
        if '\r\n' in path.read_bytes().decode('utf-8'):
            raise ValueError(f'noncanonical CRLF content: {path.relative_to(ROOT)}')
    return {'files_reviewed': len(paths), 'credential_patterns': 'no findings', 'private_paths': 'no findings'}


def manifest():
    paths = files()
    audit(paths)
    return {p.relative_to(ROOT).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}


def write_manifest():
    value = manifest()
    (ROOT/'package-manifest.json').write_text(json.dumps(value, indent=2)+'\n', encoding='utf-8', newline='\n')
    return value


def check_manifest():
    recorded = json.loads((ROOT/'package-manifest.json').read_text())
    if recorded != manifest():
        raise ValueError('package manifest does not match public file inventory')
    return len(recorded)


def package(output):
    values = write_manifest()
    output = Path(output).resolve()
    if output.is_relative_to(ROOT):
        raise ValueError('write delivery ZIP outside the repository')
    output.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(output, 'w', compression=zipfile.ZIP_DEFLATED) as z:
        for name in list(values) + ['package-manifest.json']:
            z.write(ROOT/name, f'first-party-measurement-assurance/{name}')
    with zipfile.ZipFile(output) as z:
        assert len(z.namelist()) == len(values)+1
        for name, expected in values.items():
            assert hashlib.sha256(z.read(f'first-party-measurement-assurance/{name}')).hexdigest() == expected
    return {'files': len(values)+1, 'zip_sha256': hashlib.sha256(output.read_bytes()).hexdigest()}


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--output', default='../outputs/first-party-measurement-assurance.zip')
    p.add_argument('--check', action='store_true')
    a = p.parse_args()
    print(check_manifest() if a.check else package(a.output))
