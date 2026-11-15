"""Additive full-source package and integrity inventory, preserving historical package code."""

import argparse
import hashlib
import json
import zipfile
from pathlib import Path
from package import ROOT, files as core_files, audit


def inventory():
    selected = {p.relative_to(ROOT).as_posix(): p for p in core_files()}
    for directory in [
        "docs/evidence/streaming",
        "streaming",
        "streaming_tests",
        "streaming_dbt",
        "infra/streaming",
        "infra/streaming-bootstrap",
    ]:
        for p in (ROOT / directory).rglob("*"):
            if p.is_file() and not any(
                x in p.parts
                for x in [
                    ".terraform",
                    ".local",
                    ".git",
                    ".venv",
                    ".ruff_cache",
                    ".hypothesis",
                    "__pycache__",
                    "target",
                    "logs",
                    ".pytest_cache",
                ]
            ):
                if (
                    p.suffix
                    in [
                        ".py",
                        ".sql",
                        ".tf",
                        ".hcl",
                        ".yml",
                        ".yaml",
                        ".json",
                        ".jsonl",
                        ".md",
                        ".toml",
                        ".tftpl",
                    ]
                    or p.name == "Dockerfile"
                ):
                    selected[p.relative_to(ROOT).as_posix()] = p
    selected["requirements-streaming.lock"] = ROOT / "requirements-streaming.lock"
    selected["package-manifest.json"] = ROOT / "package-manifest.json"
    audit(list(selected.values()))
    return {
        n: hashlib.sha256(p.read_bytes()).hexdigest()
        for n, p in sorted(selected.items())
    }


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--check", action="store_true")
    p.add_argument("--output")
    a = p.parse_args()
    values = inventory()
    mf = ROOT / "streaming-package-manifest.json"
    if a.check:
        if json.loads(mf.read_bytes()) != values:
            raise ValueError("streaming_package_inventory_changed")
        print(json.dumps({"checked_files": len(values)}))
        return
    mf.write_text(json.dumps(values, indent=2) + "\n", encoding="utf-8", newline="\n")
    if a.output:
        target = Path(a.output).resolve()
        if target.is_relative_to(ROOT) or target.exists():
            raise ValueError("fresh_external_package_destination_required")
        target.parent.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(target, "w", zipfile.ZIP_DEFLATED) as z:
            for name in list(values) + [mf.name]:
                z.write(ROOT / name, "first-party-measurement-assurance/" + name)
        with zipfile.ZipFile(target) as z:
            for name, h in values.items():
                if (
                    hashlib.sha256(
                        z.read("first-party-measurement-assurance/" + name)
                    ).hexdigest()
                    != h
                ):
                    raise ValueError("archive_content_mismatch")
        print(
            json.dumps(
                {
                    "files": len(values) + 1,
                    "zip_sha256": hashlib.sha256(target.read_bytes()).hexdigest(),
                }
            )
        )


if __name__ == "__main__":
    main()
