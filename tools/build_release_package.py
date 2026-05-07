#!/usr/bin/env python3
"""Build a customer-facing comparator toolkit package from tracked git files."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import posixpath
import subprocess
import sys
import zipfile
from datetime import datetime, timezone
from typing import Iterable, List, Sequence, Tuple

REQUIRED_FILES = (
    "schema_diff_reconciler.py",
    "run_fixup.py",
    "diagnostic_bundle.py",
    "comparator_reliability.py",
    "config.ini.template.txt",
    "readme_config.txt",
    "readme_lite.txt",
    "README.md",
    "blacklist_rules.json",
    "compatibility_registry.json",
)

EXCLUDED_PREFIXES = (
    ".git/",
    ".github/",
    ".codex/",
    ".agents/",
    "tools/",
    "main_reports/",
    "fixup_scripts/",
    "dbcat_output/",
    "logs/",
    "__pycache__/",
)

EXCLUDED_FILES = (
    "config.ini",
    ".env",
    ".secrets.baseline",
    "requirements-dev.txt",
    "pyproject.toml",
    "CODE_OF_CONDUCT.md",
    "SECURITY.md",
    "SUPPORT.md",
    "MAINTAINERS.md",
    "ROADMAP.md",
)

FORBIDDEN_ARCHIVE_PARTS = (
    "/config.ini",
    "/.env",
    "/.secrets.baseline",
)


def run_git(args: Sequence[str]) -> str:
    completed = subprocess.run(
        ["git"] + list(args),
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    return completed.stdout.strip()


def utc_now() -> str:
    value = datetime.now(timezone.utc).replace(microsecond=0)
    return value.isoformat().replace("+00:00", "Z")


def normalize_git_path(path: str) -> str:
    return path.strip().replace("\\", "/")


def should_package(path: str) -> bool:
    normalized = normalize_git_path(path)
    if not normalized or normalized in EXCLUDED_FILES:
        return False
    return not normalized.startswith(EXCLUDED_PREFIXES)


def tracked_files(commit: str) -> List[str]:
    output = run_git(["ls-tree", "-r", "--name-only", commit])
    files = [normalize_git_path(line) for line in output.splitlines() if line.strip()]
    return [path for path in files if should_package(path)]


def git_file_bytes(commit: str, path: str) -> bytes:
    completed = subprocess.run(
        ["git", "show", "%s:%s" % (commit, path)],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    return completed.stdout


def sha256_file(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def validate_file_list(files: Sequence[str]) -> Tuple[List[str], List[str]]:
    file_set = set(files)
    missing = [path for path in REQUIRED_FILES if path not in file_set]
    forbidden = []
    for path in files:
        if path in EXCLUDED_FILES or path.startswith(EXCLUDED_PREFIXES):
            forbidden.append(path)
        archive_path = "/" + path
        if any(archive_path.endswith(part) for part in FORBIDDEN_ARCHIVE_PARTS):
            forbidden.append(path)
    return missing, sorted(set(forbidden))


def write_zip(commit: str, version: str, output_dir: str, files: Sequence[str]) -> str:
    zip_name = "ob_comparator-%s-toolkit.zip" % version
    zip_path = os.path.join(output_dir, zip_name)
    prefix = "ob_comparator-%s" % version
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in files:
            archive_name = posixpath.join(prefix, path)
            info = zipfile.ZipInfo(archive_name)
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o644 << 16
            archive.writestr(info, git_file_bytes(commit, path))
    return zip_path


def write_json(path: str, payload: object) -> None:
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")


def write_checksums(path: str, paths: Iterable[str]) -> None:
    with open(path, "w", encoding="utf-8") as handle:
        for item in paths:
            handle.write("%s  %s\n" % (sha256_file(item), os.path.basename(item)))


def build(version: str, commitish: str, output_dir: str) -> Tuple[str, str, str]:
    commit = run_git(["rev-parse", "--verify", commitish])
    os.makedirs(output_dir, exist_ok=True)
    files = tracked_files(commit)
    missing, forbidden = validate_file_list(files)
    if missing or forbidden:
        if missing:
            print("Missing required release files:", file=sys.stderr)
            print("\n".join(missing), file=sys.stderr)
        if forbidden:
            print("Forbidden files selected for release:", file=sys.stderr)
            print("\n".join(forbidden), file=sys.stderr)
        raise SystemExit(2)

    zip_path = write_zip(commit, version, output_dir, files)
    manifest_path = os.path.join(output_dir, "ob_comparator-%s-manifest.json" % version)
    checksum_path = os.path.join(output_dir, "ob_comparator-%s-SHA256SUMS.txt" % version)
    manifest = {
        "version": version,
        "commit": commit,
        "generated_at": utc_now(),
        "toolkit_zip": os.path.basename(zip_path),
        "required_files": list(REQUIRED_FILES),
        "excluded_prefixes": list(EXCLUDED_PREFIXES),
        "excluded_files": list(EXCLUDED_FILES),
        "file_count": len(files),
        "files": list(files),
    }
    write_json(manifest_path, manifest)
    write_checksums(checksum_path, (zip_path, manifest_path))
    return zip_path, manifest_path, checksum_path


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", required=True, help="Release version without leading v")
    parser.add_argument("--commitish", default="HEAD", help="Git ref to package")
    parser.add_argument("--output-dir", default="dist", help="Output directory")
    return parser.parse_args(argv)


def main(argv: Sequence[str]) -> int:
    args = parse_args(argv)
    zip_path, manifest_path, checksum_path = build(
        version=args.version,
        commitish=args.commitish,
        output_dir=args.output_dir,
    )
    print("Built %s" % zip_path)
    print("Wrote %s" % manifest_path)
    print("Wrote %s" % checksum_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
