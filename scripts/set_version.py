#!/usr/bin/env python3
"""Synchronize package versions with Cargo.toml."""

import json
import re
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
VERSION = re.compile(r"^\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.-]+)?$")
UPDATES = (
    ("Cargo.toml", r'^(version\s*=\s*)"[^"]+"', 1),
    ("node/package.json", r'^(\s*"version"\s*:\s*)"[^"]+"', 1),
    ("node/package-lock.json", r'^(\s*"version"\s*:\s*)"[^"]+"', 2),
    ("python/pyproject.toml", r'^(version\s*=\s*)"[^"]+"', 1),
)


def versions():
    cargo = tomllib.loads((ROOT / "Cargo.toml").read_text())
    node = json.loads((ROOT / "node/package.json").read_text())
    lock = json.loads((ROOT / "node/package-lock.json").read_text())
    python = tomllib.loads((ROOT / "python/pyproject.toml").read_text())
    return cargo["package"]["version"], {
        "node/package.json": node["version"],
        "node/package-lock.json": lock["version"],
        'node/package-lock.json packages[""]': lock["packages"][""]["version"],
        "python/pyproject.toml": python["project"]["version"],
    }


def set_version(version):
    if not VERSION.fullmatch(version):
        raise SystemExit(f"invalid version: {version!r}")
    for name, pattern, expected in UPDATES:
        path = ROOT / name
        updated, count = re.subn(
            pattern,
            rf'\g<1>"{version}"',
            path.read_text(),
            count=expected,
            flags=re.MULTILINE,
        )
        if count != expected:
            raise SystemExit(f"expected {expected} version field(s) in {name}, found {count}")
        path.write_text(updated)


def check():
    expected, found = versions()
    mismatches = {name: value for name, value in found.items() if value != expected}
    if mismatches:
        raise SystemExit(f"versions do not match Cargo.toml {expected!r}: {mismatches}")
    print(f"package versions match Cargo.toml: {expected}")


if len(sys.argv) != 2 or sys.argv[1] in {"-h", "--help"}:
    raise SystemExit(f"usage: {Path(sys.argv[0]).name} VERSION | --check")
if sys.argv[1] != "--check":
    set_version(sys.argv[1])
check()
