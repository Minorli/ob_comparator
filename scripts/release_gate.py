#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Validate release evidence before publishing comparator releases."""

from __future__ import annotations

import sys
from pathlib import Path


def main() -> int:
    repo_root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(repo_root))
    from comparator_reliability import release_gate_main

    return release_gate_main(sys.argv[1:])


if __name__ == "__main__":
    sys.exit(main())
