#!/usr/bin/env python3
"""Per-file coverage gate: every source file in a `coverage json` report must be >= threshold.

Usage: coverage_gate.py [coverage.json] [threshold]
"""

from __future__ import annotations

import json
import sys


def main() -> int:
    path = sys.argv[1] if len(sys.argv) > 1 else "coverage.json"
    threshold = float(sys.argv[2]) if len(sys.argv) > 2 else 90.0

    with open(path) as f:
        files = json.load(f)["files"]

    under = []
    print(f"{'file':<70} {'%':>6}")
    for name, data in sorted(files.items()):
        pct = data["summary"]["percent_covered"]
        print(f"{name:<70} {pct:6.2f}")
        if pct < threshold:
            under.append((name, pct))

    print(f"\n{len(files)} files checked, threshold {threshold}%")
    if not files:
        # an empty report is a mis-scoped job, not a pass
        print(f"FAIL: {path} reports zero files - the coverage scope is empty or wrong")
        return 1
    if under:
        print(f"\n{len(under)} file(s) below {threshold}%:")
        for name, pct in under:
            print(f"FAIL: {name} {pct:.2f}% < {threshold}%")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
