#!/usr/bin/env python3
"""Check changed Rust and Python lines against an 80-column limit."""

import argparse
import re
import subprocess
import sys

MAX_LINE_LENGTH = 80
HUNK_RE = re.compile(r"@@ -\d+(?:,\d+)? \+(\d+)(?:,(\d+))? @@")


def read_diff(base_ref):
    command = [
        "git",
        "diff",
        "--no-ext-diff",
        "--no-color",
        "--unified=0",
        "{}...HEAD".format(base_ref),
        "--",
        "*.rs",
        "*.py",
    ]
    result = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        sys.stderr.write(result.stderr)
        raise SystemExit(result.returncode)
    return result.stdout


def find_long_lines(diff):
    violations = []
    path = None
    new_line_number = 0

    for line in diff.splitlines():
        if line.startswith("+++ b/"):
            path = line[6:]
            continue

        if line.startswith("@@"):
            match = HUNK_RE.search(line)
            if match:
                new_line_number = int(match.group(1))
            continue

        if line.startswith("+") and not line.startswith("+++"):
            added_line = line[1:]
            length = len(added_line)
            if length > MAX_LINE_LENGTH:
                violations.append(
                    (path, new_line_number, length, added_line)
                )
            new_line_number += 1
            continue

        if line.startswith("-") and not line.startswith("---"):
            continue

        if line.startswith(" "):
            new_line_number += 1

    return violations


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Fail when changed Rust or Python lines exceed 80 columns."
        )
    )
    parser.add_argument(
        "--base",
        required=True,
        help="Git ref for the pull request base branch.",
    )
    args = parser.parse_args()

    diff = read_diff(args.base)
    violations = find_long_lines(diff)

    if violations:
        for path, line_number, length, content in violations:
            print(
                "{}:{}: {} columns (limit {}): {}".format(
                    path,
                    line_number,
                    length,
                    MAX_LINE_LENGTH,
                    content,
                )
            )
        print(
            "Found {} line(s) exceeding {} columns.".format(
                len(violations),
                MAX_LINE_LENGTH,
            ),
            file=sys.stderr,
        )
        return 1

    print("Changed Rust and Python lines satisfy the 80-column limit.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
