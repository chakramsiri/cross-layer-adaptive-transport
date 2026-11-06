#!/usr/bin/env python3
"""Compare SHA-256 hashes of a source and destination file.

Phase 1 integrity check: the received file must be byte-identical to the
sent file. Prints both digests and an unambiguous PASS/FAIL result.

Usage:
    python scripts/verify_sha256.py <source> <destination>

Exit codes:
    0  PASS  hashes match
    1  FAIL  hashes differ
    2  error (file missing, unreadable)
"""

import argparse
import hashlib
import sys
from pathlib import Path

READ_CHUNK_SIZE = 1 << 20


def sha256_file(path):
    """Return the hex SHA-256 digest of a file, read in chunks."""

    digest = hashlib.sha256()

    with open(path, "rb") as file:
        while True:
            chunk = file.read(READ_CHUNK_SIZE)
            if not chunk:
                break
            digest.update(chunk)

    return digest.hexdigest()


def compare_files(source, destination):
    """Return True when both files have the same SHA-256 digest."""

    return sha256_file(source) == sha256_file(destination)


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Compare SHA-256 hashes of a source and destination file."
    )
    parser.add_argument("source", help="file that was sent")
    parser.add_argument("destination", help="file that was received")
    args = parser.parse_args(argv)

    for label, path in (
        ("source", args.source),
        ("destination", args.destination),
    ):
        if not Path(path).is_file():
            print(f"ERROR: {label} file not found: {path}")
            return 2

    source_digest = sha256_file(args.source)
    destination_digest = sha256_file(args.destination)

    print(f"source      {source_digest}")
    print(f"destination {destination_digest}")

    if source_digest == destination_digest:
        print("PASS: SHA-256 hashes match")
        return 0

    print("FAIL: SHA-256 hashes differ")
    return 1


if __name__ == "__main__":
    sys.exit(main())
