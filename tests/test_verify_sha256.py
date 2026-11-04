"""Tests for scripts/verify_sha256.py (Phase 1 integrity helper)."""

import hashlib
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from scripts.verify_sha256 import sha256_file, compare_files

REPO_ROOT = Path(__file__).resolve().parents[1]


class VerifySha256Tests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.directory = Path(self.tmp.name)

    def _write(self, name, data):
        path = self.directory / name
        path.write_bytes(data)
        return path

    def test_identical_files_compare_equal(self):
        source = self._write("a.bin", b"same bytes\n")
        destination = self._write("b.bin", b"same bytes\n")

        self.assertTrue(compare_files(source, destination))

    def test_different_files_compare_unequal(self):
        source = self._write("a.bin", b"one\n")
        destination = self._write("b.bin", b"two\n")

        self.assertFalse(compare_files(source, destination))

    def test_empty_files_compare_equal(self):
        source = self._write("a.bin", b"")
        destination = self._write("b.bin", b"")

        self.assertTrue(compare_files(source, destination))

    def test_sha256_matches_hashlib_reference(self):
        data = b"reference check" * 100
        source = self._write("a.bin", data)

        self.assertEqual(
            sha256_file(source), hashlib.sha256(data).hexdigest()
        )

    def test_cli_reports_pass(self):
        source = self._write("a.bin", b"payload")
        destination = self._write("b.bin", b"payload")

        result = subprocess.run(
            [
                sys.executable,
                "scripts/verify_sha256.py",
                str(source),
                str(destination),
            ],
            capture_output=True,
            text=True,
            cwd=REPO_ROOT,
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("PASS", result.stdout)

    def test_cli_reports_fail(self):
        source = self._write("a.bin", b"payload")
        destination = self._write("b.bin", b"tampered")

        result = subprocess.run(
            [
                sys.executable,
                "scripts/verify_sha256.py",
                str(source),
                str(destination),
            ],
            capture_output=True,
            text=True,
            cwd=REPO_ROOT,
        )

        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("FAIL", result.stdout)

    def test_cli_reports_missing_file(self):
        source = self._write("a.bin", b"payload")

        result = subprocess.run(
            [
                sys.executable,
                "scripts/verify_sha256.py",
                str(source),
                str(self.directory / "missing.bin"),
            ],
            capture_output=True,
            text=True,
            cwd=REPO_ROOT,
        )

        self.assertEqual(result.returncode, 2)
        self.assertIn("not found", result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
