# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed under
# the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
# See the License for the specific language governing permissions and limitations under the License.
"""Shared lab utilities and retired-builder boundary regressions."""
import importlib.machinery
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

SOURCE = Path(__file__).resolve().parents[1] / "lab_common.py"
LOADER = importlib.machinery.SourceFileLoader("lab_common_test", str(SOURCE))
SPEC = importlib.util.spec_from_loader(LOADER.name, LOADER)
smoke = importlib.util.module_from_spec(SPEC)
LOADER.exec_module(smoke)


class LabCommonTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def test_symlink_and_control_paths_refuse(self):
        target = self.root / "target"
        target.write_text("preserve")
        link = self.root / "link"
        link.symlink_to(target)
        for path in (link, self.root / "bad\npath", self.root / ".." / "escape"):
            with self.assertRaises(smoke.SmokeError):
                smoke.safe_path(path)
        with self.assertRaises(smoke.SmokeError):
            smoke.write_json(link, {"bad": True})
        self.assertEqual("preserve", target.read_text())

    def test_runner_nonzero_timeout_and_redaction(self):
        with patch.object(subprocess, "run", return_value=subprocess.CompletedProcess(
                ["docker"], 3, "", 'password="sensitive"')):
            with self.assertRaises(smoke.SmokeError) as error:
                smoke.run(["docker"], {})
            self.assertNotIn("sensitive", str(error.exception))
        with patch.object(subprocess, "run", side_effect=subprocess.TimeoutExpired(["docker"], 1)):
            with self.assertRaises(smoke.SmokeError):
                smoke.run(["docker"], {})

    def test_retired_builder_rejects_without_creating_a_run(self):
        target = self.root / "unused"
        result = subprocess.run([sys.executable, str(SOURCE.parent / "native-smoke"),
                                 "--run-root", str(target)], capture_output=True, text=True, timeout=10)
        self.assertEqual(2, result.returncode)
        self.assertFalse(json.loads(result.stderr)["ok"])
        self.assertIn("retired", json.loads(result.stderr)["error"])
        self.assertFalse(target.exists())


if __name__ == "__main__":
    unittest.main()
