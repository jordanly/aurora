# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed under
# the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
# See the License for the specific language governing permissions and limitations under the License.
"""Failure regressions; these mocks are not physical runtime qualification."""
import importlib.machinery
import importlib.util
import json
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

SOURCE = Path(__file__).resolve().parents[1] / "process-smoke"
LOADER = importlib.machinery.SourceFileLoader("process_smoke", str(SOURCE))
SPEC = importlib.util.spec_from_loader(LOADER.name, LOADER)
smoke = importlib.util.module_from_spec(SPEC)
LOADER.exec_module(smoke)


class FakeHarness:
    def __init__(self, root):
        self.run = root
        self.evidence = {"cases": []}
        self.cleanup_called = False
        self.incomplete = False
        self.case_error = False
        self.cleanup_error = False

    def cases(self):
        if self.case_error:
            self.evidence["cases"].append({"case": smoke.CASE_NAMES[0], "ok": False})
            raise smoke.CheckFailure("Expected physical outcome missing")
        names = smoke.CASE_NAMES[:1] if self.incomplete else smoke.CASE_NAMES
        self.evidence["cases"] = [{"case": name, "ok": True} for name in names]

    def cleanup(self):
        self.cleanup_called = True
        if self.cleanup_error:
            raise smoke.CheckFailure("Cleanup unresolved")

    def save(self):
        smoke.write_json(self.run / "result.json", self.evidence)


class ProcessSmokeTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def test_incomplete_declared_cases_cannot_succeed(self):
        harness = FakeHarness(self.root)
        harness.incomplete = True
        self.assertNotEqual(0, smoke.run_harness(harness))
        self.assertFalse(harness.evidence["ok"])
        self.assertTrue(harness.cleanup_called)

    def test_partial_case_failure_retains_evidence_and_cleans(self):
        harness = FakeHarness(self.root)
        harness.case_error = True
        self.assertNotEqual(0, smoke.run_harness(harness))
        persisted = json.loads((self.root / "result.json").read_text())
        self.assertFalse(persisted["ok"])
        self.assertFalse(persisted["cases"][0]["ok"])
        self.assertEqual("complete", persisted["cleanup"])

    def test_cleanup_failure_overrides_all_case_success(self):
        harness = FakeHarness(self.root)
        harness.cleanup_error = True
        self.assertEqual(5, smoke.run_harness(harness))
        self.assertFalse(harness.evidence["ok"])
        self.assertEqual("failed", harness.evidence["cleanup"])
        self.assertTrue(self.root.is_dir())

    def test_terminal_success_without_cleanup_is_rejected(self):
        harness = object.__new__(smoke.Harness)
        attempt = {"reserved": True, "execution": {
            "phase": "terminal", "outcome": "succeeded", "exitCode": 0,
            "cleanup": "unknown", "ready": False}}
        harness.wait_attempt = lambda *args, **kwargs: ("key", attempt)
        with self.assertRaises(smoke.CheckFailure):
            harness.terminal({}, "succeeded", code=0)

    def test_wrong_exit_code_is_not_success(self):
        harness = object.__new__(smoke.Harness)
        attempt = {"reserved": False, "execution": {
            "phase": "terminal", "outcome": "failed", "exitCode": 8,
            "cleanup": "complete", "ready": False}}
        harness.wait_attempt = lambda *args, **kwargs: ("key", attempt)
        with self.assertRaises(smoke.CheckFailure):
            harness.terminal({}, "failed", code=7)

    def test_admission_only_never_satisfies_terminal_wait(self):
        harness = object.__new__(smoke.Harness)
        harness.current = {}
        harness.attempt = lambda _: ("key", {"reserved": True, "execution": None})
        with self.assertRaises(smoke.CheckFailure):
            harness.wait_attempt({}, lambda a: (a.get("execution") or {}).get("phase") == "terminal",
                                 timeout=0.01)

    def fake_agent(self, output):
        executable = self.root / "fake-agent"
        executable.write_text("#!/usr/bin/env python3\nimport sys,time\n"
                              "sys.stdin.buffer.readline()\n"
                              f"sys.stdout.buffer.write({output!r})\n"
                              "sys.stdout.buffer.flush()\ntime.sleep(10)\n")
        executable.chmod(0o700)
        agent = smoke.Agent(executable, self.root, self.root / "unused-config", 0)
        self.addCleanup(lambda: agent.crash() if agent.process.poll() is None else agent.release_streams())
        return agent

    def test_partial_reply_obeys_deadline(self):
        agent = self.fake_agent(b'{"ok":')
        started = time.monotonic()
        with self.assertRaises(smoke.CheckFailure):
            agent.request({"action": "inspect"}, timeout=0.15)
        self.assertLess(time.monotonic() - started, 1)

    def test_malformed_reply_is_rejected(self):
        agent = self.fake_agent(b'{bad-json}\n')
        with self.assertRaises(smoke.CheckFailure):
            agent.request({"action": "inspect"})

    def test_failed_agent_start_closes_stderr(self):
        stream = (self.root / "agent-0.stderr").open("xb", buffering=0)
        with patch.object(Path, "open", return_value=stream):
            with self.assertRaises(FileNotFoundError):
                smoke.Agent(self.root / "missing", self.root, self.root / "config", 0)
        self.assertTrue(stream.closed)

    def test_identity_mismatch_never_pins_unrelated_pid(self):
        owned = smoke.OwnedProcesses()
        with patch.object(smoke.os, "pidfd_open", return_value=51), \
                patch.object(smoke, "process_info", return_value=("different", "S")), \
                patch.object(smoke.os, "close") as close:
            owned.observe({"execution": {"pid": 123, "start": "expected"}})
            self.assertEqual({}, owned.handles)
            close.assert_called_once_with(51)

    def test_symlink_control_traversal_and_error_redaction(self):
        target = self.root / "preserved"
        target.write_text("unchanged")
        link = self.root / "link"
        link.symlink_to(target)
        for path in (link, self.root / "bad\npath", self.root / ".." / "escape"):
            with self.assertRaises(smoke.CheckFailure):
                smoke.safe_path(path)
        with self.assertRaises(smoke.CheckFailure):
            smoke.write_json(link, {"overwrite": True})
        self.assertEqual("unchanged", target.read_text())
        self.assertNotIn("sensitive", smoke.public_error(Exception('token="sensitive"')))

    def test_build_failure_is_preserved_without_starting_agent(self):
        root = self.root / "new-run"
        with patch.object(smoke.platform, "machine", return_value="aarch64"), \
                patch.object(smoke.os, "getuid", return_value=1000), \
                patch.object(smoke, "build", side_effect=smoke.CheckFailure("build failed")), \
                patch.object(smoke, "Agent") as agent:
            self.assertNotEqual(0, smoke.main(["--run-root", str(root)]))
        agent.assert_not_called()
        self.assertFalse(json.loads((root / "result.json").read_text())["ok"])


if __name__ == "__main__":
    unittest.main()
