# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed under
# the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
# See the License for the specific language governing permissions and limitations under the License.
"""Daemon-free failure and ownership regressions for the native smoke lane."""
import importlib.machinery
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

SOURCE = Path(__file__).resolve().parents[1] / "native-smoke"
LOADER = importlib.machinery.SourceFileLoader("native_smoke", str(SOURCE))
SPEC = importlib.util.spec_from_loader(LOADER.name, LOADER)
smoke = importlib.util.module_from_spec(SPEC)
LOADER.exec_module(smoke)


class FakeDocker:
    def __init__(self, lane, expected):
        self.lane = lane
        self.expected = expected
        self.commands = []
        self.round = -1
        self.active = False
        self.collision = None
        self.failure = None
        self.reuse = False
        self.unknown = False
        self.wrong_bind = False

    def labels(self):
        return {"com.docker.compose.project": self.lane.project,
                "com.aurora.lab.lane": "native-smoke", "com.aurora.lab.owned": "true",
                "com.aurora.lab.run": str(self.lane.args.run_root)}

    def containers(self):
        result = []
        for index, (service, state) in enumerate(smoke.SERVICES.items()):
            identity = f"{1 if self.reuse else self.round + 1:02x}{index:062x}"
            labels = dict(self.labels(), **{"com.docker.compose.service": service})
            if self.unknown:
                labels["com.aurora.lab.run"] = "/unknown"
            result.append({"Id": identity, "Image": "sha256:built", "Config": {
                "User": "1000:1000", "Labels": labels}, "Mounts": [{"Type": "bind", "RW": True,
                "Source": "/unknown" if self.wrong_bind else str(
                    self.lane.args.run_root / "state" / state), "Destination": "/var/lib/aurora"}]})
        return result

    def __call__(self, command, env, timeout=30, check=True, cwd=None):
        cmd = command[1:]
        self.commands.append(cmd)
        out = ""
        if cmd[:2] == ["image", "inspect"]:
            out = json.dumps([{"Architecture": "arm64", "Os": "linux", "Id": "sha256:built"}])
        elif cmd[:1] == ["build"]:
            if self.failure == "build":
                raise smoke.SmokeError("build failed")
        elif cmd[:1] == ["compose"]:
            if "up" in cmd:
                self.round += 1
                self.active = True
                if self.failure == "up":
                    raise smoke.SmokeError("partial startup failed")
            elif "down" in cmd:
                if self.failure == "down":
                    raise smoke.SmokeError("cleanup failed")
                self.active = False
        elif cmd[:1] == ["ps"]:
            if self.collision == "container" and self.round < 0:
                out = "unowned-container"
            elif self.active:
                out = "\n".join(item["Id"] for item in self.containers())
        elif cmd[:2] in (["network", "ls"], ["volume", "ls"], ["image", "ls"]):
            kind = cmd[0]
            if self.collision == kind and self.round < 0:
                out = "unowned-" + kind
            elif kind == "network" and self.active:
                out = "n0\nn1\nn2"
        elif cmd[:2] == ["network", "inspect"]:
            out = json.dumps([{"Name": self.lane.project + "_" + name,
                               "Internal": True, "Labels": self.labels()}
                              for name in smoke.SERVICES.values()])
        elif cmd[:1] == ["inspect"]:
            out = json.dumps(self.containers())
        elif cmd[:1] == ["wait"]:
            if self.failure == "timeout":
                raise smoke.SmokeError("Command timed out: docker")
            out = "17" if self.failure == "exit" else "0"
        elif cmd[:1] == ["logs"]:
            container = next(item for item in self.containers() if item["Id"] == cmd[-1])
            service = container["Config"]["Labels"]["com.docker.compose.service"]
            if service == "store-check":
                out = ("NATIVE_SQL_OK sqlite-jdbc=3.53.4.0 WAL FULL reopen rollback-only "
                       "arch=aarch64 java=1.8.0_462 preexisting=" + str(self.round == 1).lower())
            else:
                node = smoke.SERVICES[service]
                result = self.expected[node]
                snapshot = {"cursor": "1", "ack": "0", "commands": {result["command"]: result},
                            "attempts": {"a": {"reserved": True, "stopped": False}},
                            "observations": [{"cursor": "1", "sequence": "1", "source": {"node": node}}]}
                if self.failure == "replay" and self.round == 1:
                    snapshot["cursor"] = "2"
                out = json.dumps(result) + "\n" + json.dumps(snapshot)
        else:
            raise AssertionError("Unmocked Docker command: " + repr(cmd))
        return subprocess.CompletedProcess(command, 0, out, "")


class NativeSmokeTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / "evidence").mkdir()
        self.args = SimpleNamespace(run_root=self.root, uid=1000, gid=1000,
                                    base_image="debian@sha256:" + "a" * 64)
        self.lane = smoke.DockerLane(self.args, {})
        runtime = self.root / "runtime"
        runtime.mkdir()
        self.expected = smoke.prepare_fixtures(runtime)
        self.docker = FakeDocker(self.lane, self.expected)
        self.evidence = {"rounds": [], "recreated": False}

    def execute(self):
        with patch.object(smoke, "run", side_effect=self.docker):
            status = smoke.execute(self.lane, self.evidence, self.expected)
        persisted = json.loads((self.root / "evidence/result.json").read_text())
        self.assertEqual(status == 0, persisted["ok"])
        return status

    def test_three_services_recreated_and_dedupe_verified(self):
        self.assertEqual(0, self.execute())
        self.assertEqual("complete", self.evidence["cleanup"])
        self.assertEqual(2, len(self.evidence["rounds"]))
        first, second = self.evidence["rounds"]
        self.assertFalse(set(first["container_ids"]) & set(second["container_ids"]))
        self.assertEqual(set(smoke.SERVICES), set(second["services"]))
        self.assertNotEqual(self.expected["agent-1"]["command"], self.expected["agent-2"]["command"])
        self.assertFalse(any("--remove-orphans" in cmd for cmd in self.docker.commands))

    def test_individual_exit_failure_is_not_masked(self):
        self.docker.failure = "exit"
        self.assertNotEqual(0, self.execute())
        partial = self.evidence["rounds"][0]["services"]
        self.assertTrue(any(result["exit_code"] == 17 for result in partial.values()))
        self.assertEqual("complete", self.evidence["cleanup"])

    def test_timeout_keeps_partial_evidence_and_cleans_owned_resources(self):
        self.docker.failure = "timeout"
        self.assertNotEqual(0, self.execute())
        self.assertIn("timed out", self.evidence["error"])
        self.assertEqual("complete", self.evidence["cleanup"])

    def test_cleanup_failure_overrides_success(self):
        self.docker.failure = "down"
        self.assertEqual(5, self.execute())
        self.assertEqual("failed", self.evidence["cleanup"])
        self.assertTrue(self.root.exists())

    def test_collisions_never_enter_cleanup(self):
        for kind in ("container", "network", "volume", "image"):
            with self.subTest(kind=kind):
                self.docker.collision = kind
                self.docker.commands.clear()
                self.assertNotEqual(0, self.execute())
                self.assertFalse(any("down" in cmd for cmd in self.docker.commands))
                self.assertFalse(any("build" in cmd for cmd in self.docker.commands))

    def test_build_failure_does_not_run_down(self):
        self.docker.failure = "build"
        self.assertNotEqual(0, self.execute())
        self.assertFalse(any("down" in cmd for cmd in self.docker.commands))

    def test_partial_startup_is_cleaned(self):
        self.docker.failure = "up"
        self.assertNotEqual(0, self.execute())
        self.assertEqual("complete", self.evidence["cleanup"])

    def test_unknown_containers_are_preserved(self):
        self.docker.unknown = True
        self.assertEqual(5, self.execute())
        self.assertFalse(any("down" in cmd for cmd in self.docker.commands))

    def test_wrong_bind_is_rejected_and_preserved(self):
        self.docker.wrong_bind = True
        self.assertEqual(5, self.execute())
        self.assertFalse(any("down" in cmd for cmd in self.docker.commands))

    def test_reused_container_ids_fail(self):
        self.docker.reuse = True
        self.assertNotEqual(0, self.execute())
        self.assertIn("fresh container", self.evidence["error"])

    def test_recreation_cursor_regression_fails(self):
        self.docker.failure = "replay"
        self.assertNotEqual(0, self.execute())
        self.assertIn("Fresh agent inspection", self.evidence["error"])

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

    def test_root_and_mismatched_uid_refuse(self):
        self.args.run_root = self.root / "new"
        with patch.object(os, "getuid", return_value=1000), patch.object(os, "getgid", return_value=1000):
            for uid in (0, -1, 1001):
                self.args.uid = uid
                with self.assertRaises(smoke.SmokeError):
                    smoke.validate_args(self.args)

    def test_runner_nonzero_timeout_and_redaction(self):
        with patch.object(subprocess, "run", return_value=subprocess.CompletedProcess(
                ["docker"], 3, "", 'password="sensitive"')):
            with self.assertRaises(smoke.SmokeError) as error:
                smoke.run(["docker"], {})
            self.assertNotIn("sensitive", str(error.exception))
        with patch.object(subprocess, "run", side_effect=subprocess.TimeoutExpired(["docker"], 1)):
            with self.assertRaises(smoke.SmokeError):
                smoke.run(["docker"], {})


if __name__ == "__main__":
    unittest.main()
