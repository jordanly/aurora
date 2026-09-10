# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed under
# the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
# See the License for the specific language governing permissions and limitations under the License.
"""Mocked ownership/failure regressions; no Docker execution qualification."""
import importlib.machinery
import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import MagicMock, patch

SOURCE = Path(__file__).resolve().parents[1] / "process-container-smoke"
LOADER = importlib.machinery.SourceFileLoader("process_container_smoke", str(SOURCE))
SPEC = importlib.util.spec_from_loader(LOADER.name, LOADER)
smoke = importlib.util.module_from_spec(SPEC)
LOADER.exec_module(smoke)


def result(stdout="", code=0):
    return subprocess.CompletedProcess(["docker"], code, stdout, "")


class ContainerSmokeTest(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)

    def agent(self):
        agent = object.__new__(smoke.ContainerAgent)
        agent.run = self.root
        agent.name = "aurora-owned-test"
        agent.labels = {"com.aurora.lab.run": str(self.root),
                        "com.aurora.lab.lane": "process-container-smoke"}
        agent.image = "pinned-image"
        agent.container_id = "a" * 64
        agent.cid_path = self.root / "container.cid"
        agent.closed = False
        agent.process = MagicMock()
        agent.process.poll.return_value = 0
        agent.process.wait.return_value = 0
        agent.selector = MagicMock()
        agent.stderr = MagicMock()
        agent.release_streams = MagicMock()
        agent.request = MagicMock(return_value={"ok": True, "result": "shutdown"})
        return agent

    def inspection(self, agent):
        mounts = [{"Destination": str(self.root), "Type": "bind", "Source": str(self.root), "RW": False}]
        mounts += [{"Destination": str(self.root / name), "Type": "bind",
                    "Source": str(self.root / name), "RW": True}
                   for name in ("state", "work", "workload-evidence")]
        return {"Id": agent.container_id, "Name": "/" + agent.name, "Image": "pinned-image-id",
                "Config": {"Labels": agent.labels.copy(), "User": f"{smoke.os.getuid()}:{smoke.os.getgid()}"},
                "HostConfig": {"ReadonlyRootfs": True, "Init": True, "NetworkMode": "none",
                               "PidMode": "", "Privileged": False, "CapDrop": ["ALL"],
                               "SecurityOpt": ["no-new-privileges:true"]},
                "Mounts": mounts, "State": {"Running": False, "ExitCode": 0}}

    def test_owned_cleanup_uses_immutable_id(self):
        agent = self.agent()
        item = self.inspection(agent)
        calls = []
        def docker(*args, **kwargs):
            calls.append(args)
            return result(json.dumps([item])) if args[0] == "inspect" else result()
        with patch.object(smoke, "docker", side_effect=docker):
            agent.close()
        self.assertIn(("rm", "-f", agent.container_id), calls)
        agent.release_streams.assert_called_once()

    def test_inspect_failure_preserves_container_and_releases_client(self):
        agent = self.agent()
        with patch.object(smoke, "docker", return_value=result(code=1)) as docker:
            with self.assertRaises(smoke.native.CheckFailure):
                agent.close()
        self.assertFalse(any(c.args[0] == "rm" for c in docker.call_args_list))
        agent.release_streams.assert_called_once()

    def test_client_reap_timeout_still_closes_streams(self):
        agent = self.agent()
        agent.process.poll.return_value = None
        agent.process.wait.side_effect = subprocess.TimeoutExpired("docker", 3)
        with patch.object(agent, "inspect_container", side_effect=smoke.native.CheckFailure("unknown ownership")):
            with self.assertRaises(subprocess.TimeoutExpired):
                agent.close()
        agent.process.kill.assert_called_once()
        agent.release_streams.assert_called_once()

    def test_label_id_mount_mismatches_never_remove_container(self):
        for mutation in ("label", "id", "mount"):
            with self.subTest(mutation=mutation):
                agent = self.agent()
                item = self.inspection(agent)
                if mutation == "label":
                    item["Config"]["Labels"]["com.aurora.lab.run"] = "unrelated"
                elif mutation == "id":
                    item["Id"] = "b" * 64
                else:
                    item["Mounts"][0]["Source"] = "/unrelated"
                with patch.object(smoke, "docker", return_value=result(json.dumps([item]))) as docker:
                    with self.assertRaises(smoke.native.CheckFailure):
                        agent.close()
                self.assertFalse(any(c.args[0] == "rm" for c in docker.call_args_list))
                agent.release_streams.assert_called_once()

    def test_host_pid_privileged_or_unconfined_configuration_rejected(self):
        for field, value in (("PidMode", "host"), ("Privileged", True),
                             ("SecurityOpt", ["no-new-privileges:true", "seccomp=unconfined"])):
            with self.subTest(field=field):
                agent = self.agent()
                item = self.inspection(agent)
                item["HostConfig"][field] = value
                with patch.object(smoke, "docker", return_value=result(json.dumps([item]))):
                    with self.assertRaises(smoke.native.CheckFailure):
                        agent.inspect_container()

    def test_existing_name_collision_never_starts_or_removes(self):
        with patch.object(smoke, "docker", return_value=result("existing-id\n")) as docker, \
                patch.object(smoke.subprocess, "Popen") as popen:
            with self.assertRaises(smoke.native.CheckFailure):
                smoke.ContainerAgent(self.root, "pinned-image")
        popen.assert_not_called()
        self.assertFalse(any(c.args[0] == "rm" for c in docker.call_args_list))

    def test_partial_client_setup_failure_reaps_client(self):
        client = MagicMock()
        client.poll.return_value = None
        client.wait.side_effect = [subprocess.TimeoutExpired("docker", 3), 0]
        with patch.object(smoke, "docker", return_value=result()) as docker, \
                patch.object(smoke.subprocess, "Popen", return_value=client), \
                patch.object(smoke.os, "set_blocking", side_effect=OSError("setup failed")):
            agent = smoke.ContainerAgent(self.root, "pinned-image")
            with self.assertRaises(OSError):
                agent.start()
            # A Docker creation without cidfile stays unknown; only our attached
            # client can be reaped. Never adopt the matching name during cleanup.
            with patch.object(agent, "inspect_container", side_effect=smoke.native.CheckFailure("unknown creation")):
                with self.assertRaises(smoke.native.CheckFailure):
                    agent.close()
        client.kill.assert_called_once()
        client.wait.assert_called()
        client.stdin.close.assert_called_once()
        client.stdout.close.assert_called_once()
        self.assertTrue(agent.stderr.closed)
        self.assertFalse(any(c.args[0] == "rm" for c in docker.call_args_list))

    def test_missing_creation_identity_never_adopts_name(self):
        agent = self.agent()
        agent.container_id = None
        with patch.object(smoke, "docker") as docker:
            with self.assertRaises(smoke.native.CheckFailure):
                agent.close()
        docker.assert_not_called()
        agent.release_streams.assert_called_once()

    def test_creation_identity_symlink_never_adopts_target(self):
        agent = self.agent()
        agent.container_id = None
        target = self.root / "unrelated-cid"
        target.write_text("b" * 64)
        agent.cid_path.symlink_to(target)
        with patch.object(smoke, "docker") as docker:
            with self.assertRaises(smoke.native.CheckFailure):
                agent.close()
        docker.assert_not_called()
        self.assertEqual("b" * 64, target.read_text())
        agent.release_streams.assert_called_once()

    def test_container_inspection_never_observes_host_pid(self):
        harness = object.__new__(smoke.ContainerHarness)
        harness.agent = MagicMock()
        state = {"attempts": {"test": {"execution": {"pid": 123, "start": "10"}}}}
        harness.agent.request.return_value = {"ok": True, "result": state}
        harness.owned = MagicMock()
        with patch.object(smoke.native, "process_info") as host_info:
            self.assertEqual(state, harness.inspect())
        harness.owned.observe.assert_not_called()
        host_info.assert_not_called()

    def test_incomplete_cases_cannot_report_success(self):
        self.run_main_failure(incomplete=True)

    def test_cleanup_failure_overrides_successful_cases(self):
        self.run_main_failure(incomplete=False)

    def run_main_failure(self, incomplete):
        root = self.root / "new-run"
        class FakeHarness:
            def __init__(self, run, evidence, image):
                self.evidence = evidence
            def cases(self):
                names = smoke.CASES[:1] if incomplete else smoke.CASES
                self.evidence["cases"] = [{"case": name, "ok": True} for name in names]
            def cleanup(self):
                if not incomplete:
                    raise smoke.native.CheckFailure("unconfirmed cleanup")
        base = [{"Architecture": "arm64", "Os": "linux", "Id": "pinned-image-id"}]
        with patch.object(smoke.sys, "argv", [str(SOURCE), "--run-root", str(root)]), \
                patch.object(smoke.os, "getuid", return_value=1000), \
                patch.object(smoke.os, "getgid", return_value=1000), \
                patch.object(smoke.platform, "machine", return_value="aarch64"), \
                patch.object(smoke, "docker", return_value=result(json.dumps(base))), \
                patch.object(smoke.native, "build", return_value={}), \
                patch.object(smoke, "ContainerHarness", FakeHarness):
            code = smoke.main()
        persisted = json.loads((root / "result.json").read_text())
        self.assertNotEqual(0, code)
        self.assertFalse(persisted["ok"])
        self.assertEqual("complete" if incomplete else "failed", persisted["cleanup"])


if __name__ == "__main__":
    unittest.main()
