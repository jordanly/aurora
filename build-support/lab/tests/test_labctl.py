# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
CLI = ROOT / "build-support/lab/labctl"


class LabCtlTest(unittest.TestCase):
    def runctl(self, *args, env=None):
        merged = os.environ.copy()
        if env:
            merged.update(env)
        return subprocess.run([str(CLI), *args], cwd=ROOT, text=True, capture_output=True, env=merged)

    def test_preflight_reports_docker_and_require_status(self):
        with tempfile.TemporaryDirectory() as td:
            missing = self.runctl("preflight", "--require-docker", env={"AURORA_LAB_DOCKER": str(Path(td) / "missing")})
            self.assertNotEqual(missing.returncode, 0)
            denied = Path(td) / "denied"
            denied.write_text("#!/bin/sh\necho 'permission denied' >&2\nexit 1\n")
            denied.chmod(0o755)
            denied_result = self.runctl("preflight", "--require-docker", env={"AURORA_LAB_DOCKER": str(denied)})
            self.assertNotEqual(denied_result.returncode, 0)
            self.assertIn("permission denied", json.loads(denied_result.stdout)["docker"]["error"])
            stub = Path(td) / "docker"
            stub.write_text("#!/bin/sh\nif [ \"$1\" = info ]; then echo '{\"ServerVersion\":\"test\",\"Architecture\":\"arm64\"}'; exit 0; fi\necho 'v5.5.1'\n")
            stub.chmod(0o755)
            available = self.runctl("preflight", "--require-docker", env={"AURORA_LAB_DOCKER": str(stub)})
            self.assertEqual(available.returncode, 0, available.stderr)
            facts = json.loads(available.stdout)
            self.assertEqual(facts["docker"]["ServerVersion"], "test")
            self.assertIn("page_size", facts)
            self.assertIn("memoryControllerAvailable", facts["cgroup"])
            self.assertFalse(facts["cgroup"]["hardMemoryEnforcementVerified"])

    def test_init_render_is_deterministic_and_absolute(self):
        with tempfile.TemporaryDirectory() as td:
            default = self.runctl("init", "--root", td)
            self.assertEqual(default.returncode, 0, default.stderr)
            self.assertRegex(json.loads(default.stdout)["run_root"], r"/\d{8}t\d{6}z-[0-9a-f]{8}$")
            one = self.runctl("init", "--root", td, "--run-id", "one", "--uid", str(os.getuid()), "--gid", str(os.getgid()))
            self.assertEqual(one.returncode, 0, one.stderr)
            run = Path(json.loads(one.stdout)["run_root"])
            first = self.runctl("render", str(run))
            self.assertEqual(first.returncode, 0, first.stderr)
            rendered = (run / "generated/compose.yaml").read_text()
            self.assertIn(str(run) + "/generated/scheduler", rendered)
            self.assertIn(f'user: "{os.getuid()}:{os.getgid()}"', rendered)
            self.assertEqual(self.runctl("render", str(run)).returncode, 0)
            self.assertEqual(rendered, (run / "generated/compose.yaml").read_text())
            self.assertEqual(self.runctl("inspect", str(run)).returncode, 0)

            special_root = Path(td) / "space $ root"
            special = self.runctl("init", "--root", str(special_root), "--run-id", "special")
            self.assertEqual(special.returncode, 0, special.stderr)
            special_run = json.loads(special.stdout)["run_root"]
            self.assertEqual(self.runctl("render", special_run).returncode, 0)
            special_compose = (Path(special_run) / "generated/compose.yaml").read_text()
            self.assertIn('source: "' + special_run.replace("$", "$$") + '/generated/scheduler"', special_compose)
            self.assertNotIn("${AURORA_LAB_ROOT}", special_compose)

    def test_collision_escape_and_foreign_destroy_are_refused(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "lab"
            self.assertEqual(self.runctl("init", "--root", str(root), "--run-id", "same").returncode, 0)
            collision = self.runctl("init", "--root", str(root), "--run-id", "same")
            self.assertNotEqual(collision.returncode, 0)
            foreign = Path(td) / "foreign"
            foreign.mkdir()
            (foreign / "important").write_text("keep")
            escaped = self.runctl("destroy", str(foreign), "--confirm")
            self.assertNotEqual(escaped.returncode, 0)
            self.assertTrue((foreign / "important").exists())
            link = Path(td) / "link"
            link.symlink_to(root, target_is_directory=True)
            self.assertNotEqual(self.runctl("inspect", str(link / "same")).returncode, 0)

            nested = root / "same" / "generated" / "agent-1"
            nested.rename(nested.with_name("agent-real"))
            nested.symlink_to(nested.with_name("agent-real"), target_is_directory=True)
            refused_dir = self.runctl("render", str(root / "same"))
            self.assertNotEqual(refused_dir.returncode, 0)

            # Exercise output-link rejection independently of generated-directory checks.
            clean = self.runctl("init", "--root", str(root), "--run-id", "clean")
            clean_run = Path(json.loads(clean.stdout)["run_root"])
            preserved = Path(td) / "preserved.yaml"
            preserved.write_text("keep")
            output = clean_run / "generated" / "compose.yaml"
            output.symlink_to(preserved)
            refused = self.runctl("render", str(clean_run))
            self.assertNotEqual(refused.returncode, 0)
            self.assertEqual(preserved.read_text(), "keep")

    @unittest.skipUnless(shutil.which("docker"), "Docker CLI is unavailable")
    def test_compose_parser_preserves_project_paths_and_topology(self):
        version = subprocess.run(["docker", "compose", "version"], capture_output=True, timeout=10)
        if version.returncode != 0:
            self.skipTest("Compose plugin is unavailable")
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "space ${AURORA_LITERAL} root"
            result = self.runctl("init", "--root", str(root), "--run-id", "parser")
            self.assertEqual(result.returncode, 0, result.stderr)
            run = Path(json.loads(result.stdout)["run_root"])
            self.assertEqual(self.runctl("render", str(run)).returncode, 0)
            expected = {"scheduler": {"control"}, "agent-1": {"worker-1"},
                        "agent-2": {"worker-2"}, "proxy-1": {"control", "worker-1"},
                        "proxy-2": {"control", "worker-2"}}
            for profile in ([], ["--profile", "test"]):
                parsed = subprocess.run(
                    ["docker", "compose", *profile, "-f", str(run / "generated/compose.yaml"),
                     "config", "--format", "json"], capture_output=True, text=True, timeout=10,
                    env={**os.environ, "AURORA_LITERAL": "must-not-expand"})
                self.assertEqual(parsed.returncode, 0, parsed.stderr)
                config = json.loads(parsed.stdout)
                self.assertEqual(config["name"], "aurora-lab-parser")
                if profile:
                    expected["test-runner"] = {"control"}
                self.assertEqual(set(config["services"]), set(expected))
                for name, service in config["services"].items():
                    self.assertEqual(set(service["networks"]), expected[name])
                    self.assertTrue(service["read_only"])
                    self.assertTrue(service["init"])
                    self.assertEqual(service["cap_drop"], ["ALL"])
                    for volume in service["volumes"]:
                        if volume["type"] == "bind":
                            # Compose config re-escapes dollars when serializing its model.
                            source = volume["source"].replace("$$", "$")
                            self.assertTrue(source.startswith(str(run) + "/"), source)
                            self.assertFalse(volume["bind"]["create_host_path"])
                    if name != "scheduler":
                        self.assertFalse(service.get("ports"))
                self.assertEqual(config["services"]["scheduler"]["ports"][0]["host_ip"], "127.0.0.1")
                self.assertTrue(all(network["internal"] for network in config["networks"].values()))

    def test_malformed_manifest_and_marker_are_refused(self):
        with tempfile.TemporaryDirectory() as td:
            result = self.runctl("init", "--root", td, "--run-id", "shape")
            run = Path(json.loads(result.stdout)["run_root"])
            (run / "manifest.json").write_text("[]")
            self.assertNotEqual(self.runctl("inspect", str(run)).returncode, 0)
            for field, value in (("uid", True), ("gid", -1)):
                valid = self.runctl("init", "--root", td, "--run-id", "valid-" + field)
                altered = Path(json.loads(valid.stdout)["run_root"])
                manifest = json.loads((altered / "manifest.json").read_text())
                manifest[field] = value
                (altered / "manifest.json").write_text(json.dumps(manifest))
                self.assertNotEqual(self.runctl("inspect", str(altered)).returncode, 0)
            result = self.runctl("init", "--root", td, "--run-id", "marker")
            marked = Path(json.loads(result.stdout)["run_root"])
            (marked / ".aurora-lab-run").write_text("wrong\n")
            self.assertNotEqual(self.runctl("inspect", str(marked)).returncode, 0)

    def test_invalid_run_ids_paths_and_ports_are_refused(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "lab"
            for run_id in ("Bad", "../escape", "a" * 49, "bad\nname"):
                result = self.runctl("init", "--root", str(root), "--run-id", run_id)
                self.assertNotEqual(result.returncode, 0)
            result = self.runctl("init", "--root", str(root), "--run-id", "safe")
            run = json.loads(result.stdout)["run_root"]
            for port in ("0", "65536"):
                self.assertNotEqual(self.runctl("render", run, "--http-port", port).returncode, 0)

    def test_destroy_requires_docker_before_any_removal(self):
        with tempfile.TemporaryDirectory() as td:
            result = self.runctl("init", "--root", td, "--run-id", "keep")
            run = Path(json.loads(result.stdout)["run_root"])
            destroyed = self.runctl("destroy", str(run), "--confirm", env={"AURORA_LAB_DOCKER": str(Path(td) / "missing")})
            # On a daemon-less host this is the safety refusal; if Docker is available,
            # the intentionally unimplemented skeleton still leaves the run untouched.
            self.assertTrue(run.exists())
            self.assertNotEqual(destroyed.returncode, 0)


if __name__ == "__main__":
    unittest.main()
