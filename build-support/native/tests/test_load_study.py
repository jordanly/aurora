#!/usr/bin/env python3
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
import importlib.machinery
import importlib.util
import io
import contextlib
from pathlib import Path
import threading
import time
import unittest
from unittest.mock import Mock, patch


PATH = Path(__file__).parents[1] / "native-load-study"
loader = importlib.machinery.SourceFileLoader("native_load_study_tested", str(PATH))
spec = importlib.util.spec_from_loader(loader.name, loader)
study = importlib.util.module_from_spec(spec)
loader.exec_module(study)


class NativeLoadStudyTest(unittest.TestCase):
    class Cluster:
        class Native:
            @staticmethod
            def redact(value):
                return value.replace("secret", "[redacted]")
        native = Native()

    def test_timed_preserves_keyboard_interrupt_and_redacts_exception(self):
        with self.assertRaises(KeyboardInterrupt):
            study.timed(lambda: (_ for _ in ()).throw(KeyboardInterrupt()), self.Cluster)
        value = study.timed(lambda: (_ for _ in ()).throw(ValueError("secret detail")), self.Cluster)
        self.assertEqual(value["errorCategory"], "builtins.ValueError")
        self.assertNotIn("secret", value["error"])

    def test_burst_releases_all_workers_and_records_offsets(self):
        lock = threading.Lock()
        entered = 0

        def request():
            nonlocal entered
            with lock:
                entered += 1
            time.sleep(.002)

        result = study.run_burst(request, self.Cluster)
        outcomes = result["outcomes"]
        self.assertEqual(len(outcomes), 24)
        self.assertEqual(entered, 24)
        self.assertTrue(all(item["status"] == 200 for item in outcomes))
        self.assertLess(max(item["startOffsetMs"] for item in outcomes), 1000)

    def test_freeze_rejects_owned_id_mismatch_before_docker_command(self):
        class Cluster:
            class Native:
                @staticmethod
                def redact(value):
                    return value
            native = Native()
            def require(self, value, message):
                if not value:
                    raise ValueError(message)
            def docker(self, *args, **kwargs):
                raise AssertionError("Docker must not run")

        class Lab:
            data = {"containers": {"agent-a": {"id": "owned"}}}
            def inspect_container(self, role):
                return {"Id": "other", "State": {"StartedAt": "t", "Paused": False}}

        with self.assertRaises(ValueError):
            study.freeze_agent(Cluster(), Lab(), True)

    def test_freeze_uses_inspected_id_for_bounded_pause_and_unpause(self):
        class Cluster:
            class Native:
                @staticmethod
                def redact(value):
                    return value
            native = Native()
            def require(self, value, message):
                if not value:
                    raise ValueError(message)
            def __init__(self):
                self.commands = []
            def docker(self, action, ident, timeout):
                self.commands.append((action, ident, timeout))
                lab.state["Paused"] = action == "pause"

        class Lab:
            data = {"containers": {"agent-a": {"id": "owned"}}}
            state = {"Paused": False}
            def inspect_container(self, role):
                return {"Id": "owned", "State": {"StartedAt": "t", **self.state}}

        cluster = Cluster()
        lab = Lab()
        receipt = {}
        study.freeze_agent(cluster, lab, True, receipt=receipt)
        study.freeze_agent(cluster, lab, False, receipt=receipt)
        self.assertEqual([(a, i, t) for a, i, t in cluster.commands],
                         [("pause", "owned", 10), ("unpause", "owned", 10)])

    def test_freeze_rejects_changed_start_time_before_unpause(self):
        class Cluster:
            class Native:
                @staticmethod
                def redact(value):
                    return value
            native = Native()
            def require(self, value, message):
                if not value:
                    raise ValueError(message)
            def docker(self, action, ident, timeout):
                self.called = True

        class Lab:
            data = {"containers": {"agent-a": {"id": "owned"}}}
            def inspect_container(self, role):
                return {"Id": "owned", "State": {"StartedAt": "changed", "Paused": True}}

        cluster = Cluster()
        with self.assertRaises(ValueError):
            study.freeze_agent(cluster, Lab(), False, {"id": "owned", "startedAt": "original"})
        self.assertFalse(hasattr(cluster, "called"))

    def test_finish_trial_attempts_all_cleanup_phases_and_preserves_errors(self):
        class Cluster:
            class Native:
                @staticmethod
                def redact(value):
                    return value
            native = Native()

        class Lab:
            data = {"status": "running"}
            lock = object()
            def __init__(self):
                self.calls = []
            def collect(self):
                self.calls.append("collect")
                raise ValueError("collect failed")
            def down(self):
                self.calls.append("down")
                raise ValueError("down failed")
            def release(self):
                self.calls.append("release")

        lab = Lab()
        result = {"ok": False, "error": "primary workload error"}
        study.finish_trial(Cluster(), lab, result)
        self.assertEqual(lab.calls, ["collect", "down", "release"])
        self.assertEqual([item["phase"] for item in result["cleanupErrors"]], ["collect", "down"])
        self.assertEqual(result["error"], "primary workload error")

    def test_save_failure_marks_receipt_failed_without_losing_primary(self):
        cluster = Mock()
        cluster.native.redact.side_effect = lambda value: value
        cluster.save.side_effect = OSError("disk full")
        value = {"ok": True, "error": "workload error"}
        with contextlib.redirect_stderr(io.StringIO()) as log:
            self.assertFalse(study.save_receipt(cluster, Path("receipt.json"), value))
        self.assertFalse(value["ok"])
        self.assertEqual("workload error", value["error"])
        self.assertIn("disk full", log.getvalue())

    def test_ambiguous_pause_is_unfrozen_by_trial_finally(self):
        cluster = Mock()
        cluster.native.redact.side_effect = lambda value: value
        lab = cluster.Lab.return_value
        lab.data = {"run": "owned-run", "bundle": {"images": {}},
                    "containers": {"agent-a": {"id": "owned"}}, "networks": {}, "status": "stopped"}
        state = {"StartedAt": "original", "Paused": False}
        lab.inspect_container.side_effect = lambda role: {"Id": "owned", "State": dict(state)}
        def command(action, ident, timeout):
            self.assertEqual("owned", ident)
            state["Paused"] = action == "pause"
            if action == "pause":
                raise TimeoutError("pause acknowledgement lost")
        cluster.docker.side_effect = command
        benchmark = Mock()
        benchmark.host.return_value = {}
        benchmark.measured.return_value = (None, 1)
        benchmark.attempts.return_value = [{"identity": {"attempt": "a"}}, {"identity": {"attempt": "b"}}]
        benchmark.resource_sample.return_value = {}
        with patch.object(study, "require_limits", return_value={}), \
                patch.object(study, "save_receipt", return_value=True):
            result = study.trial(benchmark, cluster, Path("bundle"), Path("/tmp"), 0)
        self.assertFalse(result["ok"])
        self.assertEqual("pause acknowledgement lost", result["error"])
        self.assertTrue(result["resumedInFinally"])
        self.assertFalse(state["Paused"])
        self.assertEqual([("pause", "owned"), ("unpause", "owned")],
                         [call.args[:2] for call in cluster.docker.call_args_list])
        lab.down.assert_called_once()
        lab.release.assert_called_once()


if __name__ == "__main__":
    unittest.main()
