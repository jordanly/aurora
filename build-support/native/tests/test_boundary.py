# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import hashlib
import importlib.machinery
import importlib.util
import json
from pathlib import Path
import struct
import tempfile
import unittest
from unittest import mock
import zipfile

CHECKER = Path(__file__).resolve().parents[1] / "verify-boundary"
LOADER = importlib.machinery.SourceFileLoader("native_boundary", str(CHECKER))
SPEC = importlib.util.spec_from_loader(LOADER.name, LOADER)
boundary = importlib.util.module_from_spec(SPEC)
LOADER.exec_module(boundary)


def class_file(identity, reference=None, major=52):
    constants = []
    raw = identity.encode("ascii")
    constants.append(b"\x01" + struct.pack(">H", len(raw)) + raw)
    constants.append(b"\x07\x00\x01")
    if reference:
        raw = reference.encode("ascii")
        constants.append(b"\x01" + struct.pack(">H", len(raw)) + raw)
    return (b"\xca\xfe\xba\xbe" + struct.pack(">HHH", 0, major, len(constants) + 1)
            + b"".join(constants) + struct.pack(">HH", 0x21, 2))


def jar(path, entries):
    with zipfile.ZipFile(path, "w") as archive:
        for name, data in entries.items():
            archive.writestr(name, data)


class ManifestTests(unittest.TestCase):
    def test_complete_production_allowlist_matches_reviewed_protocol_manifest(self):
        root = CHECKER.parents[2]
        expected = {}
        for line in (root / "protocol/java/runtime-dependencies.sha256").read_text().splitlines():
            digest, name = line.split()
            expected[name] = digest
        expected[boundary.SQLITE] = "bcb1f51e36f940867e83342f9efbf5968ac44a6bef4d397bb4af7b17b45cd2fb"
        self.assertEqual(expected, boundary.EXTERNAL)
        self.assertEqual(7, len(boundary.EXTERNAL))


class BoundaryTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.lib = self.root / "lib"
        self.lib.mkdir()
        # Synthetic reviewed artifacts keep unit tests offline and independent
        # of any checkout cache. Production hashes are exercised by Gradle's
        # gate against the real runtime; the allowlist algorithm is unchanged.
        expected = {}
        for name in boundary.EXTERNAL:
            entries = {"META-INF/MANIFEST.MF": "Manifest-Version: 1.0\n"}
            if name == boundary.SQLITE:
                entries["org/sqlite/native/Linux/aarch64/libsqlitejdbc.so"] = b"\x7fELF"
            jar(self.lib / name, entries)
            expected[name] = hashlib.sha256((self.lib / name).read_bytes()).hexdigest()
        self.manifest = mock.patch.dict(boundary.EXTERNAL, expected, clear=True)
        self.manifest.start()
        self.addCleanup(self.manifest.stop)
        self.main = "org/apache/aurora/nativescheduler/NativeSchedulerMain"
        self.sql = "org/apache/aurora/scheduler/storage/sql/NativeSqlStore"
        self.validator = "org/apache/aurora/nativeprotocol/ProtocolValidator"
        self.scheduler_entries = {self.main + ".class": class_file(self.main),
                                  self.sql + ".class": class_file(self.sql)}
        jar(self.lib / "aurora-native-scheduler.jar", self.scheduler_entries)
        jar(self.lib / "protocol.jar", {self.validator + ".class": class_file(self.validator),
                                        "schema.json": "{}"})

    def test_exact_runtime_and_pinned_sqlite_jni_are_permitted(self):
        report = boundary.verify(self.lib)
        self.assertTrue(report["ok"])
        self.assertEqual(9, len(report["jars"]))
        sqlite = next(item for item in report["jars"] if item["name"] == boundary.SQLITE)
        self.assertEqual(1, sqlite["nativeLibraryCount"])
        self.assertEqual("org.xerial:sqlite-jdbc:3.53.4.0", sqlite["coordinate"])

    def test_tampered_external_hash_rejects(self):
        with (self.lib / boundary.SQLITE).open("ab") as output:
            output.write(b"tampering")
        with self.assertRaisesRegex(boundary.BoundaryError, "SHA-256 mismatch"):
            boundary.verify(self.lib)

    def test_renamed_mesos_jar_is_still_unexpected(self):
        name = "org/apache/mesos/Executor"
        jar(self.lib / "harmless.jar", {name + ".class": class_file(name)})
        with self.assertRaisesRegex(boundary.BoundaryError, "unexpected runtime JAR"):
            boundary.verify(self.lib)

    def test_additional_unexpected_jar_rejects(self):
        jar(self.lib / "junit.jar", {})
        with self.assertRaisesRegex(boundary.BoundaryError, "unexpected runtime JAR"):
            boundary.verify(self.lib)

    def test_renamed_class_and_reference_cannot_hide_in_own_jar(self):
        for reference in ("Lorg/apache/mesos/Executor;", "org.apache.zookeeper.ZooKeeper",
                          "org/apache/curator/CuratorFramework", "apache.aurora.executor.Worker",
                          "Lorg/apache/aurora/scheduler/legacy/Task;"):
            with self.subTest(reference=reference):
                changed = dict(self.scheduler_entries)
                changed[self.main + ".class"] = class_file(self.main, reference)
                jar(self.lib / "aurora-native-scheduler.jar", changed)
                with self.assertRaisesRegex(boundary.BoundaryError, "forbidden class reference"):
                    boundary.verify(self.lib)
        changed[self.main + ".class"] = class_file("org/apache/mesos/Executor")
        jar(self.lib / "aurora-native-scheduler.jar", changed)
        with self.assertRaises(boundary.BoundaryError):
            boundary.verify(self.lib)

    def test_foreign_own_classes_and_qualification_tool_reject(self):
        for name in ("com/foreign/Extra", "org/apache/aurora/scheduler/storage/sql/NativeStoreTool",
                     "org/apache/aurora/nativescheduler/Unexpected"):
            with self.subTest(name=name):
                changed = dict(self.scheduler_entries)
                changed[name + ".class"] = class_file(name)
                jar(self.lib / "aurora-native-scheduler.jar", changed)
                with self.assertRaises(boundary.BoundaryError):
                    boundary.verify(self.lib)

    def test_python_worker_nested_jar_and_foreign_resource_reject(self):
        for name in ("worker.py", "worker.pyc", "embedded.jar", "unreviewed.conf"):
            with self.subTest(name=name):
                changed = dict(self.scheduler_entries)
                changed[name] = b"unreviewed"
                jar(self.lib / "aurora-native-scheduler.jar", changed)
                with self.assertRaises(boundary.BoundaryError):
                    boundary.verify(self.lib)

    def test_protocol_profile_and_missing_schema(self):
        (self.lib / "aurora-native-scheduler.jar").unlink()
        (self.lib / boundary.SQLITE).unlink()
        self.assertEqual(7, len(boundary.verify(self.lib, "protocol")["jars"]))
        jar(self.lib / "protocol.jar", {self.validator + ".class": class_file(self.validator)})
        with self.assertRaisesRegex(boundary.BoundaryError, "schema missing"):
            boundary.verify(self.lib, "protocol")

    def test_java_version_and_truncated_class_reject(self):
        for value in (class_file(self.main, major=53), b"\xca\xfe\xba\xbe"):
            changed = dict(self.scheduler_entries)
            changed[self.main + ".class"] = value
            jar(self.lib / "aurora-native-scheduler.jar", changed)
            with self.assertRaises(boundary.BoundaryError):
                boundary.verify(self.lib)

    def test_symlinks_and_nonjar_files_reject(self):
        external = self.lib / boundary.SQLITE
        external.rename(self.root / boundary.SQLITE)
        external.symlink_to(self.root / boundary.SQLITE)
        with self.assertRaisesRegex(boundary.BoundaryError, "symlink"):
            boundary.verify(self.lib)

    def test_failure_replaces_stale_success_report_and_returns_nonzero(self):
        report = self.root / "report.json"
        report.write_text('{"ok":true}')
        (self.lib / "extra.txt").write_text("unexpected")
        with mock.patch("sys.argv", [str(CHECKER), "--lib-dir", str(self.lib), "--report", str(report)]):
            self.assertEqual(1, boundary.main())
        self.assertFalse(json.loads(report.read_text())["ok"])


if __name__ == "__main__":
    unittest.main()
