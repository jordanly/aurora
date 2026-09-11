# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed
# under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.

import hashlib
import importlib.machinery
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import unittest
from unittest import mock

HERE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(HERE))
try:
    import bootstrap
    loader = importlib.machinery.SourceFileLoader("tested_native_build", str(HERE / "native-build"))
    spec = importlib.util.spec_from_loader(loader.name, loader)
    build = importlib.util.module_from_spec(spec)
    loader.exec_module(build)
finally:
    sys.path.pop(0)


def archive(path, entries):
    with tarfile.open(path, "w:gz") as stream:
        for name, kind, content in entries:
            item = tarfile.TarInfo(name)
            item.mode = 0o755
            if kind == "file":
                item.size = len(content)
                stream.addfile(item, io.BytesIO(content))
            else:
                item.type = tarfile.SYMTYPE if kind == "symlink" else tarfile.LNKTYPE
                item.linkname = content
                stream.addfile(item)


class BootstrapTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.archive = self.root / "tool.tar.gz"
        self.cache = self.root / "cache"
        self.work = self.root / "work"
        self.seed = self.root / "seed"
        for directory in (self.cache, self.work, self.seed):
            directory.mkdir()

    def pin(self, digest):
        return {"tools": {"java": {"file": "tool.tar.gz", "sha256": digest,
                                   "url": "https://example.invalid/tool.tar.gz", "directory": "jdk"}}}

    def test_jdk_directory_and_file_symlinks_are_flattened(self):
        # These are the link shapes in the actual pinned Temurin8 archive.
        archive(self.archive, [
            ("jdk/man/ja_JP.UTF-8/man1/java.1", "file", b"manual"),
            ("jdk/jre/lib/aarch64/libjsig.so", "file", b"library"),
            ("jdk/man/ja", "symlink", "ja_JP.UTF-8"),
            ("jdk/jre/lib/aarch64/server/libjsig.so", "symlink", "../libjsig.so"),
        ])
        destination = self.work / "java"
        bootstrap.extract(self.archive, destination)
        self.assertEqual(b"manual", (destination / "jdk/man/ja/man1/java.1").read_bytes())
        self.assertEqual(b"library", (destination / "jdk/jre/lib/aarch64/server/libjsig.so").read_bytes())
        self.assertFalse(any(path.is_symlink() for path in destination.rglob("*")))

    def test_escaping_absolute_and_cyclic_links_reject(self):
        cases = [
            [("jdk/link", "symlink", "../../outside")],
            [("jdk/link", "symlink", "/etc/passwd")],
            [("jdk/file", "file", b"x"), ("jdk/loop", "symlink", ".")],
            [("jdk/a", "symlink", "b"), ("jdk/b", "symlink", "a")],
        ]
        for index, entries in enumerate(cases):
            with self.subTest(entries=entries):
                archive(self.archive, entries)
                with self.assertRaises((bootstrap.BuildError, OSError)):
                    bootstrap.extract(self.archive, self.work / str(index))

    def test_archive_duplicate_and_traversal_reject(self):
        for index, entries in enumerate([
                [("jdk/file", "file", b"one"), ("jdk/file", "file", b"two")],
                [("../outside", "file", b"escape")],
                [("/absolute", "file", b"escape")]]):
            with self.subTest(entries=entries):
                archive(self.archive, entries)
                with self.assertRaises(bootstrap.BuildError):
                    bootstrap.extract(self.archive, self.work / str(index))
        self.assertFalse((self.root / "outside").exists())

    def test_bad_seed_checksum_never_extracts_or_publishes_tool(self):
        (self.seed / "tool.tar.gz").write_bytes(b"untrusted seed")
        with mock.patch.object(bootstrap, "extract") as extract:
            with self.assertRaisesRegex(bootstrap.BuildError, "Seed tool checksum"):
                bootstrap.get_tools(self.pin("0" * 64), self.cache, self.work, self.seed, True)
            extract.assert_not_called()
        self.assertFalse((self.cache / "archives/tool.tar.gz").exists())
        self.assertEqual([], list(self.work.iterdir()))

    def test_corrupted_cached_archive_rejects_before_extract(self):
        (self.cache / "archives").mkdir()
        (self.cache / "archives/tool.tar.gz").write_bytes(b"changed cached archive")
        with mock.patch.object(bootstrap, "extract") as extract:
            with self.assertRaisesRegex(bootstrap.BuildError, "Cached tool checksum"):
                bootstrap.get_tools(self.pin("0" * 64), self.cache, self.work, offline=True)
            extract.assert_not_called()

    def test_verified_seed_is_reextracted_and_offline_missing_rejects(self):
        archive(self.seed / "tool.tar.gz", [("jdk/bin/java", "file", b"verified executable")])
        digest = bootstrap.sha(self.seed / "tool.tar.gz")
        result = bootstrap.get_tools(self.pin(digest), self.cache, self.work, self.seed, True)
        self.assertEqual(b"verified executable", (result["java"] / "bin/java").read_bytes())
        (result["java"] / "bin/java").write_bytes(b"modified extracted tool")
        other_work = self.root / "next-work"
        other_work.mkdir()
        result = bootstrap.get_tools(self.pin(digest), self.cache, other_work, offline=True)
        self.assertEqual(b"verified executable", (result["java"] / "bin/java").read_bytes())
        (self.cache / "archives/tool.tar.gz").unlink()
        with self.assertRaisesRegex(bootstrap.BuildError, "Offline tool missing"):
            bootstrap.get_tools(self.pin(digest), self.cache, self.work, offline=True)


class ProbeTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.identity = "a" * 64
        self.image = "sha256:" + "b" * 64
        self.commands = []
        self.failure = None
        self.known_id = True
        self.mismatch = False
        self.remove_failure = False
        self.token = None
        run = mock.patch.object(build, "run", side_effect=self.fake_run)
        inspect = mock.patch.object(build, "inspect", side_effect=self.inspect)
        run.start()
        inspect.start()
        self.addCleanup(run.stop)
        self.addCleanup(inspect.stop)

    def fake_run(self, args, **kwargs):
        args = list(map(str, args))
        self.commands.append(args)
        if args[:2] == ["docker", "create"]:
            self.token = args[args.index("--label") + 1].split("=", 1)[1]
            if self.known_id:
                Path(args[args.index("--cidfile") + 1]).write_text(self.identity + "\n")
            if self.failure:
                raise self.failure
            return self.identity
        if args[:3] == ["docker", "container", "rm"]:
            if self.remove_failure:
                raise bootstrap.BuildError("injected removal failure")
            return self.identity
        raise AssertionError(args)

    def inspect(self, kind, identity):
        self.assertEqual("container", kind)
        self.assertEqual(self.identity, identity)
        return {"Id": self.identity, "Image": self.image, "Config": {"Labels": {
            "com.aurora.native.probe": "foreign" if self.mismatch else self.token}}}

    def record(self):
        return json.loads((self.root / "probe-test.json").read_text())

    def removals(self):
        return [args for args in self.commands if args[:3] == ["docker", "container", "rm"]]

    def test_success_removes_only_verified_created_id(self):
        probe = build.Probe(self.image, self.root, "test")
        self.assertEqual("created", self.record()["status"])
        self.assertEqual([], self.removals())
        probe.close()
        self.assertEqual([["docker", "container", "rm", "-f", self.identity]], self.removals())
        self.assertEqual("removed", self.record()["status"])
        self.assertTrue(self.root.exists())

    def test_timeout_after_known_creation_cleans_verified_id(self):
        self.failure = subprocess.TimeoutExpired(["docker", "create"], 60)
        with self.assertRaises(subprocess.TimeoutExpired):
            build.Probe(self.image, self.root, "test")
        self.assertEqual(1, len(self.removals()))
        self.assertEqual("removed", self.record()["status"])

    def test_timeout_without_id_preserves_record_without_removal(self):
        self.failure = subprocess.TimeoutExpired(["docker", "create"], 60)
        self.known_id = False
        with self.assertRaises(subprocess.TimeoutExpired):
            build.Probe(self.image, self.root, "test")
        self.assertEqual([], self.removals())
        self.assertEqual("creating", self.record()["status"])

    def test_ownership_mismatch_preserves_container_and_record(self):
        self.mismatch = True
        with self.assertRaisesRegex(bootstrap.BuildError, "ownership mismatch"):
            build.Probe(self.image, self.root, "test")
        self.assertEqual([], self.removals())
        self.assertEqual("created", self.record()["status"])

    def test_removal_failure_keeps_creation_evidence(self):
        self.failure = subprocess.TimeoutExpired(["docker", "create"], 60)
        self.remove_failure = True
        with self.assertRaisesRegex(bootstrap.BuildError, "removal failure"):
            build.Probe(self.image, self.root, "test")
        self.assertEqual("created", self.record()["status"])
        self.assertEqual(self.identity, self.record()["id"])

    def test_existing_record_collision_never_calls_docker(self):
        path = self.root / "probe-test.json"
        path.write_text("prior ownership evidence")
        with self.assertRaisesRegex(bootstrap.BuildError, "collision"):
            build.Probe(self.image, self.root, "test")
        self.assertEqual([], self.commands)
        self.assertEqual("prior ownership evidence", path.read_text())


class JavaProfileTest(unittest.TestCase):
    def test_reviewed_profiles_select_exact_compiler_runtime_and_target(self):
        pins = json.loads((HERE / 'toolchains.json').read_text())
        for profile, compiler, runtime, bytecode in (
                ('java25', 25, 25, 69), ('java26-runtime', 25, 26, 69), ('java26', 26, 26, 70)):
            selected = bootstrap.select_java_profile(pins, profile)
            self.assertEqual(compiler, selected['java']['compilerVersionMajor'])
            self.assertEqual(runtime, selected['java']['versionMajor'])
            self.assertEqual(bytecode, selected['java']['bytecodeMajor'])
            self.assertEqual({'go', 'gradle', 'java', 'runtime'}, set(selected['tools']))
            self.assertIn('OpenJDK' + str(compiler) + 'U-jdk_', selected['tools']['java']['file'])
            self.assertIn('OpenJDK' + str(runtime) + 'U-jre_', selected['tools']['runtime']['file'])
            if runtime == 26:
                self.assertEqual('deny', selected['java']['illegalFinalFieldMutation'])
        self.assertEqual(25, bootstrap.select_java_profile(pins)['java']['versionMajor'])
        self.assertEqual(25, pins['java']['versionMajor'])

    def test_unreviewed_profile_cannot_select_arbitrary_java(self):
        with self.assertRaises(bootstrap.BuildError):
            bootstrap.select_java_profile({}, 'java27')


if __name__ == "__main__":
    unittest.main()
