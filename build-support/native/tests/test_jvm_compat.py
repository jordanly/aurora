# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed
# under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.
import argparse
import hashlib
import importlib.machinery
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

LOADER = importlib.machinery.SourceFileLoader('jvm_compat_test', str(Path(__file__).resolve().parents[1] / 'native-jvm-compat'))
SPEC = importlib.util.spec_from_loader(LOADER.name, LOADER)
compat = importlib.util.module_from_spec(SPEC)
LOADER.exec_module(compat)


class CompatibilityIsolationTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.snapshot = self.root / 'snapshot'
        self.snapshot.mkdir()
        (self.snapshot / 'scheduler.db').write_bytes(b'immutable')
        self.args = argparse.Namespace(old_bundle=self.root / 'old', new_bundle=self.root / 'new',
            snapshot=self.snapshot, config=self.root / 'lab/config/scheduler.json',
            output=self.root / 'output', javac=self.root / 'jdk/bin/javac',
            old_java_major=8, new_java_major=25, dependency_writes=False)

    def test_undeclared_classpath_jar_is_rejected(self):
        directory = self.root / 'bundle'
        java = directory / 'jre/bin/java'
        java.parent.mkdir(parents=True)
        java.write_bytes(b'verified-runtime')
        library = directory / 'context/scheduler/lib/declared.jar'
        library.parent.mkdir(parents=True)
        library.write_bytes(b'verified-library')
        jre_hash = hashlib.sha256(b'bin/java\0' + bytes.fromhex(compat.sha(java))).hexdigest()
        manifest = {'schema': 1, 'architecture': 'arm64',
                    'artifacts': {str(library.relative_to(directory)): compat.sha(library)},
                    'hostTools': {'jre': 'jre', 'jreSha256': jre_hash}}
        (directory / 'bundle.json').write_text(json.dumps(manifest))
        compat.bundle(directory)
        (library.parent / 'undeclared.jar').write_bytes(b'could-shadow-verified-classes')
        with self.assertRaisesRegex(ValueError, 'classpath differs'):
            compat.bundle(directory)

    def test_live_journal_files_refused_before_bundle_or_output_access(self):
        (self.snapshot / 'scheduler.db-wal').write_bytes(b'live')
        with patch.object(compat, 'bundle') as bundle:
            with self.assertRaisesRegex(ValueError, 'standalone'):
                compat.check(self.args)
            bundle.assert_not_called()
        self.assertFalse(self.args.output.exists())

    def test_output_cannot_mutate_original_bundle_or_lab_tree(self):
        for original in (self.args.old_bundle, self.args.new_bundle, self.snapshot,
                         self.args.config.parent.parent):
            self.args.output = original / 'nested-result'
            with self.assertRaisesRegex(ValueError, 'separate'):
                compat.check(self.args)
            self.assertFalse(self.args.output.exists())

    def test_symlinked_snapshot_refused(self):
        link = self.root / 'linked'
        link.symlink_to(self.snapshot)
        self.args.snapshot = link
        with self.assertRaisesRegex(ValueError, 'Symlink'):
            compat.check(self.args)
        self.assertFalse(self.args.output.exists())

    def test_symlinked_or_fifo_database_refused_before_bundle_access(self):
        database = self.snapshot / 'scheduler.db'
        database.unlink()
        outside = self.root / 'outside.db'
        outside.write_bytes(b'preserved')
        database.symlink_to(outside)
        with patch.object(compat, 'bundle') as bundle:
            with self.assertRaisesRegex(ValueError, 'Symlink'):
                compat.check(self.args)
            bundle.assert_not_called()
        database.unlink()
        compat.os.mkfifo(database)
        with patch.object(compat, 'bundle') as bundle:
            with self.assertRaisesRegex(ValueError, 'regular file'):
                compat.check(self.args)
            bundle.assert_not_called()
        self.assertFalse(self.args.output.exists())
        self.assertEqual(b'preserved', outside.read_bytes())

    def test_version_parsing_and_independent_native_access(self):
        self.assertEqual(8, compat.java_major('openjdk version "1.8.0_462"'))
        self.assertEqual(25, compat.java_major('openjdk version "25.0.4.1" 2026-08-18 LTS'))
        self.assertEqual(26, compat.java_major('openjdk version "26.0.2.1" 2026-08-18'))
        self.assertEqual(compat.native_flags(25) + ['--illegal-final-field-mutation=deny'],
                         compat.native_flags(26))
        self.assertEqual([], compat.native_flags(8))
        self.assertEqual(['--enable-native-access=ALL-UNNAMED', '--illegal-native-access=deny'],
                         compat.native_flags(25))
        for invalid in ('javac 25.0.4.1', '25.0.4.1', 'openjdk version "unknown"'):
            with self.assertRaises(ValueError):
                compat.java_major(invalid)

    def test_actual_major_mismatch_fails_before_copy_and_records_error(self):
        self.args.config.parent.mkdir(parents=True)
        self.args.config.write_text('{}')
        self.args.javac.parent.mkdir(parents=True)
        self.args.javac.write_text('compiler')
        tls = self.args.config.parent.parent / 'scheduler/tls'
        tls.mkdir(parents=True)
        for name in ('keystore.p12', 'truststore.p12', 'password'):
            (tls / name).write_text('unchanged')
        def run(command, output, name):
            (output / (name + '.stderr')).write_text('openjdk version "25.0.4.1"')
        with patch.object(compat, 'bundle', return_value=(self.root / 'java', self.root / 'libs', {})), \
                patch.object(compat, 'run', side_effect=run):
            with self.assertRaisesRegex(ValueError, 'requested old/new majors'):
                compat.check(self.args)
        result = json.loads((self.args.output / 'result.json').read_text())
        self.assertFalse(result['ok'])
        self.assertIn('requested old/new majors', result['error'])
        self.assertTrue(result['originalInputsUnchanged'])
        self.assertFalse((self.args.output / 'state').exists())

    def test_existing_output_preserved(self):
        self.args.output.mkdir()
        marker = self.args.output / 'existing'
        marker.write_text('preserved')
        with self.assertRaisesRegex(ValueError, 'new directory'):
            compat.check(self.args)
        self.assertEqual('preserved', marker.read_text())


if __name__ == '__main__':
    unittest.main()
