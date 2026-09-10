# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed
# under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.
import argparse
import importlib.machinery
import importlib.util
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
            output=self.root / 'output', javac=self.root / 'jdk/bin/javac')

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

    def test_existing_output_preserved(self):
        self.args.output.mkdir()
        marker = self.args.output / 'existing'
        marker.write_text('preserved')
        with self.assertRaisesRegex(ValueError, 'new directory'):
            compat.check(self.args)
        self.assertEqual('preserved', marker.read_text())


if __name__ == '__main__':
    unittest.main()
