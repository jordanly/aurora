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
"""Tests for the source inventory's reviewed relocation handling."""
import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest import mock


MODULE_PATH = Path(__file__).with_name('source-inventory.py')
SPEC = importlib.util.spec_from_file_location('source_inventory', MODULE_PATH)
source_inventory = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(source_inventory)


ORIGINAL = 'src/main/java/org/apache/aurora/scheduler/resources/AcceptedOffer.java'
CURRENT = 'src/main/java/org/apache/aurora/scheduler/mesos/AcceptedOffer.java'


def _git_output(_command, **_kwargs):
    if _command[1] == 'rev-parse':
        return 'test-commit'
    return ('100644 blob 0000000000000000000000000000000000000000\t'
            + ORIGINAL + '\0').encode()


class SourceInventoryRelocationTest(unittest.TestCase):
    def _inventory(self, root):
        rename = {
            ORIGINAL: {
                'originalPath': ORIGINAL,
                'currentPath': CURRENT,
                'reason': 'test relocation',
            },
        }
        with mock.patch.object(
            source_inventory.subprocess, 'check_output', side_effect=_git_output):
            return source_inventory.inventory('baseline', root, rename)

    def test_missing_original_and_target_remains_missing(self):
        with tempfile.TemporaryDirectory() as directory:
            result = self._inventory(Path(directory))

        self.assertEqual([ORIGINAL], result['missing'])
        self.assertEqual([], result['ambiguous'])
        item = result['components']['scheduler_java']['files'][0]
        self.assertEqual(CURRENT, item['currentPath'])
        self.assertEqual('target-missing', item['relocation']['status'])

    def test_relocated_target_is_listed_in_receipt(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            target = root / CURRENT
            target.parent.mkdir(parents=True)
            target.write_text('relocated source\n')
            result = self._inventory(root)

        self.assertEqual([], result['missing'])
        item = result['components']['scheduler_java']['files'][0]
        self.assertEqual(CURRENT, item['currentPath'])
        self.assertEqual(CURRENT, item['relocation']['currentPath'])
        self.assertEqual('relocated', item['relocation']['status'])

    def test_original_and_target_together_are_ambiguous(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            original = root / ORIGINAL
            target = root / CURRENT
            original.parent.mkdir(parents=True)
            target.parent.mkdir(parents=True)
            original.write_text('original source\n')
            target.write_text('relocated source\n')
            result = self._inventory(root)

        self.assertEqual([ORIGINAL], result['ambiguous'])
        self.assertEqual([], result['missing'])
        item = result['components']['scheduler_java']['files'][0]
        self.assertEqual('ambiguous', item['relocation']['status'])


if __name__ == '__main__':
    unittest.main()
