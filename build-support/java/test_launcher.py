#!/usr/bin/env python3
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import tempfile
import unittest
from pathlib import Path
import sys
import zipfile
import importlib.util

sys.path.insert(0, str(Path(__file__).parent))
import bootstrap
_spec = importlib.util.spec_from_file_location('root_gradle', Path(__file__).parent / 'root-gradle.py')
root_gradle = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(root_gradle)


class LauncherTest(unittest.TestCase):
    def test_arguments_are_forwarded_without_reinterpretation(self):
        cache = Path('/tmp/inplace-test-cache')
        tools = {'java': Path('/tmp/java 25'), 'gradle': Path('/tmp/gradle')}
        command, _ = root_gradle.invocation(['test name', '-Pmessage=java8/1.8'], tools, cache, {})
        root_gradle.validate_arguments(['test name', '-Pmessage=java8/1.8'])
        self.assertEqual(command[-2:], ['test name', '-Pmessage=java8/1.8'])

    def test_managed_options_are_rejected(self):
        for arguments in (
                ['-Dorg.gradle.java.home=/tmp/other'],
                ['-D', 'org.gradle.jvmargs=-Xmx8g'],
                ['--system-prop=org.gradle.java.home=/tmp/other'],
                ['--system-prop', 'org.gradle.java.installations.paths=/tmp/other']):
            with self.subTest(arguments=arguments), self.assertRaises(bootstrap.BuildError):
                root_gradle.validate_arguments(arguments)
        with self.assertRaises(bootstrap.BuildError):
            root_gradle.validate_arguments(['-PjavaVersion=8'])

    def test_archive_traversal_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive = root / 'bad.zip'
            with zipfile.ZipFile(archive, 'w') as source:
                source.writestr('../escape', 'bad')
            with self.assertRaises(bootstrap.BuildError):
                bootstrap.extract(archive, root / 'out')

    def test_corrupt_cached_archive_is_rejected_before_extraction(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archives = root / 'cache/archives'
            archives.mkdir(parents=True)
            (archives / 'java.zip').write_bytes(b'corrupt')
            pins = {'tools': {'java': {'file': 'java.zip', 'sha256': '0' * 64}}}
            with self.assertRaisesRegex(bootstrap.BuildError, 'checksum mismatch'):
                bootstrap.get_tools(pins, root / 'cache', root / 'work', offline=True)
            self.assertFalse((root / 'work').exists())

    def test_offline_missing_archive_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            pins = {'tools': {'java': {'file': 'missing.zip', 'sha256': '0', 'url': 'https://example.invalid', 'directory': 'java'}}}
            with self.assertRaises(bootstrap.BuildError):
                bootstrap.get_tools(pins, root / 'cache', root / 'work', root / 'seed', offline=True)


if __name__ == '__main__':
    unittest.main()
