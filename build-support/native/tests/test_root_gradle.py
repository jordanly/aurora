# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed
# under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.
import importlib.machinery
import importlib.util
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(HERE))
loader = importlib.machinery.SourceFileLoader('root_gradle', str(HERE / 'root-gradle'))
spec = importlib.util.spec_from_loader(loader.name, loader)
launcher = importlib.util.module_from_spec(spec)
loader.exec_module(launcher)


class RootGradleTest(unittest.TestCase):
    def test_profiles_and_passthrough(self):
        for args, expected in [([], 'java25'), (['-PnativeJavaProfile=java26'], 'java26'),
                               (['-P', 'nativeJavaProfile=java26-runtime'], 'java26-runtime'),
                               (['--project-prop=nativeJavaProfile=java26'], 'java26')]:
            self.assertEqual(expected, launcher.profile_argument(args))
        tools = {role: Path('/verified tools') / role for role in ('java', 'runtime', 'gradle')}
        args = ['check', '--offline', '-PnativeBuildRoot=/output with spaces', '-PnativeJavaProfile=java26']
        command, env = launcher.invocation(args, tools, Path('/cache'), {'JAVA_HOME': '/obsolete', 'PATH': '/bin'})
        self.assertEqual(command[-len(args)-1:-1], args)
        self.assertEqual(env['JAVA_HOME'], '/verified tools/java')
        self.assertIn('-Dorg.gradle.java.installations.auto-detect=false', command)
        self.assertIn('-PnativeJavaProfile=java26', command)

    def test_toolchain_overrides_rejected(self):
        for args in [['-Dorg.gradle.java.home=/old'], ['-D', 'org.gradle.java.installations.paths=/old'],
                     ['--system-prop=org.gradle.jvmargs=-Xmx8g']]:
            with self.assertRaises(launcher.BuildError):
                launcher.validate_arguments(args)

    def test_offline_missing_and_corrupt_cache(self):
        with tempfile.TemporaryDirectory() as directory:
            cache = Path(directory)
            env = {'AURORA_NATIVE_CACHE': directory}
            with patch.dict(os.environ, env, clear=True), patch.object(launcher.platform, 'system', return_value='Linux'), \
                    patch.object(launcher.platform, 'machine', return_value='aarch64'), \
                    patch('sys.stderr', new_callable=io.StringIO) as stderr, \
                    patch.object(launcher.subprocess, 'call') as run:
                self.assertEqual(1, launcher.main(['--offline', 'build']))
                self.assertIn('Offline tool missing', stderr.getvalue())
                pins = json.loads((HERE / 'toolchains.json').read_text())
                (cache / 'archives' / pins['tools']['gradle']['file']).write_bytes(b'corrupt')
                self.assertEqual(1, launcher.main(['--offline', 'check']))
                self.assertIn('Cached tool checksum mismatch', stderr.getvalue())
                run.assert_not_called()
                self.assertEqual([], list(cache.glob('tools-*')))

    def test_selected_tools_and_exit_status(self):
        for profile in ('java25', 'java26-runtime', 'java26'):
            with tempfile.TemporaryDirectory() as directory:
                tools = {role: Path(directory) / role for role in ('java', 'runtime', 'gradle')}
                with patch.dict(os.environ, {'AURORA_NATIVE_CACHE': directory}, clear=True), \
                        patch.object(launcher.platform, 'system', return_value='Linux'), \
                        patch.object(launcher.platform, 'machine', return_value='aarch64'), \
                        patch.object(launcher, 'get_tools', return_value=tools) as get_tools, \
                        patch.object(launcher.subprocess, 'call', return_value=7) as run:
                    self.assertEqual(7, launcher.main(['--offline', '-PnativeJavaProfile=' + profile, 'build']))
                    pins, cache, work, seed, offline = get_tools.call_args.args
                    self.assertEqual({'gradle', 'java', 'runtime'}, set(pins['tools']))
                    self.assertEqual(profile, pins['java']['profile'])
                    self.assertTrue(offline)
                    self.assertFalse(work.exists())
                    self.assertEqual(launcher.ROOT, run.call_args.kwargs['cwd'])


if __name__ == '__main__':
    unittest.main()
