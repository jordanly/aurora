# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import contextlib
import io
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import ui


class UiLauncherTest(unittest.TestCase):
    def test_npm_uses_pinned_node_and_real_entrypoint(self):
        original = {'PATH': '/system/bin', 'NPM_CONFIG_CACHE': '/external'}
        command, env = ui.invocation(
            ['test', '--', '--runInBand'], {'node': Path('/pinned')}, Path('/cache'), original)
        self.assertEqual([
            '/pinned/bin/node', '/pinned/lib/node_modules/npm/bin/npm-cli.js',
            'test', '--', '--runInBand'], command)
        self.assertEqual('/pinned/bin:/system/bin', env['PATH'])
        self.assertEqual('/cache/npm', env['npm_config_cache'])
        self.assertNotIn('NPM_CONFIG_CACHE', env)
        self.assertEqual('/system/bin', original['PATH'])

    def test_unsupported_platform_fails_before_bootstrap(self):
        with patch.object(ui.platform, 'system', return_value='Darwin'), \
                patch.object(ui, 'get_tools') as get_tools, \
                contextlib.redirect_stderr(io.StringIO()) as error:
            self.assertEqual(1, ui.main(['--version']))
            get_tools.assert_not_called()
            self.assertIn('Linux ARM64', error.getvalue())

    def test_offline_missing_archive_fails_without_download(self):
        with tempfile.TemporaryDirectory() as directory, \
                patch.dict(os.environ, {'AURORA_INPLACE_UI_CACHE': directory}, clear=True), \
                patch.object(ui.platform, 'system', return_value='Linux'), \
                patch.object(ui.platform, 'machine', return_value='aarch64'), \
                patch('bootstrap.urllib.request.urlopen') as download, \
                contextlib.redirect_stderr(io.StringIO()) as error:
            self.assertEqual(1, ui.main(['--offline', '--version']))
            download.assert_not_called()
            self.assertIn('Offline tool missing: node', error.getvalue())

    def test_main_forwards_arguments_cwd_seed_and_exit_status(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            node = root / 'node'
            for name in ('bin/node', 'lib/node_modules/npm/bin/npm-cli.js'):
                target = node / name
                target.parent.mkdir(parents=True, exist_ok=True)
                target.touch()
            with patch.dict(os.environ, {
                    'AURORA_INPLACE_UI_CACHE': str(root / 'cache'),
                    'AURORA_INPLACE_UI_SEED_ARCHIVES': str(root / 'seed')}, clear=True), \
                    patch.object(ui.platform, 'system', return_value='Linux'), \
                    patch.object(ui.platform, 'machine', return_value='arm64'), \
                    patch.object(ui, 'get_tools', return_value={'node': node}) as get_tools, \
                    patch.object(ui.subprocess, 'call', return_value=7) as run:
                self.assertEqual(7, ui.main(['--offline', 'run', 'build']))
                self.assertEqual(root / 'seed', get_tools.call_args.args[3])
                self.assertTrue(get_tools.call_args.args[4])
                self.assertEqual(['--offline', 'run', 'build'], run.call_args.args[0][-3:])
                self.assertEqual(ui.ROOT / 'ui', run.call_args.kwargs['cwd'])


if __name__ == '__main__':
    unittest.main()
