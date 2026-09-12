#!/usr/bin/env python3
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Run npm arguments in the original UI with checksum-pinned Node.

AURORA_INPLACE_UI_CACHE defaults to .cache/inplace-ui in the repository.
AURORA_INPLACE_UI_SEED_ARCHIVES supplies a directory of offline tool archives;
AURORA_INPLACE_SEED_ARCHIVES is also accepted as the shared launcher fallback.
Pass --offline to prevent tool downloads and forward npm's offline option.
The archive is checked and freshly extracted for every command; npm's downloaded
packages remain cached. The UI lock is independent of the Gradle launcher lock.
"""
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import tempfile

from bootstrap import BuildError, get_tools, real_path, require

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent


def invocation(arguments, tools, cache, environment):
    node = tools['node']
    env = dict(environment)
    env['PATH'] = str(node / 'bin') + os.pathsep + env.get('PATH', '')
    env.pop('NPM_CONFIG_CACHE', None)
    env['npm_config_cache'] = str(cache / 'npm')
    env['npm_config_update_notifier'] = 'false'
    # bootstrap materializes archive symlinks as files. Calling bin/npm would
    # resolve its relative imports from the wrong directory after extraction.
    command = [str(node / 'bin/node'),
               str(node / 'lib/node_modules/npm/bin/npm-cli.js'), *arguments]
    return command, env


def main(arguments=None):
    arguments = list(sys.argv[1:] if arguments is None else arguments)
    try:
        require(platform.system() == 'Linux' and
                platform.machine().lower() in ('aarch64', 'arm64'),
                'The UI launcher currently supports Linux ARM64 only')
        pins = json.loads((HERE / 'ui-tools.json').read_text())
        cache = Path(os.environ.get(
            'AURORA_INPLACE_UI_CACHE', str(ROOT / '.cache/inplace-ui'))).absolute()
        seed_value = os.environ.get('AURORA_INPLACE_UI_SEED_ARCHIVES') or os.environ.get(
            'AURORA_INPLACE_SEED_ARCHIVES')
        seed = Path(seed_value).absolute() if seed_value else None
        real_path(cache)
        cache.mkdir(parents=True, exist_ok=True, mode=0o700)
        if cache.stat().st_uid == os.getuid():
            cache.chmod(0o700)
        require(cache.stat().st_uid == os.getuid() and cache.stat().st_mode & 0o022 == 0,
                'UI cache must be owned by this user and not group/world writable')
        import fcntl
        lock_path = cache / 'ui.lock'
        real_path(lock_path)
        with lock_path.open('a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            with tempfile.TemporaryDirectory(prefix='tools-', dir=cache) as work:
                tools = get_tools(pins, cache, Path(work), seed, '--offline' in arguments)
                command, env = invocation(arguments, tools, cache, os.environ)
                require(Path(command[0]).is_file() and Path(command[1]).is_file(),
                        'Pinned Node archive is missing node or the npm entrypoint')
                return subprocess.call(command, cwd=ROOT / 'ui', env=env)
    except (BuildError, OSError, ValueError, KeyError) as error:
        print('In-place UI build failed: ' + str(error), file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
