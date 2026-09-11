#!/usr/bin/env python3
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Run Aurora's original Gradle graph with the pinned Java 25 toolchain."""
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

def validate_arguments(arguments):
    protected = ('org.gradle.java.home', 'org.gradle.java.installations.', 'org.gradle.jvmargs')
    for index, argument in enumerate(arguments):
        value = arguments[index + 1] if argument in ('-D', '--system-prop', '-P', '--project-prop') and index + 1 < len(arguments) else argument
        value = value.removeprefix('--system-prop=').removeprefix('-D')
        value = value.removeprefix('--project-prop=').removeprefix('-P')
        require(not value.startswith(protected), 'Toolchain/JVM options are managed by the pinned launcher')
        if value.startswith(('javaVersion=', 'sourceCompatibility=', 'targetCompatibility=', 'release=')):
            require(value.rsplit('=', 1)[1] not in ('8', '1.8'),
                    'Java 8 is rejected; this build requires Java 25')

def invocation(arguments, tools, cache, environment):
    env = dict(environment)
    env.update(JAVA_HOME=str(tools['java']), GRADLE_USER_HOME=str(cache / 'gradle'))
    env['PATH'] = str(tools['java'] / 'bin') + os.pathsep + env.get('PATH', '')
    command = [str(tools['gradle'] / 'bin/gradle'), '--no-daemon', '--max-workers=2',
               '--warning-mode=fail', '-Dorg.gradle.java.installations.auto-detect=false',
               '-Dorg.gradle.java.installations.auto-download=false',
               '-Dorg.gradle.java.installations.paths=' + str(tools['java']),
               '-Dorg.gradle.java.home=' + str(tools['java']),
               '-Dorg.gradle.jvmargs=-Xmx256m -XX:MaxMetaspaceSize=192m -XX:ActiveProcessorCount=2 '
               '-Dfile.encoding=UTF-8 --enable-native-access=ALL-UNNAMED',
               '--project-cache-dir', str(cache / 'project'),
               '-PauroraBuildRoot=' + str(cache / 'build'), *arguments]
    return command, env

def main(arguments=None):
    arguments = list(sys.argv[1:] if arguments is None else arguments)
    try:
        require(platform.system() == 'Linux' and platform.machine().lower() in ('aarch64', 'arm64'),
                'The in-place launcher currently supports Linux ARM64 only')
        validate_arguments(arguments)
        pins = json.loads((HERE / 'toolchains.json').read_text())
        cache = Path(os.environ.get('AURORA_INPLACE_CACHE', str(ROOT / '.cache/inplace-build'))).absolute()
        seed_value = os.environ.get('AURORA_INPLACE_SEED_ARCHIVES')
        seed = Path(seed_value).absolute() if seed_value else None
        real_path(cache)
        cache.mkdir(parents=True, exist_ok=True, mode=0o700)
        # Tighten an existing user-owned cache created by a permissive umask.
        if cache.stat().st_uid == os.getuid():
            cache.chmod(0o700)
        require(cache.stat().st_uid == os.getuid() and cache.stat().st_mode & 0o022 == 0,
                'Build cache must be owned by this user and not group/world writable')
        import fcntl
        lock_path = cache / 'build.lock'
        real_path(lock_path)
        with lock_path.open('a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            with tempfile.TemporaryDirectory(prefix='tools-', dir=cache) as work:
                tools = get_tools(pins, cache, Path(work), seed, '--offline' in arguments)
                command, env = invocation(arguments, tools, cache, os.environ)
                return subprocess.call(command, cwd=ROOT, env=env)
    except (BuildError, OSError, ValueError, KeyError) as error:
        print('In-place Gradle build failed: ' + str(error), file=sys.stderr)
        return 1

if __name__ == '__main__':
    sys.exit(main())
