#!/usr/bin/env python3
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Verify installed Aurora launchers, runtime jars and Java/UI artifacts without starting services."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import zipfile


class VerificationError(Exception):
    pass


def require(condition, message):
    if not condition:
        raise VerificationError(message)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def run(command, environment, directory, timeout):
    completed = subprocess.run(command, cwd=directory, env=environment,
                               stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                               text=True, errors='replace', timeout=timeout, check=False)
    return {'command': [str(arg) for arg in command], 'exitCode': completed.returncode,
            'output': completed.stdout,
            'outputSha256': digest(completed.stdout.encode('utf-8'))}


def verify_help(result, main_class, required_options):
    require(result['exitCode'] == 1,
            main_class + ': expected usage exit 1, got ' + str(result['exitCode']))
    output = result['output']
    require(re.search(r'Usage:\s*' + re.escape(main_class), output) is not None,
            main_class + ': expected original usage banner')
    for option in required_options:
        require(option in output, main_class + ': usage missing ' + option)
    for failure in ('NoClassDefFoundError', 'ClassNotFoundException',
                    'Could not find or load main class', 'UnsatisfiedLinkError', 'Recovering from'):
        require(failure not in output, main_class + ': unexpected startup failure/action: ' + failure)
    result['expectedExitCode'] = 1
    result['usageVerified'] = True


def verify_classpath(script_text, distribution):
    references = re.findall(r'\$APP_HOME/lib/([^:\s"\n]+)', script_text)
    require(references, 'Installed launcher has no explicit runtime classpath')
    require(len(references) == len(set(references)), 'Installed launcher has duplicate classpath entries')
    for entry in references:
        require((distribution / 'lib' / entry).is_file(),
                'Installed launcher references missing runtime jar: ' + entry)
        require(entry.endswith('.jar'), 'Installed runtime classpath must use packaged jars: ' + entry)
    jars = {path.name for path in (distribution / 'lib').glob('*.jar')}
    require(set(references) == jars, 'Installed runtime jars and launcher classpath differ')
    loose_classes = list((distribution / 'lib').rglob('*.class'))
    require(not loose_classes, 'Distribution contains unpackaged generated classes')
    return references


def inspect_jars(distribution):
    jars = sorted((distribution / 'lib').glob('*.jar'))
    require(jars, 'No installed runtime jars')
    members, artifacts = {}, {}
    for path in jars:
        with zipfile.ZipFile(path) as archive:
            names = archive.namelist()
            require(len(names) == len(set(names)), 'Duplicate jar entries: ' + path.name)
            members[path] = set(names)
        data = path.read_bytes()
        artifacts[path.name] = {'sha256': digest(data), 'size': len(data)}

    def containing(entry):
        found = [path for path, names in members.items() if entry in names]
        require(len(found) == 1, 'Expected exactly one runtime jar containing ' + entry)
        return found[0]

    scheduler = containing('org/apache/aurora/scheduler/app/SchedulerMain.class')
    api = containing('org/apache/aurora/gen/ReadOnlyScheduler.class')
    commons = containing('org/apache/aurora/common/application/Lifecycle.class')
    required = {
        scheduler: [
            'org/apache/aurora/scheduler/storage/durability/RecoveryTool.class',
            'scheduler/assets/scheduler/index.html',
            'scheduler/assets/js/bundle.js',
            'scheduler/assets/js/bundle.js.map',
            'scheduler/assets/js/thrift.js',
            'scheduler/assets/bower_components/jquery/dist/jquery.min.js'],
        api: [
            'org/apache/aurora/gen/AuroraAdmin.class',
            'org/apache/aurora/scheduler/storage/entities/IScheduledTask.class',
            'org/apache/aurora/scheduler/gen/client/ReadOnlyScheduler.js',
            'org/apache/aurora/scheduler/gen/client/AuroraAdmin.js',
            'org/apache/aurora/scheduler/gen/client/api_types.js',
            'org/apache/aurora/scheduler/gen/client/api.html']}
    for path, entries in required.items():
        with zipfile.ZipFile(path) as archive:
            for entry in entries:
                require(entry in members[path], path.name + ': missing ' + entry)
                require(archive.getinfo(entry).file_size > 0, path.name + ': empty ' + entry)
        artifacts[path.name]['requiredEntries'] = entries

    for path in (scheduler, api, commons):
        with zipfile.ZipFile(path) as archive:
            classes = [name for name in members[path] if name.endswith('.class')]
            require(classes, 'No Aurora classes in ' + path.name)
            for name in classes:
                data = archive.read(name)
                require(len(data) >= 8 and data[:4] == bytes.fromhex('cafebabe'),
                        'Invalid class header: ' + name)
                major = int.from_bytes(data[6:8], 'big')
                require(major == 69, f'{path.name}: {name} has bytecode {major}, expected 69')
            artifacts[path.name].update(auroraClassCount=len(classes), bytecodeMajor=69)

    dependencies = {}
    for entry in (
            'org/sqlite/JDBC.class', 'com/google/inject/Guice.class',
            'com/google/common/collect/ImmutableList.class', 'org/apache/thrift/TBase.class',
            'org/apache/mesos/v1/Protos.class', 'org/apache/zookeeper/ZooKeeper.class',
            'org/eclipse/jetty/server/Server.class', 'com/google/gson/Gson.class',
            'javax/xml/bind/annotation/XmlElement.class'):
        dependencies[entry] = containing(entry).name
    return artifacts, dependencies


def verify(distribution, receipt, timeout):
    require(distribution.is_dir(), 'Installed distribution does not exist: ' + str(distribution))
    java_home_value = os.environ.get('JAVA_HOME')
    require(java_home_value, 'JAVA_HOME must select the pinned Java runtime')
    java_home = Path(java_home_value).absolute()
    java = java_home / 'bin/java'
    require(java.is_file() and os.access(java, os.X_OK), 'JAVA_HOME does not contain executable java')
    environment = dict(os.environ)
    # Check the installed default launchers without caller-supplied JVM or application overrides.
    for key in ('JAVA_OPTS', 'JAVA_TOOL_OPTIONS', '_JAVA_OPTIONS', 'JDK_JAVA_OPTIONS',
                'AURORA_SCHEDULER_OPTS', 'RECOVERY_TOOL_OPTS', 'CLASSPATH'):
        environment.pop(key, None)
    environment['JAVA_HOME'] = str(java_home)
    environment['PATH'] = str(java_home / 'bin') + os.pathsep + environment.get('PATH', '')
    with tempfile.TemporaryDirectory(prefix='aurora-distribution-check-') as directory:
        runtime = run([str(java), '--version'], environment, directory, timeout)
        receipt['java'] = runtime
        require(runtime['exitCode'] == 0, 'Java runtime version command failed')
        match = re.search(r'^(?:openjdk|java) (\d+)', runtime['output'], re.MULTILINE)
        require(match and int(match.group(1)) >= 25, 'Installed launcher requires Java 25 or newer')
        runtime['javaHome'] = str(java_home)
        artifacts, dependencies = inspect_jars(distribution)
        receipt.update(artifacts=artifacts, runtimeDependencies=dependencies, launchers={})
        for name, main_class, arguments, options in (
                ('aurora-scheduler', 'org.apache.aurora.scheduler.app.SchedulerMain',
                 ['-help'], ['-cluster_name', '-serverset_path']),
                ('recovery-tool', 'org.apache.aurora.scheduler.storage.durability.RecoveryTool',
                 ['--help'], ['-from', '-to', '--help'])):
            script = distribution / 'bin' / name
            windows = script.with_suffix('.bat')
            require(script.is_file() and os.access(script, os.X_OK),
                    'Installed Unix launcher is missing or not executable: ' + str(script))
            require(windows.is_file() and windows.stat().st_size > 0,
                    'Installed Windows launcher is missing: ' + str(windows))
            script_text = script.read_text()
            require(main_class in script_text, 'Installed launcher has wrong entrypoint: ' + name)
            references = verify_classpath(script_text, distribution)
            result = run([str(script), *arguments], environment, directory, timeout)
            receipt['launchers'][name] = result
            result.update(unixSha256=digest(script.read_bytes()),
                          windowsSha256=digest(windows.read_bytes()), classpathJars=references)
            verify_help(result, main_class, options)
    receipt['status'] = 'passed'


def main(arguments=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('distribution', type=Path)
    parser.add_argument('--receipt', type=Path, required=True)
    parser.add_argument('--timeout', type=float, default=30)
    args = parser.parse_args(arguments)
    distribution = args.distribution.absolute()
    receipt = {'schema': 1, 'distribution': str(distribution), 'status': 'failed',
               'checkedAt': datetime.now(timezone.utc).isoformat()}
    try:
        require(args.timeout > 0, 'Timeout must be positive')
        verify(distribution, receipt, args.timeout)
    except (VerificationError, OSError, ValueError, zipfile.BadZipFile,
            subprocess.TimeoutExpired) as error:
        receipt['error'] = str(error)
        print('Distribution verification failed: ' + str(error), file=sys.stderr)
    args.receipt.parent.mkdir(parents=True, exist_ok=True)
    args.receipt.write_text(json.dumps(receipt, indent=2, sort_keys=True) + '\n')
    if receipt['status'] == 'passed':
        print('Installed distribution verified; receipt: ' + str(args.receipt))
        return 0
    return 1


if __name__ == '__main__':
    sys.exit(main())
