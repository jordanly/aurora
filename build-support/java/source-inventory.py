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
"""Inventory original Aurora inputs without mistaking file counts for test results.

The baseline is an immutable Git commit; execution results belong in separate
test receipts. Missing inputs fail this initial build-restoration audit. Modified
inputs are listed for review, not automatically rejected as semantic changes.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess


ROOT = Path(__file__).resolve().parents[2]
BASELINE = '11ebaeeb071cb182c388a40755e84f60dda32260'
COMPONENTS = {
    'scheduler_java': 'src/main/java/',
    'scheduler_java_tests': 'src/test/java/',
    'benchmarks': 'src/jmh/',
    'commons_java': 'commons/src/main/java/',
    'commons_java_tests': 'commons/src/test/java/',
    'api_schemas': 'api/src/',
    'runtime_resources': 'src/main/resources/',
    'test_resources': 'src/test/resources/',
    'python_runtime_and_tools': 'src/main/python/',
    'python_tests': 'src/test/python/',
    'ui': 'ui/',
    'build_plugins': 'buildSrc/',
}


def inventory(baseline):
    commit = subprocess.check_output(
        ['git', 'rev-parse', '--verify', baseline + '^{commit}'], cwd=ROOT, text=True).strip()
    entries = subprocess.check_output(['git', 'ls-tree', '-rz', commit], cwd=ROOT)
    groups = {name: {'root': prefix, 'files': []} for name, prefix in COMPONENTS.items()}
    missing = []
    modified = []
    for entry in entries.split(b'\0'):
        if not entry:
            continue
        metadata, raw_path = entry.split(b'\t', 1)
        mode, kind, blob = metadata.decode().split()
        path = raw_path.decode()
        group = next((name for name, prefix in COMPONENTS.items() if path.startswith(prefix)), None)
        if group is None or kind != 'blob':
            continue
        item = {'path': path, 'baselineGitBlob': blob}
        current = ROOT / path
        if current.is_symlink() or current.is_file():
            data = os.readlink(current).encode() if current.is_symlink() else current.read_bytes()
            current_blob = hashlib.sha1(b'blob ' + str(len(data)).encode() + b'\0' + data).hexdigest()
            if current_blob != blob or (mode == '120000') != current.is_symlink():
                item['currentGitBlob'] = current_blob
                item['currentIsSymlink'] = current.is_symlink()
                modified.append(path)
        else:
            item['missing'] = True
            missing.append(path)
        groups[group]['files'].append(item)
    for group in groups.values():
        group['fileCount'] = len(group['files'])
        group['javaSourceCount'] = sum(item['path'].endswith('.java') for item in group['files'])
    return {
        'baselineCommit': commit,
        'scope': 'Original tracked source, schema, resource, UI and build-plugin inputs; counts are files, not executed tests.',
        'components': groups,
        'missing': missing,
        'modified': modified,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline', default=BASELINE)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    result = inventory(args.baseline)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({'output': str(args.output), 'missing': result['missing'],
                      'modified': result['modified'],
                      'counts': {name: group['fileCount'] for name, group in result['components'].items()}}))
    return 1 if result['missing'] else 0


if __name__ == '__main__':
    raise SystemExit(main())
