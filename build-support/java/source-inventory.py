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

Four reviewed Java files moved from ``resources`` to ``mesos`` during the
restoration. They are accepted only through the explicit rename map beside this
module; an original and its mapped target existing together is an ambiguity and
still fails the audit.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess


ROOT = Path(__file__).resolve().parents[2]
BASELINE = '11ebaeeb071cb182c388a40755e84f60dda32260'
RENAME_MAP_PATH = Path(__file__).with_name('source-inventory-renames.json')
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


def _load_rename_map(path=RENAME_MAP_PATH):
    data = json.loads(path.read_text())
    renames = data.get('renames')
    if not isinstance(renames, list):
        raise ValueError('rename map must contain a renames list')
    result = {}
    targets = set()
    for rename in renames:
        original = rename.get('originalPath')
        current = rename.get('currentPath')
        if not isinstance(original, str) or not isinstance(current, str):
            raise ValueError('rename entries require originalPath and currentPath')
        if original in result or current in targets:
            raise ValueError('rename map contains duplicate paths')
        if (Path(original).is_absolute() or Path(current).is_absolute()
                or '..' in Path(original).parts or '..' in Path(current).parts):
            raise ValueError('rename paths must be relative')
        result[original] = rename
        targets.add(current)
    return result


def _present(path):
    return path.is_symlink() or path.is_file()


def inventory(baseline, root=ROOT, rename_map=None):
    if rename_map is None:
        rename_map = _load_rename_map()
    commit = subprocess.check_output(
        ['git', 'rev-parse', '--verify', baseline + '^{commit}'], cwd=root, text=True).strip()
    entries = subprocess.check_output(['git', 'ls-tree', '-rz', commit], cwd=root)
    groups = {name: {'root': prefix, 'files': []} for name, prefix in COMPONENTS.items()}
    missing = []
    modified = []
    ambiguous = []
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
        current = root / path
        relocation = rename_map.get(path)
        mapped_current = root / relocation['currentPath'] if relocation else None
        if relocation:
            item['currentPath'] = relocation['currentPath']
            item['relocation'] = dict(relocation)
        if _present(current) and mapped_current is not None and _present(mapped_current):
            item['ambiguous'] = True
            item['relocation']['status'] = 'ambiguous'
            ambiguous.append(path)
        elif not _present(current) and mapped_current is not None and _present(mapped_current):
            item['relocation']['status'] = 'relocated'
            current = mapped_current
        elif relocation:
            item['relocation']['status'] = (
                'original-present' if _present(current) else 'target-missing')
        if _present(current):
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
        'ambiguous': ambiguous,
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
                      'ambiguous': result['ambiguous'],
                      'modified': result['modified'],
                      'counts': {name: group['fileCount'] for name, group in result['components'].items()}}))
    return 1 if result['missing'] or result['ambiguous'] else 0


if __name__ == '__main__':
    raise SystemExit(main())
