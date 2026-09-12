#!/usr/bin/env python3
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Build and cache the checksum-pinned Apache Thrift 0.10.0 compiler.

The compiler is built from the official source archive so generated code stays
compatible with Aurora's original Thrift 0.10 schemas. On success this command
prints only the absolute compiler path; diagnostics and build output use stderr.
"""

import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile
import urllib.request

import bootstrap


ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
DEFAULT_CACHE = ROOT / '.cache' / 'inplace-thrift'
RECEIPT = 'compiler-receipt.json'
EXTRACTION_RECIPE = (
    'bootstrap.extract followed by validated regular-file archive mtime restoration'
)


def fail(message):
    raise bootstrap.BuildError(message)


def digest(path):
    return bootstrap.sha256(path)


def load_pins():
    with (HERE / 'thrift-tools.json').open() as stream:
        pins = json.load(stream)
    source = pins['source']
    required = ('version', 'file', 'url', 'sha256')
    if any(not source.get(key) for key in required):
        fail('Thrift source pin is incomplete')
    if not source['url'].startswith('https://'):
        fail('Thrift source requires HTTPS')
    if len(source['sha256']) != 64 or any(c not in '0123456789abcdef' for c in source['sha256']):
        fail('Thrift source checksum is invalid')
    if not pins.get('compiler') or not pins.get('configure'):
        fail('Thrift compiler build pin is incomplete')
    return pins


def ensure_user_cache(cache):
    bootstrap.real_path(cache)
    cache.mkdir(parents=True, exist_ok=True, mode=0o700)
    if cache.stat().st_uid == os.getuid():
        cache.chmod(0o700)
    if cache.stat().st_uid != os.getuid() or cache.stat().st_mode & 0o022:
        fail('Thrift cache must be owned by this user and not group/world writable')


def download_archive(source, archive, seed, offline):
    bootstrap.real_path(archive)
    if archive.exists():
        if not archive.is_file() or digest(archive) != source['sha256']:
            fail('Cached Thrift source checksum mismatch')
        return

    temporary = archive.with_name(archive.name + '.part')
    bootstrap.real_path(temporary)
    if temporary.exists():
        fail('Incomplete Thrift source download exists: ' + str(temporary))
    try:
        candidate = seed / source['file'] if seed is not None else None
        if candidate is not None and candidate.is_file():
            if digest(candidate) != source['sha256']:
                fail('Seed Thrift source checksum mismatch')
            shutil.copyfile(candidate, temporary)
        else:
            if offline:
                fail('Offline Thrift source is missing: ' + source['file'])
            print('Downloading pinned Apache Thrift source', file=sys.stderr, flush=True)
            with urllib.request.urlopen(source['url'], timeout=60) as response, temporary.open('xb') as target:
                if not response.url.startswith('https://'):
                    fail('Insecure Thrift source redirect')
                shutil.copyfileobj(response, target)
        if digest(temporary) != source['sha256']:
            fail('Thrift source checksum mismatch')
        os.replace(temporary, archive)
    finally:
        if temporary.is_file():
            temporary.unlink()


def run_checked(command, cwd):
    print('+ ' + ' '.join(command), file=sys.stderr, flush=True)
    subprocess.run(command, cwd=cwd, stdout=sys.stderr, stderr=sys.stderr, check=True)


def extract_source(archive, destination):
    """Extract safely and restore regular-file mtimes needed by autotools makefiles."""
    bootstrap.extract(archive, destination)
    with tarfile.open(archive, 'r:*') as source:
        for item in source:
            if not item.isfile():
                continue
            target = bootstrap.archive_member(destination, item.name)
            bootstrap.require(
                not target.is_symlink() and target.is_file(),
                'Extracted archive member is not a regular file: ' + item.name)
            os.utime(target, (item.mtime, item.mtime), follow_symlinks=False)


def compiler_version(compiler):
    result = subprocess.run([str(compiler), '--version'], stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, text=True, check=False)
    if result.returncode != 0:
        return ''
    return result.stdout.strip()


def recipe_hash(pins):
    recipe = {
        'compiler': pins['compiler'],
        'configure': pins['configure'],
        'make': ['make', '-C', 'compiler/cpp', '-j1'],
        'extraction': EXTRACTION_RECIPE,
    }
    encoded = json.dumps(recipe, sort_keys=True, separators=(',', ':')).encode()
    return hashlib.sha256(encoded).hexdigest()


def receipt_matches(receipt_path, source_dir, source, compiler, compiler_pin, recipe):
    bootstrap.real_path(receipt_path)
    bootstrap.real_path(source_dir)
    bootstrap.real_path(compiler)
    if not receipt_path.is_file() or not compiler.is_file():
        return False
    try:
        receipt = json.loads(receipt_path.read_text())
    except (OSError, ValueError):
        return False
    expected_version = 'Thrift version ' + source['version']
    if not (receipt.get('source_sha256') == source['sha256'] and
            receipt.get('source_url') == source['url'] and
            receipt.get('source_version') == source['version'] and
            receipt.get('compiler') == compiler_pin and
            receipt.get('recipe_sha256') == recipe and
            receipt.get('compiler_sha256') == digest(compiler) and
            receipt.get('version') == expected_version):
        return False
    return compiler_version(compiler) == expected_version


def build(pins, cache, archive):
    source = pins['source']
    source_dir = cache / 'source'
    compiler = source_dir / pins['compiler']
    receipt = cache / RECEIPT
    recipe = recipe_hash(pins)
    if receipt_matches(receipt, source_dir, source, compiler, pins['compiler'], recipe):
        return compiler

    if source_dir.exists() or source_dir.is_symlink():
        bootstrap.real_path(source_dir)
        if source_dir.is_dir():
            shutil.rmtree(source_dir)
        else:
            source_dir.unlink()
    staging = Path(tempfile.mkdtemp(prefix='extract-', dir=cache))
    extract_dir = staging / 'out'
    try:
        extract_source(archive, extract_dir)
        extracted = extract_dir / ('thrift-' + source['version'])
        if not extracted.is_dir():
            fail('Thrift source archive layout differs')
        os.replace(extracted, source_dir)
    finally:
        if staging.exists():
            shutil.rmtree(staging)

    for tool in ('make', 'g++'):
        if shutil.which(tool) is None:
            fail('Thrift compiler build requires ' + tool)
    run_checked(['./configure', *pins['configure']], source_dir)
    run_checked(['make', '-C', 'compiler/cpp', '-j1'], source_dir)
    if not compiler.is_file():
        fail('Thrift compiler build produced no compiler')
    version = compiler_version(compiler)
    if version != 'Thrift version ' + source['version']:
        fail('Built compiler version is not Thrift ' + source['version'])
    receipt.write_text(json.dumps({
        'source_version': source['version'],
        'source_url': source['url'],
        'source_sha256': source['sha256'],
        'compiler': pins['compiler'],
        'recipe_sha256': recipe,
        'compiler_sha256': digest(compiler),
        'version': version,
    }, indent=2) + '\n')
    return compiler


def main():
    try:
        pins = load_pins()
        cache = Path(os.environ.get('AURORA_INPLACE_THRIFT_CACHE', str(DEFAULT_CACHE))).absolute()
        seed_value = os.environ.get('AURORA_INPLACE_THRIFT_SEED_ARCHIVES') or os.environ.get(
            'AURORA_INPLACE_SEED_ARCHIVES')
        seed = Path(seed_value).absolute() if seed_value else None
        ensure_user_cache(cache)
        import fcntl
        lock_path = cache / 'thrift.lock'
        bootstrap.real_path(lock_path)
        with lock_path.open('a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            archive_dir = cache / 'archives'
            archive_dir.mkdir(mode=0o700, exist_ok=True)
            archive = archive_dir / pins['source']['file']
            download_archive(pins['source'], archive, seed, '--offline' in sys.argv[1:])
            compiler = build(pins, cache, archive)
        print(compiler.resolve())
        return 0
    except (bootstrap.BuildError, OSError, ValueError, KeyError, subprocess.CalledProcessError) as error:
        print('Thrift bootstrap failed: ' + str(error), file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
