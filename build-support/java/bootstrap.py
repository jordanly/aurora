#!/usr/bin/env python3
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Checksum-pinned archive bootstrapper for Aurora's in-place Java build."""
import hashlib
import os
from pathlib import Path, PurePosixPath
import shutil
import stat
import tarfile
import urllib.request
import zipfile

class BuildError(Exception):
    pass

def require(condition, message):
    if not condition:
        raise BuildError(message)

def sha256(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()

def real_path(path):
    require(path.is_absolute(), 'Path must be absolute: ' + str(path))
    require(not any(item.is_symlink() for item in (path, *path.parents)),
            'Symlink path rejected: ' + str(path))

def archive_member(root, name):
    value = PurePosixPath(name)
    require(name and not value.is_absolute() and '..' not in value.parts and '\\' not in name,
            'Unsafe archive entry: ' + name)
    target = root.joinpath(*value.parts)
    real_path(target)
    return target

def extract(archive, destination):
    """Extract regular files while rejecting traversal and special files."""
    require(not destination.exists(), 'Extraction destination must be new')
    destination.mkdir(mode=0o700)
    seen, total = set(), 0
    def reserve(name, size):
        nonlocal total
        target = archive_member(destination, name)
        require(target not in seen, 'Duplicate archive entry')
        seen.add(target)
        require(0 <= size <= 512 * 1024 * 1024 and total + size <= 2 * 1024 ** 3,
                'Tool archive is too large')
        total += size
        return target
    if archive.suffix == '.zip':
        with zipfile.ZipFile(archive) as source:
            for item in source.infolist():
                target = reserve(item.filename, item.file_size)
                mode = item.external_attr >> 16
                require(not stat.S_ISLNK(mode), 'ZIP symlink rejected')
                require(stat.S_IFMT(mode) in (0, stat.S_IFREG, stat.S_IFDIR), 'ZIP special file')
                if item.is_dir():
                    target.mkdir(parents=True, exist_ok=True)
                else:
                    target.parent.mkdir(parents=True, exist_ok=True)
                    with source.open(item) as src, target.open('xb') as dst:
                        shutil.copyfileobj(src, dst)
                    target.chmod(0o755 if mode & 0o111 else 0o644)
        return
    links = []
    with tarfile.open(archive, 'r:*') as source:
        for item in source:
            target = reserve(item.name, item.size)
            if item.isdir():
                target.mkdir(parents=True, exist_ok=True)
            elif item.isfile():
                target.parent.mkdir(parents=True, exist_ok=True)
                source_file = source.extractfile(item)
                require(source_file is not None, 'Unreadable archive member')
                with source_file, target.open('xb') as dst:
                    shutil.copyfileobj(source_file, dst)
                target.chmod(0o755 if item.mode & 0o111 else 0o644)
            elif item.issym() or item.islnk():
                links.append((target, item.linkname, item.islnk()))
            else:
                raise BuildError('Archive special file rejected')
    for target, name, hard in links:
        require(not Path(name).is_absolute() and '\\' not in name, 'Absolute archive link')
        source = ((destination if hard else target.parent) / name).resolve()
        require(source.is_relative_to(destination) and
                (source.is_file() or source.is_dir() and not hard),
                'Escaping/dangling archive link')
        target.parent.mkdir(parents=True, exist_ok=True)
        if source.is_dir():
            require(not target.is_relative_to(source), 'Cyclic archive directory link')
            shutil.copytree(source, target)
        else:
            shutil.copyfile(source, target)
            shutil.copymode(source, target)

def get_tools(pins, cache, work, seed=None, offline=False):
    archives = cache / 'archives'
    archives.mkdir(parents=True, exist_ok=True, mode=0o700)
    result = {}
    for name, pin in pins['tools'].items():
        archive = archives / pin['file']
        real_path(archive)
        if archive.exists():
            require(archive.is_file() and sha256(archive) == pin['sha256'],
                    'Cached tool checksum mismatch: ' + name)
        else:
            temporary = archive.with_name(archive.name + '.part')
            real_path(temporary)
            require(not temporary.exists(), 'Incomplete download exists: ' + str(temporary))
            try:
                candidate = seed / pin['file'] if seed is not None else None
                if candidate is not None and candidate.is_file():
                    require(sha256(candidate) == pin['sha256'], 'Seed tool checksum mismatch: ' + name)
                    shutil.copyfile(candidate, temporary)
                else:
                    require(not offline, 'Offline tool missing: ' + name)
                    require(pin['url'].startswith('https://'), 'Tools require HTTPS')
                    print('Downloading pinned ' + name, flush=True)
                    with urllib.request.urlopen(pin['url'], timeout=60) as source, temporary.open('xb') as target:
                        require(source.url.startswith('https://'), 'Insecure tool redirect')
                        shutil.copyfileobj(source, target)
                require(sha256(temporary) == pin['sha256'], 'Tool checksum mismatch: ' + name)
                os.replace(temporary, archive)
            finally:
                if temporary.is_file():
                    temporary.unlink()
        destination = work / name
        extract(archive, destination)
        result[name] = destination / pin['directory']
        require(result[name].is_dir(), 'Tool archive layout differs: ' + name)
    return result
