# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed
# under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.
"""Checksum-pinned native build tools; no reliance on a previous checkout's tools."""
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


def sha(path):
    digest = hashlib.sha256()
    with open(path, 'rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def real_path(path):
    require(path.is_absolute(), 'Path must be absolute: ' + str(path))
    require(not any(p.is_symlink() for p in (path, *path.parents)),
            'Symlink path rejected: ' + str(path))


def member_path(root, name):
    value = PurePosixPath(name)
    require(name and not value.is_absolute() and '..' not in value.parts and '\\' not in name,
            'Unsafe archive entry: ' + name)
    target = root.joinpath(*value.parts)
    real_path(target)
    return target


def extract(archive, destination):
    """Extract into a fresh directory; links are deferred and must stay inside it."""
    require(not destination.exists(), 'Extraction destination must be new')
    destination.mkdir(mode=0o700)
    seen = set()
    total = 0

    def reserve(name, size):
        nonlocal total
        target = member_path(destination, name)
        require(target not in seen, 'Duplicate archive entry')
        seen.add(target)
        total += size
        require(0 <= size <= 512 * 1024 * 1024 and total <= 2 * 1024 ** 3,
                'Tool archive is too large')
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
    else:
        links = []
        with tarfile.open(archive, 'r:*') as source:
            for item in source:
                target = reserve(item.name, item.size)
                if item.isdir():
                    target.mkdir(parents=True, exist_ok=True)
                elif item.isfile():
                    target.parent.mkdir(parents=True, exist_ok=True)
                    with source.extractfile(item) as src, target.open('xb') as dst:
                        shutil.copyfileobj(src, dst)
                    target.chmod(0o755 if item.mode & 0o111 else 0o644)
                elif item.issym() or item.islnk():
                    links.append((target, item.linkname, item.islnk()))
                else:
                    raise BuildError('Archive special file rejected')
        for target, name, hard in links:
            require(not Path(name).is_absolute() and '\\' not in name, 'Absolute archive link')
            source = (destination if hard else target.parent) / name
            source = source.resolve()
            require(source.is_relative_to(destination) and (source.is_file() or source.is_dir() and not hard), 'Escaping/dangling archive link')
            target.parent.mkdir(parents=True, exist_ok=True)
            # Flatten links; the resulting tool/runtime is self-contained.
            if source.is_dir():
                require(not target.is_relative_to(source), 'Cyclic archive directory link')
                shutil.copytree(source, target)
            else:
                shutil.copyfile(source, target)
                shutil.copymode(source, target)


def get_tools(pins, cache, work, seed=None, offline=False):
    archives = cache / 'archives'
    archives.mkdir(exist_ok=True)
    result = {}
    for name, pin in pins['tools'].items():
        archive = archives / pin['file']
        real_path(archive)
        if archive.exists():
            require(archive.is_file() and sha(archive) == pin['sha256'], 'Cached tool checksum mismatch: ' + name)
        else:
            temporary = archive.with_suffix(archive.suffix + '.part')
            real_path(temporary)
            require(not temporary.exists(), 'Incomplete download exists: ' + str(temporary))
            try:
                candidate = seed / pin['file'] if seed is not None else None
                if candidate is not None and candidate.is_file():
                    require(sha(candidate) == pin['sha256'], 'Seed tool checksum mismatch: ' + name)
                    shutil.copyfile(candidate, temporary)
                else:
                    require(not offline, 'Offline tool missing: ' + name)
                    require(pin['url'].startswith('https://'), 'Tools require HTTPS')
                    print('Downloading pinned ' + name, flush=True)
                    with urllib.request.urlopen(pin['url'], timeout=60) as src, temporary.open('xb') as dst:
                        require(src.url.startswith('https://'), 'Insecure tool redirect')
                        shutil.copyfileobj(src, dst)
                require(sha(temporary) == pin['sha256'], 'Downloaded tool checksum mismatch: ' + name)
                os.replace(temporary, archive)
            finally:
                if temporary.is_file():
                    temporary.unlink()
        # Never execute an unchecked extracted tool cache. Each build unpacks the verified archive.
        destination = work / name
        print('Extracting verified ' + name, flush=True)
        extract(archive, destination)
        result[name] = destination / pin['directory']
        require(result[name].is_dir(), 'Tool archive layout differs: ' + name)
    return result
