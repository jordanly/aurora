# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed under
# the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
# See the License for the specific language governing permissions and limitations under the License.
"""Shared lab process, path, hashing and durable evidence utilities."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import subprocess

BASE_IMAGE = json.loads((Path(__file__).resolve().parents[1] / "native/toolchains.json").read_text())["baseImage"]


class SmokeError(Exception):
    pass


def redact(value: str) -> str:
    value = re.sub(r"-----BEGIN [^-]*PRIVATE KEY-----.*?-----END [^-]*PRIVATE KEY-----",
                   "<redacted private key>", value, flags=re.S)
    value = re.sub(r"(?i)(authorization[\"']?\s*[:=]\s*)[^\r\n]+", r"\1<redacted>", value)
    value = re.sub(r"(?i)(password|token|secret|private[_-]?key)([\"']?\s*[=:]\s*[\"']?)"
                   r"[^\s,\"'}]+", r"\1\2<redacted>", value)
    return value[:65536]


def run(command: list[str], env: dict[str, str], timeout: int = 30,
        check: bool = True, cwd: Path | None = None):
    try:
        result = subprocess.run(command, env=env, cwd=cwd, text=True,
                                capture_output=True, timeout=timeout, check=False)
    except subprocess.TimeoutExpired as error:
        raise SmokeError("Command timed out: " + Path(command[0]).name) from error
    except OSError as error:
        raise SmokeError("Command unavailable: " + Path(command[0]).name) from error
    if check and result.returncode:
        raise SmokeError("Command failed: " + Path(command[0]).name + ": "
                         + redact(result.stderr or result.stdout))
    return result


def safe_path(path: Path) -> None:
    if not path.is_absolute() or ".." in path.parts:
        raise SmokeError("Path must be absolute without traversal")
    if any(ord(char) < 32 or ord(char) > 126 for char in str(path)):
        raise SmokeError("Control/non-ASCII characters are unsupported in paths")
    for ancestor in (path, *path.parents):
        if ancestor.is_symlink():
            raise SmokeError("Symlink paths are unsupported")


def regular(path: Path) -> None:
    safe_path(path.absolute())
    if not path.is_file():
        raise SmokeError("Required regular artifact missing: " + str(path))


def sha(path: Path) -> str:
    regular(path)
    return hashlib.sha256(path.read_bytes()).hexdigest()


def tree_sha(directory: Path, allow_internal_links: bool = False) -> str:
    digest = hashlib.sha256()
    for path in sorted(directory.rglob("*")):
        if path.is_symlink():
            try:
                path.resolve(strict=True).relative_to(directory.resolve())
            except (ValueError, OSError) as error:
                raise SmokeError("Symlink escapes toolchain tree") from error
            if not allow_internal_links:
                raise SmokeError("Symlink in source tree")
            digest.update(str(path.relative_to(directory)).encode() + b"\0link\0")
            digest.update(os.readlink(path).encode() + b"\0")
            continue
        if path.is_file():
            digest.update(str(path.relative_to(directory)).encode() + b"\0")
            digest.update(bytes.fromhex(sha(path)))
    return digest.hexdigest()


def write_json(path: Path, value: dict) -> None:
    safe_path(path)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "w") as output:
        json.dump(value, output, indent=2, sort_keys=True)
        output.write("\n")
        output.flush()
        os.fsync(output.fileno())


