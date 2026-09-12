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
"""Check Apache license headers in source files under explicit roots.

The checker intentionally examines only the leading comment.  License text
appearing later in a source file cannot satisfy the check.
"""
import argparse
import os
from pathlib import Path
import sys


LICENSE_LINES = (
    'Licensed under the Apache License, Version 2.0 (the "License");',
    'you may not use this file except in compliance with the License.',
    'You may obtain a copy of the License at',
    'http://www.apache.org/licenses/LICENSE-2.0',
    'Unless required by applicable law or agreed to in writing, software',
    'distributed under the License is distributed on an "AS IS" BASIS,',
    'WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.',
    'See the License for the specific language governing permissions and',
    'limitations under the License.',
)

DEFAULT_EXTENSIONS = frozenset({
    '.gradle', '.groovy', '.java', '.js', '.proto', '.py', '.sh', '.thrift',
})
SKIP_DIRECTORIES = frozenset({
    '.cache', '.git', '.gradle', '.idea', '.pytest_cache', '__pycache__', 'build',
    'dist', 'node_modules',
})


def _normalized(lines):
  """Normalize comment framing while retaining the license's actual words."""
  return [line.strip() for line in lines if line.strip()]


def _comment_text(lines):
  """Return leading comment text, or None when no leading comment exists."""
  if not lines:
    return None
  index = 0
  if lines[0].startswith('\ufeff'):
    lines[0] = lines[0][1:]
  if lines[0].startswith('#!'):
    index = 1
  if index >= len(lines):
    return None

  first = lines[index].lstrip()
  if first.startswith('/*'):
    text = []
    for line in lines[index:]:
      stripped = line.strip()
      if stripped.startswith('/*'):
        stripped = stripped[2:]
      elif stripped.endswith('*/'):
        stripped = stripped[:-2]
        text.append(stripped.lstrip(' *'))
        return text
      text.append(stripped.lstrip(' *'))
    return None

  prefix = None
  if first.startswith('#'):
    prefix = '#'
  elif first.startswith('//'):
    prefix = '//'
  if prefix is None:
    return None
  text = []
  for line in lines[index:]:
    stripped = line.lstrip()
    if not stripped.startswith(prefix):
      break
    text.append(stripped[len(prefix):].lstrip())
  return text or None


def has_license_header(path, expected=LICENSE_LINES):
  """Return whether *path* starts with the canonical Apache header."""
  try:
    content = path.read_text(encoding='utf-8')
  except (OSError, UnicodeError):
    return False
  return _normalized(_comment_text(content.replace('\r\n', '\n').split('\n')) or []) == list(expected)


def source_files(roots, extensions=DEFAULT_EXTENSIONS):
  """Yield regular, non-symlink source files below *roots* deterministically."""
  for root in sorted((Path(root) for root in roots), key=str):
    if not root.exists() or root.is_symlink():
      continue
    if root.is_file():
      if root.suffix in extensions:
        yield root
      continue
    for directory, names, filenames in os.walk(root, topdown=True, followlinks=False):
      names[:] = sorted(name for name in names if name not in SKIP_DIRECTORIES)
      for name in sorted(filenames):
        path = Path(directory) / name
        if not path.is_symlink() and path.suffix in extensions:
          yield path


def check_roots(roots, extensions=DEFAULT_EXTENSIONS, expected=LICENSE_LINES):
  """Return paths whose leading headers are absent or non-canonical."""
  return [path for path in source_files(roots, extensions) if not has_license_header(path, expected)]


def main(argv=None):
  parser = argparse.ArgumentParser(description=__doc__)
  parser.add_argument('roots', nargs='+', type=Path,
                      help='tracked source roots to check')
  parser.add_argument('--extensions', nargs='+', default=sorted(DEFAULT_EXTENSIONS),
                      help='file extensions to check, including the leading dot')
  parser.add_argument('--header', type=Path,
                      default=Path(__file__).resolve().parents[2] / 'config/checkstyle/apache.header',
                      help='canonical license text without comment framing')
  args = parser.parse_args(argv)
  missing = [str(root) for root in args.roots if not root.exists()]
  if missing:
    parser.error('source root does not exist: %s' % ', '.join(missing))
  if any(root.is_symlink() for root in args.roots):
    parser.error('source roots must not be symlinks')
  expected = _normalized(args.header.read_text(encoding='utf-8').splitlines())
  if not expected:
    parser.error('canonical license header is empty')
  files = list(source_files(args.roots, frozenset(args.extensions)))
  if not files:
    parser.error('no matching source files found')
  failures = [path for path in files if not has_license_header(path, expected)]
  for path in failures:
    print(path, file=sys.stderr)
  if failures:
    print('license header check failed: %d file(s)' % len(failures), file=sys.stderr)
    return 1
  print('Verified license headers in %d source files' % len(files))
  return 0


if __name__ == '__main__':
  sys.exit(main())
