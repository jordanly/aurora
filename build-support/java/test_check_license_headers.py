# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import importlib.util
from pathlib import Path
import os
import contextlib
import io
import tempfile
import unittest


SPEC = importlib.util.spec_from_file_location(
    'check_license_headers', Path(__file__).with_name('check-license-headers.py'))
checker = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(checker)


JAVA_HEADER = '''/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
'''


class LicenseHeaderTest(unittest.TestCase):
  def write(self, directory, name, content, newline='\n'):
    path = Path(directory) / name
    path.write_bytes(content.replace('\n', newline).encode('utf-8'))
    return path

  def test_accepts_canonical_block_header_with_crlf(self):
    with tempfile.TemporaryDirectory() as directory:
      path = self.write(directory, 'Good.java', JAVA_HEADER + 'class Good {}\n', '\r\n')
      self.assertTrue(checker.has_license_header(path))

  def test_accepts_shebang_and_hash_header(self):
    with tempfile.TemporaryDirectory() as directory:
      header = '\n'.join('# ' + line for line in checker.LICENSE_LINES) + '\n'
      path = self.write(directory, 'good.py', '#!/usr/bin/env python3\n' + header)
      self.assertTrue(checker.has_license_header(path))

  def test_rejects_license_text_after_source(self):
    with tempfile.TemporaryDirectory() as directory:
      path = self.write(directory, 'Bad.java', 'class Bad {}\n' + JAVA_HEADER)
      self.assertFalse(checker.has_license_header(path))

  def test_rejects_wrong_or_missing_header(self):
    with tempfile.TemporaryDirectory() as directory:
      wrong = self.write(directory, 'Wrong.java', '/* Licensed under another license. */\n')
      missing = self.write(directory, 'Missing.java', 'package example;\n')
      self.assertEqual([missing, wrong], sorted(checker.check_roots([directory])))

  def test_skips_caches_and_symlinks(self):
    with tempfile.TemporaryDirectory() as directory:
      self.write(directory, 'Good.java', JAVA_HEADER + 'class Good {}\n')
      cache = Path(directory) / 'build'
      cache.mkdir()
      self.write(cache, 'Bad.java', 'class Bad {}\n')
      cache = Path(directory) / '.cache'
      cache.mkdir()
      self.write(cache, 'AlsoBad.java', 'class AlsoBad {}\n')
      link = Path(directory) / 'Link.java'
      try:
        os.symlink(Path(directory) / 'Missing.java', link)
      except (OSError, NotImplementedError):
        pass
      self.assertEqual([], checker.check_roots([directory]))

  def test_custom_header_is_used_by_cli(self):
    with tempfile.TemporaryDirectory() as directory:
      custom = self.write(directory, 'custom.header', 'Custom license text.\n')
      source = self.write(directory, 'Custom.java', '// Custom license text.\nclass Custom {}\n')
      with contextlib.redirect_stdout(io.StringIO()):
        self.assertEqual(0, checker.main([
            directory, '--header', str(custom), '--extensions', '.java']))
      source.write_text(JAVA_HEADER + 'class Custom {}\n', encoding='utf-8')
      with contextlib.redirect_stderr(io.StringIO()):
        self.assertEqual(1, checker.main([
            directory, '--header', str(custom), '--extensions', '.java']))

  def test_cli_rejects_symlink_root(self):
    with tempfile.TemporaryDirectory() as directory:
      real = Path(directory) / 'real'
      real.mkdir()
      self.write(real, 'Good.java', JAVA_HEADER + 'class Good {}\n')
      link = Path(directory) / 'link'
      try:
        os.symlink(real, link, target_is_directory=True)
      except (OSError, NotImplementedError):
        self.skipTest('symlinks are unavailable')
      with self.assertRaises(SystemExit) as error:
        checker.main([str(link), '--extensions', '.java'])
      self.assertEqual(2, error.exception.code)

  def test_cli_rejects_empty_scope(self):
    with tempfile.TemporaryDirectory() as directory:
      self.write(directory, 'ignored.txt', 'not a source file\n')
      with self.assertRaises(SystemExit) as error:
        checker.main([directory, '--extensions', '.java'])
      self.assertEqual(2, error.exception.code)


if __name__ == '__main__':
  unittest.main()
