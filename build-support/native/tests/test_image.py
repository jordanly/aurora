# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed
# under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.
import hashlib
import importlib.machinery
import importlib.util
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest

loader = importlib.machinery.SourceFileLoader('image_boundary', str(Path(__file__).resolve().parents[1] / 'verify-image'))
spec = importlib.util.spec_from_loader(loader.name, loader)
image = importlib.util.module_from_spec(spec)
loader.exec_module(image)


class ImageVerifyTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.base = [('etc/debian_version', b'12'), ('bin', 'usr/bin'), ('usr/bin/tool', b'elf')]
        self.payload = [('opt/aurora/bin/agent', b'agent')]
        self.expected = self.root / 'expected.json'
        self.expected.write_text(json.dumps({'/opt/aurora/bin/agent': hashlib.sha256(b'agent').hexdigest()}))
        self.tar(self.root / 'base.tar', self.base)

    def tar(self, path, entries):
        with tarfile.open(path, 'w') as archive:
            for name, value in entries:
                member = tarfile.TarInfo(name)
                if isinstance(value, bytes):
                    member.size = len(value)
                    archive.addfile(member, io.BytesIO(value))
                else:
                    member.type = tarfile.SYMTYPE
                    member.linkname = value
                    archive.addfile(member)

    def verify(self, entries):
        self.tar(self.root / 'image.tar', entries)
        return image.verify(self.root / 'image.tar', self.expected, self.root / 'base.tar')

    def test_valid_base_symlink_and_generated_files(self):
        report = self.verify(self.base + self.payload + [('etc/hostname', b'random-container')])
        self.assertTrue(report['ok'])

    def test_missing_and_tampered_payload(self):
        for payload in ([], [('opt/aurora/bin/agent', b'tamper')]):
            with self.subTest(payload=payload), self.assertRaises(ValueError):
                self.verify(self.base + payload)

    def test_extra_payload(self):
        with self.assertRaisesRegex(ValueError, 'Unexpected payload'):
            self.verify(self.base + self.payload + [('opt/aurora/lib/sneaky.jar', b'foreign')])

    def test_payload_links_and_root_link(self):
        for extra in ([('opt/aurora/bin/agent', '../other')], self.payload + [('opt/aurora', '/tmp')]):
            with self.subTest(extra=extra), self.assertRaises(ValueError):
                self.verify(self.base + extra)

    def test_renamed_foreign_base_payload(self):
        for name in ('usr/lib/renamed.jar', 'usr/bin/harmless', 'usr/lib/python3/site-packages/worker.py'):
            with self.subTest(name=name), self.assertRaisesRegex(ValueError, 'base file'):
                self.verify(self.base + self.payload + [(name, b'foreign')])

    def test_changed_missing_base_file_and_symlink(self):
        for entries in (self.base[1:], [('etc/debian_version', b'changed')] + self.base[1:],
                        [self.base[0], ('bin', 'tmp'), self.base[2]]):
            with self.subTest(entries=entries), self.assertRaises(ValueError):
                self.verify(entries + self.payload)

    def test_generated_file_cannot_be_symlink(self):
        with self.assertRaisesRegex(ValueError, 'not a regular file'):
            self.verify(self.base + self.payload + [('etc/hosts', '/opt/aurora/bin/agent')])

    def test_duplicate_and_traversal(self):
        for extra in (self.payload, [('opt/aurora/../evil', b'foreign')], [('/tmp/evil', b'foreign')]):
            with self.subTest(extra=extra), self.assertRaises(ValueError):
                self.verify(self.base + self.payload + extra)

    def test_invalid_expected_manifest(self):
        for value in ('{}', '{"/opt/aurora/../a":"' + '0' * 64 + '"}',
                      '{"/opt/aurora/a":"' + '0' * 64 + '","/opt/aurora/a":"' + '0' * 64 + '"}'):
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.expected.write_text(value)
                self.verify(self.base + self.payload)


if __name__ == '__main__':
    unittest.main()
