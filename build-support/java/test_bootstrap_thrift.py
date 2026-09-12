#!/usr/bin/env python3
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Focused tests for the checksum-pinned Thrift bootstrap helper."""

import importlib.util
import io
import json
from pathlib import Path
import os
import subprocess
import sys
import tarfile
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).parent))
_SPEC = importlib.util.spec_from_file_location(
    'bootstrap_thrift', Path(__file__).with_name('bootstrap-thrift.py'))
bootstrap_thrift = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(bootstrap_thrift)


ROOT = Path(__file__).resolve().parents[2]


class BootstrapThriftTest(unittest.TestCase):

  def test_configure_recipe_builds_compiler_only(self):
    self.assertEqual([
        '--without-libs',
        '--without-tests',
        '--without-tutorial',
        '--disable-plugin',
    ], bootstrap_thrift.load_pins()['configure'])

  def test_extract_source_preserves_autotools_file_order(self):
    with tempfile.TemporaryDirectory() as directory:
      root = Path(directory)
      archive = root / 'source.tar.gz'
      with tarfile.open(archive, 'w:gz') as output:
        for name, mtime in (
            ('thrift-0.10.0/Makefile.am', 100),
            ('thrift-0.10.0/Makefile', 200),
            ('thrift-0.10.0/configure', 300)):
          data = name.encode()
          member = tarfile.TarInfo(name)
          member.size = len(data)
          member.mtime = mtime
          member.mode = 0o755 if name.endswith('configure') else 0o644
          output.addfile(member, io.BytesIO(data))
      destination = root / 'extracted'

      bootstrap_thrift.extract_source(archive, destination)

      self.assertEqual(100, (destination / 'thrift-0.10.0/Makefile.am').stat().st_mtime)
      self.assertEqual(200, (destination / 'thrift-0.10.0/Makefile').stat().st_mtime)
      self.assertEqual(300, (destination / 'thrift-0.10.0/configure').stat().st_mtime)

  def test_source_pin_matches_verified_archive(self):
    pins = bootstrap_thrift.load_pins()
    cache = Path(os.environ.get(
        'AURORA_INPLACE_THRIFT_CACHE', str(bootstrap_thrift.DEFAULT_CACHE)))
    archive = cache / 'archives' / pins['source']['file']
    if not archive.is_file():
      self.skipTest('seeded Thrift archive is unavailable')
    self.assertEqual(pins['source']['sha256'], bootstrap_thrift.digest(archive))

  def test_offline_missing_source_has_no_stdout(self):
    with tempfile.TemporaryDirectory() as directory:
      seed = Path(directory) / 'seed'
      seed.mkdir()
      environment = dict(os.environ,
                         AURORA_INPLACE_THRIFT_CACHE=str(Path(directory) / 'cache'),
                         AURORA_INPLACE_THRIFT_SEED_ARCHIVES=str(seed))
      result = subprocess.run(
          [sys.executable, str(Path(__file__).with_name('bootstrap-thrift.py')), '--offline'],
          cwd=ROOT, env=environment, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
          text=True, check=False)
    self.assertNotEqual(0, result.returncode)
    self.assertEqual('', result.stdout)
    self.assertIn('Offline Thrift source is missing', result.stderr)

  def test_receipt_gates_reuse_on_compiler_hash_and_version(self):
    pins = bootstrap_thrift.load_pins()
    with tempfile.TemporaryDirectory() as directory:
      cache = Path(directory)
      source = cache / 'source'
      compiler = source / pins['compiler']
      compiler.parent.mkdir(parents=True)
      compiler.write_text('#!/bin/sh\necho Thrift version 0.10.0\n')
      compiler.chmod(0o755)
      receipt = {
          'source_version': pins['source']['version'],
          'source_url': pins['source']['url'],
          'source_sha256': pins['source']['sha256'],
          'compiler': pins['compiler'],
          'recipe_sha256': bootstrap_thrift.recipe_hash(pins),
          'compiler_sha256': bootstrap_thrift.digest(compiler),
          'version': bootstrap_thrift.compiler_version(compiler),
      }
      (cache / bootstrap_thrift.RECEIPT).write_text(json.dumps(receipt))
      result = bootstrap_thrift.build(pins, cache, Path('/unused/archive'))
      self.assertEqual(compiler, result)

  def test_tampered_compiler_is_not_executed_for_receipt_check(self):
    pins = bootstrap_thrift.load_pins()
    with tempfile.TemporaryDirectory() as directory:
      cache = Path(directory)
      source = cache / 'source'
      compiler = source / pins['compiler']
      compiler.parent.mkdir(parents=True)
      marker = cache / 'executed'
      compiler.write_text('#!/bin/sh\ntouch "' + str(marker) + '"\necho bad\n')
      compiler.chmod(0o755)
      receipt = {
          'source_version': pins['source']['version'],
          'source_url': pins['source']['url'],
          'source_sha256': pins['source']['sha256'],
          'compiler': pins['compiler'],
          'recipe_sha256': bootstrap_thrift.recipe_hash(pins),
          'compiler_sha256': '0' * 64,
          'version': 'Thrift version 0.10.0',
      }
      receipt_path = cache / bootstrap_thrift.RECEIPT
      receipt_path.write_text(json.dumps(receipt))
      self.assertFalse(bootstrap_thrift.receipt_matches(
          receipt_path, source, pins['source'], compiler, pins['compiler'],
          bootstrap_thrift.recipe_hash(pins)))
      self.assertFalse(marker.exists())

  def test_receipt_rejects_wrong_recipe_and_version(self):
    pins = bootstrap_thrift.load_pins()
    with tempfile.TemporaryDirectory() as directory:
      cache = Path(directory)
      source = cache / 'source'
      compiler = source / pins['compiler']
      compiler.parent.mkdir(parents=True)
      compiler.write_text('#!/bin/sh\necho Thrift version 0.10.0-extra\n')
      compiler.chmod(0o755)
      receipt_path = cache / bootstrap_thrift.RECEIPT
      receipt_path.write_text(json.dumps({
          'source_version': pins['source']['version'],
          'source_url': pins['source']['url'],
          'source_sha256': pins['source']['sha256'],
          'compiler': pins['compiler'],
          'recipe_sha256': '0' * 64,
          'compiler_sha256': bootstrap_thrift.digest(compiler),
      }))
      self.assertFalse(bootstrap_thrift.receipt_matches(
          receipt_path, source, pins['source'], compiler, pins['compiler'],
          bootstrap_thrift.recipe_hash(pins)))


if __name__ == '__main__':
  unittest.main()
