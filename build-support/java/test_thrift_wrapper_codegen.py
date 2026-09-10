# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Bounded, dependency-free wrapper regressions; no Pants or Thrift binary needed."""
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
GENERATOR = ROOT / 'src/main/python/apache/aurora/tools/java/thrift_wrapper_codegen.py'
API = ROOT / 'api/src/main/thrift/org/apache/aurora/gen/api.thrift'
spec = importlib.util.spec_from_file_location('codegen', str(GENERATOR))
codegen = importlib.util.module_from_spec(spec)
spec.loader.exec_module(codegen)

FIXTURE = '''namespace java org.apache.aurora.gen
enum Color { RED = 1, BLUE = 2 }
struct Child { 1: string name }
struct Empty { }
struct Example {
  1: bool enabled
  2: i64 count
  3: Child child
  4: list<Child> children
  5: set<Color> colors
  6: map<string, string> lookup
}
union Choice {
  1: string text
  2: Child child
}
service AuroraReadOnly { Example get(1: string name) }
service AuroraAdmin extends AuroraReadOnly { void put(1: Example value) }
'''


def render(struct):
  output = io.StringIO()
  codegen.generate_java(struct).dump(output)
  return output.getvalue()


def generate(input_file, destination, seed='1'):
  subprocess.run([sys.executable, '-Werror', str(GENERATOR), str(input_file),
                  str(destination / 'java'), str(destination / 'resources')],
                 env=dict(os.environ, PYTHONHASHSEED=seed), check=True,
                 stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=20)
  return {str(p.relative_to(destination)): hashlib.sha256(p.read_bytes()).hexdigest()
          for p in sorted(destination.rglob('*')) if p.is_file()}


class WrapperGeneratorTest(unittest.TestCase):
  def setUp(self):
    self.structs = {s.name: s for s in codegen.parse_structs(FIXTURE)}

  def test_fields_are_reusable_and_generation_is_repeatable(self):
    struct = self.structs['Example']
    self.assertEqual(6, len(list(struct.fields)))
    self.assertEqual(6, len(list(struct.fields)))
    self.assertEqual(render(struct), render(struct))

  def test_equality_hash_and_string_include_fields(self):
    output = render(self.structs['Example'])
    for name in ('enabled', 'count', 'child', 'children', 'colors', 'lookup'):
      self.assertIn('Objects.equals(%s, other.%s)' % (name, name), output)
      self.assertIn('.add("%s", %s)' % (name, name), output)
    self.assertIn('Objects.hash(\n          enabled,\n          count,', output)

  def test_empty_struct_equality_is_valid(self):
    output = render(self.structs['Empty'])
    self.assertIn('return true;', output)
    self.assertNotIn('return ;', output)

  def test_containers_and_nested_structs_keep_immutable_types(self):
    output = render(self.structs['Example'])
    for expected in ('ImmutableList<IChild>', 'ImmutableSet<Color>',
                     'ImmutableMap<String, String>', 'IChild.build(wrapped.getChild())',
                     'child.newBuilder()', 'import org.apache.aurora.gen.Color;'):
      self.assertIn(expected, output)
    self.assertEqual(['RED', 'BLUE'], self.structs['Color'].values)

  def test_union_keeps_discriminant_and_copy_cases(self):
    output = render(self.structs['Choice'])
    for expected in ('Choice._Fields.TEXT', 'Choice._Fields.CHILD',
                     'case TEXT:', 'case CHILD:', 'Choice.child((Child) value)',
                     'Objects.equals(setField, other.setField)'):
      self.assertIn(expected, output)

  def test_cli_metadata_inherits_methods_and_skips_enums(self):
    with tempfile.TemporaryDirectory() as tmp:
      destination = Path(tmp)
      thrift = destination / 'fixture.thrift'
      thrift.write_text(FIXTURE)
      generate(thrift, destination / 'output')
      java = destination / 'output/java/org/apache/aurora/scheduler/storage/entities'
      metadata = (java / 'AuroraAdminMetadata.java').read_text()
      self.assertIn('"put",', metadata)
      self.assertIn('"get",', metadata)
      self.assertIn('Example.class', metadata)
      self.assertIn('String.class', metadata)
      self.assertFalse((java / 'IColor.java').exists())

  def test_storage_without_services_generates_no_files(self):
    with tempfile.TemporaryDirectory() as tmp:
      self.assertEqual({}, generate(API.with_name('storage.thrift'), Path(tmp)))

  def test_api_matches_source_derived_golden_and_hash_seeds(self):
    golden = json.loads(Path(__file__).with_name('wrapper-api-sha256.json').read_text())
    with tempfile.TemporaryDirectory() as tmp:
      first = generate(API, Path(tmp) / 'first', '1')
      second = generate(API, Path(tmp) / 'second', '42')
      self.assertEqual(89, len(first))
      self.assertEqual(first, second)
      self.assertEqual(golden, first)


if __name__ == '__main__':
  unittest.main()
