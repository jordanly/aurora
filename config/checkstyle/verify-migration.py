#!/usr/bin/env python3
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Compare historical scope, import and class rules with their current migration."""
import argparse
import copy
import json
from pathlib import Path
import subprocess
import tempfile
import xml.etree.ElementTree as ET


FIXTURES = {
    'ImplicitPrivateClass': ('class ImplicitPrivateClass { private static class Nested {} }\n', 0),
    'ExplicitPrivateClass': ('class ExplicitPrivateClass { private ExplicitPrivateClass() {} }\n', 1),
    'NestedPrivateCtor': ('class NestedPrivateCtor { private static class Nested {\n'
                          ' private Nested() {} } }\n', 1),
    'InterfacePrivateCtor': ('interface InterfacePrivateCtor { class Nested {\n'
                             ' private Nested() {} } }\n', 0),
    'AnnotationPrivateCtor': ('@interface AnnotationPrivateCtor { class Nested {\n'
                              ' private Nested() {} } }\n', 0),
    'PublicCtor': ('class PublicCtor { public PublicCtor() {} }\n', 0),
    'ClassOnlyHash': ('class ClassOnlyHash { public int hashCode() { return 1; } }\n', 1),
    'ClassBothMethods': ('class ClassBothMethods { public int hashCode() { return 1; }\n'
                         ' public boolean equals(Object other) { return other == this; } }\n', 0),
    'Scopes': ('public class Scopes {\n public int a; protected int b; int c; private int d;\n'
               ' private static class PrivateNested { public int a; int b; }\n'
               ' public static class PublicNested { public int a; private int b; }\n'
               ' interface NestedInterface { int VALUE = 1; }\n}\n', 0),
    'GroupGaps': ('import java.io.File;\n\nimport java.util.List;\n\n'
                  'import javax.inject.Inject;\n\nimport org.example.Z;\n'
                  'class GroupGaps {}\n', 0),
    'MissingGap': ('import java.io.File;\nimport org.example.Z;\nclass MissingGap {}\n', 1),
    'WrongGroup': ('import org.example.Z;\n\nimport java.io.File;\nclass WrongGroup {}\n', 1),
    'WrongOrder': ('import java.util.List;\nimport java.io.File;\nclass WrongOrder {}\n', 1),
    'StaticGaps': ('import java.io.File;\n\nimport static java.util.Collections.emptyList;\n\n'
                   'import static java.util.Collections.emptySet;\n\n'
                   'import static org.example.Z.VALUE;\nclass StaticGaps {}\n', 0),
    'StaticWrongOrder': ('import static java.util.Collections.emptySet;\n'
                         'import static java.util.Collections.emptyList;\n'
                         'class StaticWrongOrder {}\n', 1),
    'StaticWrongGroup': ('import static org.example.Z.VALUE;\n\n'
                         'import static java.util.Collections.emptyList;\n'
                         'class StaticWrongGroup {}\n', 1),
    'StaticMissingGap': ('import static java.util.Collections.emptyList;\n'
                         'import static org.example.Z.VALUE;\nclass StaticMissingGap {}\n', 1),
    'StaticPlacement': ('import static java.util.Collections.emptyList;\n\n'
                        'import java.io.File;\nclass StaticPlacement {}\n', 1),
}


# Checkstyle 7.3 cannot parse records. Verify their Java-provided counterpart methods
# on the modern parser, alongside ordinary-class negative controls in both versions.
RECORD_FIXTURES = {
    'RecordOnlyHash': ('record RecordOnlyHash(int value) {\n'
                       ' public int hashCode() { return value; } }\n', 0),
    'RecordOnlyEquals': ('record RecordOnlyEquals(int value) {\n'
                         ' public boolean equals(Object other) { return other == this; } }\n', 0),
    'RecordNestedBadClass': ('record RecordNestedBadClass(int value) {\n'
                             ' static class Bad { public int hashCode() { return 1; } } }\n', 1),
}


def configuration(historical):
    root = ET.Element('module', name='Checker')
    walker = ET.SubElement(root, 'module', name='TreeWalker')
    source = ET.parse(Path(__file__).with_name('checkstyle.xml')).getroot()
    for name in ('JavadocVariable', 'ImportOrder', 'FinalClass', 'EqualsHashCode'):
        module = copy.deepcopy(source.find("./module[@name='TreeWalker']/module[@name='" + name + "']"))
        if historical:
            if name == 'JavadocVariable':
                module.clear()
                module.set('name', name)
                ET.SubElement(module, 'property', name='excludeScope', value='private')
            else:
                for property in list(module):
                    if property.get('name') in ('staticGroups', 'separatedStaticGroups',
                                               'sortStaticImportsAlphabetically'):
                        module.remove(property)
        walker.append(module)
    if not historical:
        for module in source.findall("./module[@name='TreeWalker']/module[@name='SuppressionXpathSingleFilter']"):
            walker.append(copy.deepcopy(module))
        root.append(copy.deepcopy(source.find("./module[@name='SuppressionSingleFilter']")))
    return ('<?xml version="1.0"?>\n<!DOCTYPE module PUBLIC '
            '"-//Puppy Crawl//DTD Check Configuration 1.3//EN" '
            '"http://www.puppycrawl.com/dtds/configuration_1_3.dtd">\n' + ET.tostring(root, encoding='unicode'))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--java', required=True)
    parser.add_argument('--old-classpath', required=True)
    parser.add_argument('--new-classpath', required=True)
    parser.add_argument('--receipt', type=Path, required=True)
    args = parser.parse_args()
    receipt = {}
    with tempfile.TemporaryDirectory(prefix='aurora-checkstyle-migration-') as temp:
        directory = Path(temp)
        inputs = {}
        for name, (source, _) in {**FIXTURES, **RECORD_FIXTURES}.items():
            path = directory / (name + '.java')
            path.write_text(source)
            inputs[name] = str(path)
        for label, classpath in (('7.3', args.old_classpath), ('14.1.0', args.new_classpath)):
            config = directory / ('config-' + label + '.xml')
            config.write_text(configuration(label == '7.3'))
            output = directory / ('result-' + label + '.xml')
            fixtures = FIXTURES if label == '7.3' else {**FIXTURES, **RECORD_FIXTURES}
            paths = [inputs[name] for name in fixtures]
            result = subprocess.run([args.java, '-cp', classpath,
                'com.puppycrawl.tools.checkstyle.Main', '-c', str(config), '-f', 'xml',
                '-o', str(output), *paths], capture_output=True, text=True, timeout=60)
            if not output.exists():
                raise AssertionError(label + ': ' + result.stdout + result.stderr)
            report = ET.parse(output).getroot()
            found = {Path(f.get('name')).stem: len(f.findall('error')) for f in report.findall('file')}
            expected = {name: count for name, (_, count) in fixtures.items()}
            if found != expected or result.returncode != sum(expected.values()):
                raise AssertionError(f'{label}: {found}; exit={result.returncode}; '
                                     + output.read_text() + result.stderr)
            receipt[label] = {'exitCode': result.returncode, 'violations': found}
    args.receipt.parent.mkdir(parents=True, exist_ok=True)
    args.receipt.write_text(json.dumps(receipt, indent=2, sort_keys=True) + '\n')
    print('Both Checkstyle versions preserve all ' + str(len(FIXTURES)) + ' shared fixture contracts; 3 additional record contracts passed.')


if __name__ == '__main__':
    main()
