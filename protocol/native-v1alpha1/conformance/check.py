#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Strict schema + bounded semantic/golden checks; no durable state machine."""
import argparse
import copy
import hashlib
import json
import pathlib
import re
import subprocess

from jsonschema import Draft202012Validator

ROOT = pathlib.Path(__file__).resolve().parents[1]


def pairs(items):
    result = {}
    for key, value in items:
        if key in result:
            raise ValueError('duplicate key: ' + key)
        result[key] = value
    return result


def reject_number(value):
    raise ValueError('only integer JSON numbers are allowed: ' + value)


def integer_token(value):
    if re.fullmatch(r'0|[1-9][0-9]*', value) is None or int(value) > 9007199254740991:
        raise ValueError('expected unsigned safe JSON integer: ' + value)
    return int(value)


def profile(value):
    # Apply the same printable-ASCII profile to keys and values before schema checks.
    if isinstance(value, str):
        if any(ord(char) < 32 or ord(char) > 126 for char in value):
            raise ValueError('non-printable ASCII string')
    elif isinstance(value, dict):
        for key, item in value.items():
            profile(key)
            profile(item)
    elif isinstance(value, list):
        for item in value:
            profile(item)


def read(path):
    value = json.loads(path.read_text(encoding='utf-8'), object_pairs_hook=pairs,
                       parse_int=integer_token, parse_float=reject_number,
                       parse_constant=reject_number)
    profile(value)
    return value


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'),
                      ensure_ascii=False).encode('ascii')


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def semantics(value):
    # Counters have a single lossless representation and an unsigned 64-bit bound.
    counters = {'revision', 'desiredRevision', 'schedulerEpoch', 'sequence', 'cursor',
                'generation', 'watermark', 'committedCursor'}
    def walk(node):
        if isinstance(node, dict):
            for key, item in node.items():
                if key in counters and int(item) > 18446744073709551615:
                    raise ValueError('counter overflow: ' + key)
                walk(item)
        elif isinstance(node, list):
            for item in node:
                walk(item)
    walk(value)
    process = value.get('template', value.get('assignment'))
    if process:
        if not isinstance(process["argv"][0], str) or not process["argv"][0].startswith("/"):
            raise ValueError("executable must be an absolute preinstalled path")
        if (process['resources']['memoryEnforcement'] == 'hard'
                and 'hard-memory' not in process['requiredCapabilities']):
            raise ValueError('hard memory needs explicit capability requirement')
        names = [port['name'] for port in process['ports']]
        if len(set(names)) != len(names):
            raise ValueError('duplicate port name')
        if value['kind'] == 'Run':
            sockets = [(port['network'], port['family'], port['protocol'], port['number'])
                       for port in process['ports']]
            if len(set(sockets)) != len(sockets):
                raise ValueError('duplicate socket')
        for argument in process['argv']:
            if isinstance(argument, dict) and argument['portRef'] not in names:
                raise ValueError('unknown argv port reference')
        if process['readiness']['kind'] == 'tcp' and process['readiness']['port'] not in names:
            raise ValueError('unresolved readiness port')
    if value['kind'] == 'Run' and value['identity']['process'] != process['process']:
        raise ValueError('assignment process differs from identity')
    if value['kind'] == 'Delivery':
        body = value['body']
        for key in ('cluster', 'incarnation'):
            if value['authority'][key] != body['identity'][key]:
                raise ValueError('authority scope differs from immutable command')
        if value['bodySha256'] != digest(body):
            raise ValueError('command body digest mismatch')
        semantics(body)


def admit_capabilities(process, advertised):
    missing = set(process["requiredCapabilities"]) - set(advertised)
    if missing:
        raise ValueError("missing agent capabilities: " + ", ".join(sorted(missing)))


def check():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('adapters', nargs='*', help='canonical-only adapter command')
    parser.add_argument('--validator', action='append', default=[],
                        help='full structural/semantic validator command, canonical stdout')
    arguments = parser.parse_args()
    schema = read(ROOT / 'schema.json')
    Draft202012Validator.check_schema(schema)
    validator = Draft202012Validator(schema)
    golden = read(ROOT / 'fixtures/golden.json')
    valid = sorted((ROOT / 'fixtures/valid').glob('*.json'))
    invalid = sorted((ROOT / 'fixtures/invalid').glob('*.json'))
    for path in valid:
        value = read(path)
        validator.validate(value)
        semantics(value)
        expected = golden[path.stem]
        assert canonical(value).decode('ascii') == expected['canonical'], path
        assert digest(value) == expected['sha256'], path
    hard = copy.deepcopy(read(ROOT / 'fixtures/valid/batch.json'))
    hard['template']['resources']['memoryEnforcement'] = 'hard'
    hard['template']['requiredCapabilities'] = ['hard-memory']
    validator.validate(hard)
    semantics(hard)
    try:
        admit_capabilities(hard['template'], [])
    except ValueError:
        pass
    else:
        raise AssertionError('hard memory admitted without agent support')
    admit_capabilities(hard['template'], ['hard-memory'])
    for path in invalid:
        try:
            value = read(path)
            validator.validate(value)
            semantics(value)
        except (ValueError, __import__('jsonschema').ValidationError):
            continue
        raise AssertionError('invalid fixture accepted: ' + str(path))
    original = read(ROOT / 'fixtures/valid/delivery.json')
    refreshed = read(ROOT / 'fixtures/valid/delivery-refreshed-authority.json')
    assert original['body'] == refreshed['body']
    assert original['bodySha256'] == refreshed['bodySha256']
    assert digest(original) != digest(refreshed)
    hashes = read(ROOT / 'fixtures/hashes.json')
    batch = read(ROOT / 'fixtures/valid/batch.json')
    assert digest(batch['template']) == hashes['batchTemplateSha256']
    assignment = {key: original['body'][key]
                  for key in ('identity', 'target', 'desiredRevision', 'templateSha256', 'assignment')}
    assert digest(assignment) == hashes['batchAssignmentSha256']
    service = read(ROOT / 'fixtures/valid/service.json')
    assert digest(service['template']) == hashes['serviceTemplateSha256']
    assignments = []
    for suffix in ('a', 'b'):
        run = read(ROOT / ('fixtures/valid/service-run-' + suffix + '.json'))
        assert run['templateSha256'] == digest(service['template'])
        resolved = copy.deepcopy(service['template'])
        ports = {port['name']: port['number'] for port in run['assignment']['ports']}
        resolved['argv'] = [str(ports[arg['portRef']]) if isinstance(arg, dict) else arg
                            for arg in resolved['argv']]
        resolved['ports'] = run['assignment']['ports']
        assert resolved == run['assignment']
        assignment = {key: run[key] for key in
                      ('identity', 'target', 'desiredRevision', 'templateSha256', 'assignment')}
        assignments.append(digest(assignment))
        assert assignments[-1] == hashes['serviceAssignment' + suffix.upper() + 'Sha256']
    assert assignments[0] != assignments[1]
    changed = copy.deepcopy(original['body'])
    changed['assignment']['env']['LANG'] = 'C.UTF-8'
    assert changed['command'] == original['body']['command']
    assert digest(changed) != original['bodySha256']
    # A wire example is not evidence of authentication or committed ACK correctness.
    assert set(golden) == {path.stem for path in valid}
    print('PASS: %d valid, %d invalid, %d canonical/hash goldens; authority refresh invariant'
          % (len(valid), len(invalid), len(golden)))
    parser_invalid = sorted((ROOT / 'fixtures/parser-invalid').glob('*.json'))
    for path in parser_invalid:
        try:
            read(path)
        except ValueError:
            continue
        raise AssertionError('Python parser/profile accepted invalid input: ' + str(path))
    print('PASS: %d Python parser/profile rejection vectors' % len(parser_invalid))
    # Optional standalone adapters print canonical document bytes followed by newline.
    # Invoke once per fixture, compare actual language parsing/encoding/hash semantics.
    for command in arguments.adapters + arguments.validator:
        import shlex
        for path in valid:
            encoded = subprocess.check_output(shlex.split(command) + [str(path)], timeout=10)
            assert encoded == canonical(read(path)) + b'\n', (command, path)
            assert hashlib.sha256(encoded[:-1]).hexdigest() == golden[path.stem]['sha256']
        for path in parser_invalid:
            result = subprocess.run(shlex.split(command) + [str(path)],
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=10)
            assert result.returncode != 0, ('parser accepted invalid input', command, path)
        print('PASS: %d decode/encode/hash and %d parser rejection vectors via %s'
              % (len(valid), len(parser_invalid), command))
        if command in arguments.validator:
            for path in invalid:
                result = subprocess.run(shlex.split(command) + [str(path)],
                                        stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=10)
                assert result.returncode != 0, ('validator accepted invalid input', command, path)
            print('PASS: %d structural/semantic rejection vectors via %s' % (len(invalid), command))


if __name__ == '__main__':
    check()
