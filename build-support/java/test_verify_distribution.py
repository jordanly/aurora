# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import importlib.util
from pathlib import Path
import tempfile
import unittest


SPEC = importlib.util.spec_from_file_location(
    'verify_distribution', Path(__file__).with_name('verify-distribution.py'))
CHECKER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CHECKER)


class DistributionContractTest(unittest.TestCase):
    def test_original_usage_contract(self):
        result = {'exitCode': 1, 'output': 'Usage: example.Main [options]\n--help'}
        CHECKER.verify_help(result, 'example.Main', ['--help'])
        self.assertTrue(result['usageVerified'])

    def test_failure_exit_alone_is_insufficient(self):
        for code, output in (
                (1, 'failed'), (0, 'Usage: example.Main --help'),
                (2, 'Usage: example.Main --help'), (1, 'Usage: wrong.Main --help'),
                (1, 'Usage: example.Main'),
                (1, 'Usage: example.Main --help\nNoClassDefFoundError: missing'),
                (1, 'Usage: example.Main --help\nRecovering from SNAPSHOT')):
            with self.subTest(code=code, output=output):
                with self.assertRaises(CHECKER.VerificationError):
                    CHECKER.verify_help({'exitCode': code, 'output': output},
                                        'example.Main', ['--help'])

    def test_packaged_classpath(self):
        with tempfile.TemporaryDirectory() as directory:
            distribution = Path(directory)
            (distribution / 'lib').mkdir()
            (distribution / 'lib/app.jar').touch()
            script = 'CLASSPATH=$APP_HOME/lib/app.jar\n'
            self.assertEqual(['app.jar'], CHECKER.verify_classpath(script, distribution))
            for invalid in (script.rstrip() + ':$APP_HOME/lib/classes\n',
                            script.rstrip() + ':$APP_HOME/lib/app.jar\n',
                            'CLASSPATH=$APP_HOME/lib/missing.jar\n'):
                with self.subTest(script=invalid):
                    with self.assertRaises(CHECKER.VerificationError):
                        CHECKER.verify_classpath(invalid, distribution)
            (distribution / 'lib/extra.jar').touch()
            with self.assertRaises(CHECKER.VerificationError):
                CHECKER.verify_classpath(script, distribution)
            (distribution / 'lib/extra.jar').unlink()
            (distribution / 'lib/Generated.class').touch()
            with self.assertRaises(CHECKER.VerificationError):
                CHECKER.verify_classpath(script, distribution)


if __name__ == '__main__':
    unittest.main()
