# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# http://www.apache.org/licenses/LICENSE-2.0
"""Regression checks for launcher input isolation and failed-command evidence."""
import argparse
import importlib.machinery
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

loader = importlib.machinery.SourceFileLoader('launcher_test', str(Path(__file__).resolve().parents[1] / 'native-launcher-check'))
spec = importlib.util.spec_from_loader(loader.name, loader)
launcher = importlib.util.module_from_spec(spec)
loader.exec_module(launcher)


class LauncherCheckTest(unittest.TestCase):
    def test_failed_input_audit_persists_failure_and_rejects_symlink(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / 'input'
            source.write_text('same bytes')
            compat = launcher.helper()
            immutable = {str(source): compat.sha(source)}
            with patch.object(compat, 'sha', side_effect=PermissionError('unreadable input')):
                with self.assertRaises(ValueError): launcher.finish(compat, root, immutable, {'ok': True})
            self.assertFalse(json.loads((root / 'launcher-check.json').read_text())['ok'])
            source.rename(root / 'target')
            source.symlink_to(root / 'target')
            with self.assertRaises(ValueError): launcher.finish(compat, root, immutable, {'ok': True})
            report = json.loads((root / 'launcher-check.json').read_text())
            self.assertFalse(report['originalInputsUnchanged'])

    def test_bad_output_refused_before_bundle_access_or_creation(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            bundle = root / 'bundle'
            bundle.mkdir()
            (root / 'link').symlink_to(bundle, target_is_directory=True)
            compat = launcher.helper()
            with patch.object(launcher, 'helper', return_value=compat), patch.object(compat, 'bundle') as read:
                for output in (Path('relative-output'), bundle / 'new', root / 'link' / 'new'):
                    with self.assertRaises(ValueError):
                        launcher.check(argparse.Namespace(bundle=bundle, output=output))
                read.assert_not_called()
            self.assertFalse((bundle / 'new').exists())

    def test_failed_exit_retains_output_and_observed_exit(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary)
            report = {'commands': {}}
            with self.assertRaises(ValueError):
                launcher.execute([sys.executable, '-c', 'import sys; print("failure-detail"); sys.exit(7)'],
                                 output, {'PATH': '/usr/bin:/bin'}, report, 'failed')
            self.assertEqual(7, report['commands']['failed']['exitCode'])
            self.assertEqual(b'failure-detail\n', (output / 'failed.stdout').read_bytes())
            self.assertIn('stdoutSha256', report['commands']['failed'])


if __name__ == '__main__':
    unittest.main()
