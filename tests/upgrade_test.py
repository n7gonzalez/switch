# Copyright 2016 The Switch Authors. All rights reserved.
# Licensed under the Apache License, Version 2, which is in the LICENSE file.

import filecmp
import os
import os.path
import shutil
import sys
import tempfile
import unittest

import switch_mod.solve
import switch_mod.utilities
from switch_mod.upgrade import upgrade_inputs
from .examples_test import get_expectation_path, read_file, write_file, TOP_DIR
import switch_mod.upgrade.upgrade_2_0_0b1 as upgrade_2_0_0b1

UPDATE_EXPECTATIONS = False

def find_example_dirs(path):
    for dirpath, dirnames, filenames in os.walk(path):
        for dirname in dirnames:
            path = os.path.join(dirpath, dirname)
            if os.path.exists(os.path.join(path, 'inputs', 'modules.txt')):
                yield path

def make_test(example_dir):
    def test_upgrade():
        temp_dir = tempfile.mkdtemp(prefix='switch_test_')
        example_name = os.path.basename(os.path.normpath(example_dir))
        upgrade_dir = os.path.join(temp_dir, example_name)
        shutil.copytree(example_dir, upgrade_dir, ignore=shutil.ignore_patterns('outputs'))
        upgrade_dir_inputs = os.path.join(upgrade_dir, 'inputs')
        upgrade_dir_outputs = os.path.join(upgrade_dir, 'outputs')
        upgrade_inputs(upgrade_dir_inputs)
        try:
            # Custom python modules may be in the example's working directory
            sys.path.append(upgrade_dir)
            switch_mod.solve.main([
                '--inputs-dir', upgrade_dir_inputs,
                '--outputs-dir', upgrade_dir_outputs])
            total_cost = read_file(os.path.join(upgrade_dir_outputs, 'total_cost.txt'))
        finally:
            sys.path.remove(upgrade_dir)
            shutil.rmtree(temp_dir)
        expectation_file = get_expectation_path(example_dir)
        if UPDATE_EXPECTATIONS:
            write_file(expectation_file, total_cost)
        else:
            expected = float(read_file(expectation_file))
            actual = float(total_cost)
            if not switch_mod.utilities.approx_equal(expected, actual,
                                                     tolerance=0.0001):
                raise AssertionError(
                    'Mismatch for total_cost (the objective function value):\n'
                    'Expected value:  {}\n'
                    'Actual value:    {}\n'
                    'Run "python -m tests.upgrade_test.py --update" to'
                    'update the expectations if this change is expected.'
                    .format(expected, actual))

    name = os.path.basename(os.path.normpath(example_dir))
    return unittest.FunctionTestCase(
        test_upgrade, description='Test Upgrade Example: %s' % name)

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    for example_dir in find_example_dirs(os.path.join(TOP_DIR, 'tests', 'upgrade_dat')):
        if get_expectation_path(example_dir):
            suite.addTest(make_test(example_dir))
    return suite


if __name__ == '__main__':
    if sys.argv[1:2] == ['--update']:
        UPDATE_EXPECTATIONS = True
        sys.argv.pop(1)
    unittest.main()
