#!/usr/bin/env python

import unittest

start_dir = "tests"
pattern = "*.py"
top_level_dir = "."

suite = unittest.TestLoader().discover(start_dir, pattern, top_level_dir)
unittest.TextTestRunner(verbosity=2).run(suite)
