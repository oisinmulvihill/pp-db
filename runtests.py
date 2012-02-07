#!/usr/bin/env python
"""
Use nosetests to run the unit tests for this project.
"""
import sys

import nose
import logging

sys.path.insert(0, "./lib")

# Set up basic logging to prevent missing logger errors:
#
level = logging.CRITICAL

log = logging.getLogger()
hdlr = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
log.addHandler(hdlr)
log.setLevel(level)


result = nose.core.TestProgram(env=dict(
    NOSE_WHERE="lib/commondb"
)).success

nose.result.end_capture()

