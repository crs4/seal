# Copyright (C) 2011-2016 CRS4.
#
# This file is part of Seal.
#
# Seal is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Seal is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Seal.  If not, see <http://www.gnu.org/licenses/>.

import re
import time
import unittest

from seal.lib.mr.hadoop_event_monitor import *
from seal.lib.mr.test_utils import map_context, SavingLogger

class TestHadoopEventMonitor(unittest.TestCase):

    def setUp(self):
        self.map_ctx = map_context(None, None)
        self.logger = SavingLogger()
        self.count_group = "Test"
        self.monitor = HadoopEventMonitor(self.count_group, self.logger, self.map_ctx)

    def test_start_stop(self):
        self.monitor.start("test_start_stop")
        time.sleep(0.5)
        self.monitor.stop("test_start_stop")
        self.assertTrue( re.match('done with test_start_stop.*', self.logger.contents()) is not None)
        self.assertTrue( re.match('done with test_start_stop.*', self.map_ctx.status_messages[-1]) is not None)
        # extract the measured time and ensure it's > 0
        m = re.search("done with test_start_stop\s+\(\s*(-?[0-9.]+\s*)", self.map_ctx.status_messages[-1])
        self.assertTrue(m is not None, "couldn't extract time from timing status message")
        self.assertTrue(float(m.group(1)) > 0)

    def test_time_block(self):
        with self.monitor.time_block("test_time_block"):
            # in block
            time.sleep(0.5)

        self.assertTrue( re.match('done with test_time_block.*', self.logger.contents()) is not None)
        self.assertTrue( re.match('done with test_time_block.*', self.map_ctx.status_messages[-1]) is not None)
        # extract the measured time and ensure it's > 0
        m = re.search("done with test_time_block\s+\(\s*(-?[0-9.]+\s*)", self.map_ctx.status_messages[-1])
        self.assertTrue(m is not None, "couldn't extract time from timing status message")
        self.assertTrue(float(m.group(1)) > 0)

    def test_count_default(self):
        # this corresponds to how the mock context creates the counter ids,
        # and the event monitor makes the name uppercase.
        counter_id = "%s:%s" % (self.count_group, "TEST_COUNT")
        self.monitor.count("test_count")
        self.assertEqual(1, self.map_ctx.counters[counter_id])
        self.monitor.count("test_count")
        self.assertEqual(2, self.map_ctx.counters[counter_id])

    def test_count_nondefault_value(self):
        counter_id = "%s:%s" % (self.count_group, "TEST_COUNT")
        self.monitor.count("test_count", 8)
        self.assertEqual(8, self.map_ctx.counters[counter_id])
        self.monitor.count("test_count", 5)
        self.assertEqual(13, self.map_ctx.counters[counter_id])

    def test_count_default_preexisting(self):
        counter_id = "%s:%s" % (self.count_group, "TEST_COUNT")
        self.monitor.add_counter("test_count")
        self.monitor.count("test_count")
        self.assertEqual(1, self.map_ctx.counters[counter_id])
        self.monitor.count("test_count")
        self.assertEqual(2, self.map_ctx.counters[counter_id])

    def test_has_counter_add_counter(self):
        self.assertFalse( self.monitor.has_counter("test_has_counter") )
        self.monitor.count("test_has_counter")
        self.assertTrue( self.monitor.has_counter("test_has_counter") )
        self.assertFalse( self.monitor.has_counter("other_counter") )
        self.monitor.add_counter("other_counter")
        self.assertTrue( self.monitor.has_counter("other_counter") )

    def test_new_status(self):
        self.assertEqual(0, len(self.map_ctx.status_messages))
        self.monitor.new_status("test_new_status")
        self.assertEqual(1, len(self.map_ctx.status_messages))
        self.assertEqual("test_new_status", self.map_ctx.status_messages[-1])
        self.assertEqual("test_new_status\n", self.logger.contents())

    def test_new_status_lower_log_level(self):
        self.logger.log_level = SavingLogger.ERROR
        self.assertEqual(0, len(self.map_ctx.status_messages))
        self.monitor.new_status("test_new_status")
        self.assertEqual(1, len(self.map_ctx.status_messages))
        self.assertEqual("test_new_status", self.map_ctx.status_messages[-1])
        self.assertEqual("", self.logger.contents())

    def test_log_debug(self):
        self.logger.log_level = SavingLogger.DEBUG
        self.monitor.log_debug("debug")
        self.assertEqual("debug\n", self.logger.contents())
        self.monitor.log_info("info")
        self.assertEqual("debug\ninfo\n", self.logger.contents())

    def test_log_info(self):
        self.logger.log_level = SavingLogger.INFO
        self.monitor.log_debug("debug")
        self.assertEqual("", self.logger.contents())
        self.monitor.log_info("info")
        self.assertEqual("info\n", self.logger.contents())
        self.monitor.log_error("error")
        self.assertEqual("info\nerror\n", self.logger.contents())

    def test_log_warning(self):
        self.logger.log_level = SavingLogger.WARNING
        self.monitor.log_info("info")
        self.assertEqual("", self.logger.contents())
        self.monitor.log_warning("warning")
        self.assertEqual("warning\n", self.logger.contents())
        self.monitor.log_error("error")
        self.assertEqual("warning\nerror\n", self.logger.contents())

    def test_log_error(self):
        self.logger.log_level = SavingLogger.ERROR
        self.monitor.log_info("info")
        self.assertEqual("", self.logger.contents())
        self.monitor.log_error("error")
        self.assertEqual("error\n", self.logger.contents())
        self.monitor.log_critical("critical")
        self.assertEqual("error\ncritical\n", self.logger.contents())

    def test_log_critical(self):
        self.logger.log_level = SavingLogger.CRITICAL
        self.monitor.log_info("info")
        self.assertEqual("", self.logger.contents())
        self.monitor.log_critical("critical")
        self.assertEqual("critical\n", self.logger.contents())

def suite():
    """Get a suite with all the tests from this module"""
    return unittest.TestLoader().loadTestsFromTestCase(TestHadoopEventMonitor)

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())
