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

from seal.lib.event_monitor import EventMonitor
from seal.lib import TIME_TICK

import time
import logging
logging.basicConfig(level=logging.DEBUG)

class HadoopEventMonitor(EventMonitor):
    """
    Event monitor implementation to log information through the
    Hadoop context.
    """
    def __init__(self, event_class, logger, ctx):
        """
        Parameters:
        event_class:  the category in which all counters will be created.
        logger:  logger object to use
        ctx:  Hadoop context
        """
        self.event_class = event_class
        self.logger = logger
        self.ctx = ctx
        self.__start_times = {}
        self.__counters = {}

    def __get_counter(self, name, timing=False):
        if not self.__counters.has_key(name):
            if timing:
                self.add_counter(name, "TIME_" + name.upper() + " (ms)")
            else:
                self.add_counter(name, name.upper())
        return self.__counters[name]

    def __get_timing_counter(self, name):
        return self.__get_counter(name, timing=True)

    def start(self, s):
        self.__start_times[s] = time.time()

    def stop(self, s, write_status=True):
        delta = time.time() - self.__start_times[s]
        self.ctx.incrementCounter(self.__get_timing_counter(s), int(delta/TIME_TICK))
        if write_status:
            status = "done with %s (%.3f s)" % (s, delta)
            self.ctx.setStatus(status)
            self.log_info(status)

    def stop_batch(self, s, offset, n):
        delta = time.time() - self.__start_times[s]
        self.ctx.incrementCounter(self.__get_timing_counter(s), int(delta/TIME_TICK))
        status = "done with %s (offset=%d, n=%d) (%.3f s)" % (s, offset, n, delta)
        self.ctx.setStatus(status)
        self.log_info(status)

    def count(self, name, value=1):
        self.ctx.incrementCounter(self.__get_counter(name), value)

    def has_counter(self, name):
        return self.__counters.has_key(name)

    def add_counter(self, name, display_name=None):
        if self.__counters.has_key(name):
            raise ValueError("counter '%s' is already defined" % name)
        self.__counters[name] = self.ctx.getCounter(self.event_class, display_name if display_name else name.upper())

    #################################
    # logging methods
    #################################
    def new_status(self, status_msg):
        self.ctx.setStatus(status_msg)
        self.log_debug(status_msg)

    def log_debug(self, *args):
        self.logger.debug(*args)

    def log_info(self, *args):
        self.logger.info(*args)

    def log_warning(self, *args):
        self.logger.warning(*args)

    def log_error(self, *args):
        self.logger.error(*args)

    def log_critical(self, *args):
        self.logger.critical(*args)

