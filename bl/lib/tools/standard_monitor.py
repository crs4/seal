# Copyright (C) 2011 CRS4.
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

from event_monitor import EventMonitor
import time

class StandardMonitor(EventMonitor):
	"""
	Implementation of EventMonitor that writes to a logger
	and implements counting and timing internally.
	"""
	def __init__(self, logger):
		self.__output = logger
		self.__counters = {}
		self.__start_times = {}

	def start(self, event_name):
		self.__start_times[event_name] = time.time()

	def stop(self, event_name):
		delta = time.time() - self.__start_times[event_name]
		self.__output.info( "done with %s (%.3f s)" % (event_name, delta))

	def count(self, event_name, value=1):
		if not self.__counters.has_key(event_name):
			self.__counters[event_name] = value
		else:
			self.__counters[event_name] += value

	def has_counter(self, event_name):
		self.__counters.has_key(event_name)

	def add_counter(self, event_name, display_name=None):
		if self.__counters.has_key(name):
			raise ValueError("counter '%s' is already defined" % name)
		self.__counters[event_name] = 0

	def new_status(self, status_msg):
		self.__output.info(status_msg)

	def log_debug(self, *args):
		self.__output.debug(*args)

	def log_info(self, *args):
		self.__output.info(*args)

	def log_warning(self, *args):
		self.__output.warning(*args)

	def log_error(self, *args):
		self.__output.error(*args)

	def log_critical(self, *args):
		self.__output.critical(*args)

	def each_counter(self):
		for k,v in self.__counters.items():
			yield k,v
