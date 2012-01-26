# Copyright (C) 2011-2012 CRS4.
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

import unittest
import re

from bl.mr.lib.emit_sam_link import EmitSamLink
from bl.mr.lib.hit_processor_chain_link import HitProcessorChainLink
from bl.mr.lib.hadoop_event_monitor import HadoopEventMonitor
from bl.mr.test_utils import map_context, SavingLogger
from bl.lib.seq.aligner.mapping import *

class TestEmitSamLink(unittest.TestCase):
	def setUp(self):
		self.map_ctx = map_context(None, None)
		self.count_group = "Test"
		self.logger = SavingLogger()
		self.monitor = HadoopEventMonitor(self.count_group, self.logger, self.map_ctx)
		self.emitter = EmitSamLink(self.map_ctx, self.monitor)
		# create two mappings, m1, m2.  We put them in self.pair
		# m1 has:
		#   name = first
		# 	tid = tid1
		# m2 has:
		#   name = second
		#   tid = tid2
		self.pair = [ SimpleMapping(), SimpleMapping() ]
		self.m1, self.m2 = self.pair
		self.m1.set_name("first")
		self.m1.tid = "tid1"
		self.m2.set_name("second")
		self.m2.tid = "tid2"

	def test_constructor_link(self):
		h = EmitSamLink(self.map_ctx, self.monitor)
		self.assertTrue(h.next_link is None)
		other = HitProcessorChainLink()
		h = EmitSamLink(self.map_ctx, self.monitor, other)
		self.assertEqual(other, h.next_link)

	def test_process(self):
		self.emitter.process(self.pair)
		self.assertEqual(["first", "second"], sorted(self.map_ctx.emitted.keys()))
		self.assertEqual(1, len(self.map_ctx.emitted["first"]))
		self.assertTrue(re.search("tid1", self.map_ctx.emitted["first"][0]))
		self.assertEqual(1, len(self.map_ctx.emitted["second"]))
		self.assertTrue(re.search("tid2", self.map_ctx.emitted["second"][0]))

	def test_emitted_type(self):
		self.emitter.process(self.pair)
		for k in self.map_ctx.emitted.keys():
			self.assertTrue(isinstance(k, str))
		for v in [ item for ary in self.map_ctx.emitted.values() for item in ary ]:
			self.assertTrue(isinstance(v, str))

	def test_first_null(self):
		self.pair[0] = None
		self.emitter.process(self.pair)
		self.assertEqual(["second"], self.map_ctx.emitted.keys())
		self.assertEqual(1, len(self.map_ctx.emitted["second"]))
		self.assertTrue(re.search("tid2", self.map_ctx.emitted["second"][0]))

	def test_second_null(self):
		self.pair[1] = None
		self.emitter.process(self.pair)
		self.assertEqual(["first"], self.map_ctx.emitted.keys())
		self.assertEqual(1, len(self.map_ctx.emitted["first"]))
		self.assertTrue(re.search("tid1", self.map_ctx.emitted["first"][0]))

	def test_forward_pair(self):
		class Receiver(object):
			def process(self, pair):
				self.received = pair

		receiver = Receiver()
		self.emitter.set_next(receiver)
		self.emitter.process(self.pair)
		self.assertEqual(self.pair, receiver.received)

def suite():
	return unittest.TestLoader().loadTestsFromTestCase(TestEmitSamLink)

if __name__ == '__main__':
	unittest.TextTestRunner(verbosity=2).run(suite())
