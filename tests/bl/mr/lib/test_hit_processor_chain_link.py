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
from bl.mr.lib.hit_processor_chain_link import HitProcessorChainLink

class TestHitProcessorChainLink(unittest.TestCase):
	def setUp(self):
		self.h1 = HitProcessorChainLink()
		self.h2 = HitProcessorChainLink()

	def test_default_constructor(self):
		self.assertTrue(self.h1.next_link is None)

	def test_constructor(self):
		h = HitProcessorChainLink(self.h1)
		self.assertEqual(self.h1, h.next_link)

	def test_set_next(self):
		self.assertTrue(self.h1.next_link is None)
		retval = self.h1.set_next(self.h2)
		self.assertEqual(self.h2, retval)
		self.assertEqual(self.h2, self.h1.next_link)

	def test_process_no_next(self):
		self.h1.process("test") # shouldn't raise

	def test_process_next(self):
		class Receiver(object):
			def process(self, pair):
				self.received = pair

		receiver = Receiver()
		self.h1.set_next(receiver)
		self.h1.process("test")
		self.assertEqual(receiver.received, "test")

def suite():
	return unittest.TestLoader().loadTestsFromTestCase(TestHitProcessorChainLink)

if __name__ == '__main__':
	unittest.TextTestRunner(verbosity=2).run(suite())
