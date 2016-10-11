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

import unittest

from seal.lib.mr.hit_processor_chain_link import HitProcessorChainLink
from seal.lib.mr.filter_link import RapiFilterLink
from seal.lib.aligner.mapping import SimpleMapping
from seal.lib.standard_monitor import StandardMonitor
from seal.lib.mr.test_utils import SavingLogger
import tseal.test_utils as tu

class TestRapiFilterLink(unittest.TestCase):

    # mini object to let us peek at what the filter forwards to the next link
    class Receiver(HitProcessorChainLink):
        def __init__(self, *args):
            super(TestRapiFilterLink.Receiver, self).__init__(*args)
            self.orig_received = None
            self.aln_received = None

        def process(self, orig_pair, aln_pair):
            self.orig_received = orig_pair
            self.aln_received = aln_pair

    def setUp(self):
        self.monitor = StandardMonitor(SavingLogger())
        self.filter = RapiFilterLink(self.monitor)
        self.receiver = self.filter.set_next(type(self).Receiver())
        # create two mappings, m1, m2.  We put them in self.pair
        # m1 has:
        #   name = first
        # 	tid = tid1
        # m2 has:
        #   name = second
        #   tid = tid2
        self.orig_pair = {}
        self.pair = [ tu.FakeRead(id="first"), tu.FakeRead(id="second") ]
        self.m1, self.m2 = self.pair
        self.m1.alignments.append(
                tu.FakeAlignment(
                    contig=tu.FakeContig(name="tid1"),
                    mapped=True,
                    mapq=50))
        self.m2.alignments.append(
                tu.FakeAlignment(
                    contig=tu.FakeContig(name="tid2"),
                    mapped=True,
                    mapq=30))

    def test_constructor_link(self):
        h = RapiFilterLink(self.monitor)
        self.assertTrue(h.next_link is None)
        other = HitProcessorChainLink()
        h = RapiFilterLink(self.monitor, other)
        self.assertEqual(other, h.next_link)

    def test_filter_none(self):
        self.filter.process(self.orig_pair, self.pair)
        self.assertFalse(self.receiver.aln_received is None)
        self.assertEqual(self.m1.id, self.receiver.aln_received[0].id)
        self.assertEqual(self.m2.id, self.receiver.aln_received[1].id)
        # ensure there are no counters (i.e. nothing was filtered)
        self.assertFalse( [ c for c in self.monitor.each_counter() ] )

    def test_filter_one(self):
        self.filter.min_hit_quality = self.m2.mapq + 1
        self.filter.process(self.orig_pair, self.pair)
        self.assertFalse(self.receiver.aln_received is None)
        self.assertTrue(self.receiver.aln_received[1] is None)
        self.assertEqual(self.m1.id, self.receiver.aln_received[0].id)
        counter_list = [ c for c in self.monitor.each_counter() ]
        self.assertTrue(len(counter_list) == 1)
        name, value = counter_list[0]
        self.assertEqual("reads filtered: low quality", name)
        self.assertEqual(1, value)


    def test_filter_two(self):
        self.filter.min_hit_quality = self.m1.mapq + 1
        self.filter.process(self.orig_pair, self.pair)
        self.assertTrue(self.receiver.aln_received is None)
        counter_list = [ c for c in self.monitor.each_counter() ]
        self.assertTrue(len(counter_list) == 1)
        name, value = counter_list[0]
        self.assertEqual("reads filtered: low quality", name)
        self.assertEqual(2, value)

    def test_without_next_link(self):
        h = RapiFilterLink(self.monitor)
        h.process(self.orig_pair, self.pair) # shouldn't raise

    def test_filter_unmapped_1(self):
        self.m1.alignments[0].mapped = False
        self.m1.alignments[0].mapq = 0
        self.filter.remove_unmapped = True
        self.filter.process(self.orig_pair, self.pair)
        self.assertFalse(self.receiver.aln_received is None)
        self.assertTrue(self.receiver.aln_received[0] is None)
        self.assertFalse(self.receiver.aln_received[1] is None)
        counter_list = [ c for c in self.monitor.each_counter() ]
        self.assertTrue(len(counter_list) == 1)
        name, value = counter_list[0]
        self.assertEqual("reads filtered: unmapped", name)
        self.assertEqual(1, value)


def suite():
    return unittest.TestLoader().loadTestsFromTestCase(TestRapiFilterLink)

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())
