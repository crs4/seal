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
import array

import seal.lib.io.protobuf_mapping as io
from seal.lib.aligner.mapping import SimpleMapping

import tseal.test_utils as tu

class TestProtobufSerialization(unittest.TestCase):

    def protected_compare_seq_maps(self, map_a, map_b):
        self.assertEqual(map_a.get_base_qualities(), map_b.get_base_qualities())
        self.assertEqual(map_a.get_cigar()        , map_b.get_cigar())
        self.assertEqual(map_a.flag         , map_b.flag)
        #self.assertEqual(map_a.isize       , map_b.isize)
        #self.assertEqual(map_a.mpos        , map_b.mpos)
        #self.assertEqual(map_a.mtid        , map_b.mtid)
        self.assertEqual(map_a.get_name()   , map_b.get_name())
        self.assertEqual(map_a.pos          , map_b.pos)
        self.assertEqual(map_a.qual         , map_b.qual)
        self.assertEqual([ t for t in map_a.each_tag() ]          , [ t for t in map_b.each_tag() ])
        self.assertEqual(map_a.tid           , map_b.tid)
        self.assertEqual(map_a.ref_id        , map_b.ref_id)
        self.assertEqual(map_a.get_seq_5(), map_b.get_seq_5())
        self.assertEqual(map_a.get_ascii_base_qual(), map_b.get_ascii_base_qual())
        self.assertEqual(map_a.get_cigar_str(), map_b.get_cigar_str())
        self.assertEqual(map_a.flag_string() , map_b.flag_string())

class TestProtobufSeqMapping(TestProtobufSerialization):

    def setUp(self):
        self.mapping = SimpleMapping()

    def test_simple(self):
        self.__try_and_compare_maps(self.mapping)

    def test_seq(self):
        small_seq = "AGCTNN"
        self.mapping.set_seq_5(small_seq)
        self.mapping.set_base_qualities(array.array("B", [ i + 25 for i in range(len(small_seq)) ]))
        self.__try_and_compare_maps(self.mapping)

    def test_cigar(self):
        self.mapping.set_cigar([ (100, "M") ])
        self.__try_and_compare_maps(self.mapping)
        self.mapping.set_cigar([ (80, "M"), (20, "S") ])
        self.__try_and_compare_maps(self.mapping)

    def test_flag(self):
        self.mapping.flag = 0x04
        self.__try_and_compare_maps(self.mapping)

    def test_name(self):
        self.mapping.set_name("CRESSIA_129:1:1:10029:122606#0/2")
        self.__try_and_compare_maps(self.mapping)

    def test_qual(self):
        self.mapping.qual = 60
        self.__try_and_compare_maps(self.mapping)

    def test_tags(self):
        self.mapping.add_tag( ("XC", 'i', 22) )
        self.mapping.add_tag( ("R2", 'Z', "bla") )
        self.__try_and_compare_maps(self.mapping)

    def test_pos(self):
        self.mapping.pos = 280781134
        self.__try_and_compare_maps(self.mapping)

    def test_tid(self):
        self.mapping.tid = "chr19"
        self.__try_and_compare_maps(self.mapping)

    def test_ref_id(self):
        self.mapping.ref_id = 12
        self.__try_and_compare_maps(self.mapping)
        self.mapping.ref_id = 0
        self.__try_and_compare_maps(self.mapping)

    def test_no_ref_id(self):
        self.mapping.ref_id = None
        self.__try_and_compare_maps(self.mapping)

    def __try_and_compare_maps(self, mapping):
        self.protected_compare_seq_maps(mapping, self.__pipe_map_through(mapping))

    def __pipe_map_through(self, mapping):
        message = io.serialize_seq(mapping)
        return io.unserialize_seq(message)


class TestProtobufPairMapping(TestProtobufSerialization):
    def setUp(self):
        self.pair = (SimpleMapping(), SimpleMapping())
        self.pair[0].set_read1(True)
        self.pair[1].set_read2(True)

        for m in self.pair:
            m.set_paired(True)
            m.set_name("CRESSIA_129:1:1:10605:12550#0")
            m.tid = "chr21"
            m.ref_id = 20
            m.qual = 60
            m.set_cigar( [(100, "M")] )
            m.mtid = "chr21"
            m.m_ref_id = 20

        m1,m2 = self.pair
        m1.pos = m2.mpos = 25839277
        m2.pos = m1.mpos = 25839109
        m1.isize = -268
        m2.isize = 268

    def test_good_pair(self):
        self.__try_and_compare_pair(self.pair)

    def test_isize_zero(self):
        for m in self.pair:
            m.isize = 0
        self.__try_and_compare_pair(self.pair)

    def test_isize_inverted_positions(self):
        # swap positions, invert insert size
        m1, m2 = self.pair
        m2.pos, m1.pos = m1.pos, m2.pos
        m2.mpos, m1.mpos = m1.mpos, m2.mpos
        m1.isize *= -1
        m2.isize *= -1
        self.__try_and_compare_pair(self.pair)

    def test_pair_wo_read1(self):
        pair = (None, self.pair[1])
        m2 = pair[1]
        m2.mtid = None
        m2.m_ref_id = None
        m2.mpos = 0
        m2.isize = 0
        self.__try_and_compare_pair(pair)

    def test_pair_wo_read2(self):
        pair = (self.pair[0], None)
        m1 = pair[0]
        m1.mtid = None
        m1.m_ref_id = None
        m1.mpos = 0
        m1.isize = 0
        self.__try_and_compare_pair(pair)

    def test_both_none(self):
        self.assertRaises(ValueError, io.serialize_pair, (None, None))

    def test_name(self):
        self.pair[0].set_name("my_name/1")
        self.pair[1].set_name("my_name/2")
        # the /1 and /2 will be removed by the serialization
        new_pair = self.__pipe_pair_through(self.pair)
        self.assertEqual("my_name", new_pair[0].get_name())
        self.assertEqual("my_name", new_pair[1].get_name())

    def __try_and_compare_pair(self, pair):
        self.__compare_pair(pair, self.__pipe_pair_through(pair))

    def __pipe_pair_through(self, pair):
        message = io.serialize_pair(pair)
        return io.unserialize_pair(message)

    def __compare_pair(self, pair1, pair2):
        self.assertEqual(len(pair1), len(pair2))
        for i in xrange(len(pair1)):
            if pair1[i] is None:
                self.assertEqual(pair1[i], pair2[i])
            else:
                self.protected_compare_seq_maps(pair1[i], pair2[i])

def suite():
    """Get a suite with all the tests from this module"""
    return tu.disabled_test_msg("TestProtobufSerialization and TestProtobufSeqMapping disabled")

    suite = unittest.TestSuite()
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestProtobufSeqMapping))
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestProtobufPairMapping))
    return suite

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())
