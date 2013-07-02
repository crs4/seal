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
import ctypes as ct
import os
import re
import struct
import sys

import seal.lib.aligner.bwa.bwa_core as bwa
import testing_utilities as tu

class TestBwaCore(unittest.TestCase):
    def setUp(self):
        self.toy_data = [
            [ "name1", "CAAAT", "latte", "CAGAT", "acqua" ],
            [ "name2", "TTAGG", "beefy", "CTTGG", "caffe" ],
        ]
        self.toy_bwsa = bwa.build_bws_array(self.toy_data, qtype="fastq-illumina")

    def tearDown(self):
        for seq_array in self.toy_bwsa:
            bwa.free_seq(len(self.toy_data), seq_array)


    def test_bwa_init_sequences_no_trim(self):
        row_type = (ct.c_char_p * 3)
        pointers = (row_type*len(self.toy_data))()
        q_offset = bwa.Q_OFFSET["fastq-illumina"]
        for i, row in enumerate(self.toy_data):
            for j in xrange(3):
                pointers[i][j] = row[j]
        array = bwa.init_sequences( ct.cast(pointers, bwa.p_char_p), len(self.toy_data), q_offset, 0)

        resulting_qoffset = q_offset - bwa.Q_OFFSET["fastq-sanger"]
        for i, row in enumerate(self.toy_data):
            self.assertEqual(-1, array[i].tid)
            self.assertEqual(len(row[1]), array[i].len)
            self.assertEqual(len(row[1]), array[i].full_len)
            self.assertEqual(len(row[1]), array[i].clip_len)
            self.assertEqual(row[0], array[i].get_name())
            self.assertEqual(row[1][::-1], array[i].get_seq())
            self.assertEqual(row[2], ''.join( [chr(array[i].qual[k]+resulting_qoffset) for k in xrange(array[i].full_len)] ))

        bwa.free_seq(len(self.toy_data), array)

    def test_init_sequences_with_trimming(self):
        data = []
        data.append(("seq1", "NNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNN",
                "####################################################################################################"))
        row_type = (ct.c_char_p * 3)
        pointers = (row_type*len(data))()
        q_offset = bwa.Q_OFFSET["fastq-sanger"]
        for i, row in enumerate(data):
            for j in xrange(3):
                pointers[i][j] = row[j]
        array = bwa.init_sequences( ct.cast(pointers, bwa.p_char_p), len(data), q_offset, 15)
        self.assertEqual(35, array[0].clip_len)
        bwa.free_seq(len(data), array)


    def test_build_bws_array_simple(self):
        for i in xrange(len(self.toy_data)):
            self.assertEqual(self.toy_data[i][0], self.toy_bwsa[0][i].get_name())
            self.assertEqual(self.toy_data[i][0], self.toy_bwsa[1][i].get_name())

    def test_trimming_min_length(self):
        pair = ( # (name, r1, q1, r2, q2)
            ("seq_name",
             "NNNNNNNNNNNNACAGCTTTTGAGNGGGGCNCAGCCTCTTCCTTGTGGTGTTGCAGGACGGCAGGGAGTCAGTTGGGTTTCNACTCTTTAAGGACAGTGA",
             "####################################################################################################",
            "TTTTTATTTCTAGTGTTTATATTGATGAACAGAAATATACTGACATATTAACTTTTTTCATATAAATTTTTTCAAATTTTTGGTTAAGGCTTTTTCTGTC",
            "GGGGGF@EGGD;EEEGG??EDEEDAE==E?EEE??>B6C:=CDCBGFGFFFGFGGF=EF5<?=)=9B6?BAA-A:;;>;5*2*57AA>AA>AA>CA@CED"),
        )
        # The first sequence should have a clipped length of 35 at q=15.
        # 35 is the minimum trimmed sequence length.
        bwsa = bwa.build_bws_array(pair, qtype="fastq-sanger", trim_qual=15)
        self.assertEqual(100, bwsa[0][0].full_len)
        self.assertEqual(35, bwsa[0][0].clip_len)
        self.assertEqual(35, bwsa[0][0].len)
        self.assertEqual(100, bwsa[1][0].clip_len) # this one isn't clipped at q=15
        for seq_array in bwsa:
            bwa.free_seq(len(pair), seq_array)

    def test_build_bws_array_with_trim_qual(self):
        pair = ( # read, qual, trimmed length at qual=15
            ("pair1",
             "ATNCACCTGCCTTGGCCTCGCAAAGTGCTGGGATTACAGGCATGANNNNNNNNNNNNNNNNNNCNGTNNNNNNNNNNNNNTNCANATNNNNNNNNNNNTG",
             "==#@7::;;?FF?FFGGGFFGGGGE=CDEAE=AE?BBC??@###########################################################",
             "TTATAAGCACTGCACATAACTTTCTCCCTAATCTTTACAACAATCNNNTNNNCNNANNNNNCNCAGGNNNNCNNGNNNNNGNCANTGNNNNNNNNNNNGT",
             "EFDFFEEC?BEEAEE=A?CCE??EEAEEEEEECECEDEAC5@;7?#######################################################"
            ),
        )
        lengths = (42, 46)
        bwsa = bwa.build_bws_array(pair, qtype="fastq-sanger", trim_qual=15)
        self.assertEqual(lengths[0], bwsa[0][0].clip_len)
        self.assertEqual(lengths[1], bwsa[1][0].clip_len)
        for seq_array in bwsa:
            bwa.free_seq(len(pair), seq_array)

    def test_build_bws_array_with_trim_qual_zero(self):
        pair = ( # read, qual, trimmed length at qual=15
            ("pair1",
             "ATNCACCTGCCTTGGCCTCGCAAAGTGCTGGGATTACAGGCATGANNNNNNNNNNNNNNNNNNCNGTNNNNNNNNNNNNNTNCANATNNNNNNNNNNNTG",
             "==#@7::;;?FF?FFGGGFFGGGGE=CDEAE=AE?BBC??@###########################################################",
             "TTATAAGCACTGCACATAACTTTCTCCCTAATCTTTACAACAATCNNNTNNNCNNANNNNNCNCAGGNNNNCNNGNNNNNGNCANTGNNNNNNNNNNNGT",
             "EFDFFEEC?BEEAEE=A?CCE??EEAEEEEEECECEDEAC5@;7?#######################################################"
            ),
        )
        bwsa = bwa.build_bws_array(pair, qtype="fastq-sanger", trim_qual=0)
        self.assertEqual(100, bwsa[0][0].clip_len)
        self.assertEqual(100, bwsa[1][0].clip_len)
        for seq_array in bwsa:
            bwa.free_seq(len(pair), seq_array)

    def test_build_bws_array_with_illumina_q(self):
        pair = (
            ("pair1",
            "GCGTTAGTTTGTGGTGAAAGAAGCAAAATATTATGAATTTTGAAAATCTAGAGTCAATCTTCAAATTTCTTTTTATTCATTAATATCTCTGGCCTTTGTT",
            "TcLacaU_b_a\\aa`L\\a]X`dddYM^Z\\W]]W]WWbbYTZLZLZRI[VYQVWUU^b\\ccTc\\c^cYcYcZ^_ULUVUX[bY`^BBBBBBBBBBBBBBBB",
            "ATCAATCTAGGAGTTTGTATATACCACAAGAACAATGCTTGTCAAATCACTCACTTCTTTTCCCCTCTCTGGAGCCAGGCTTCGAGAAGTTGTATCCAGA",
            "B_a`aaTT__Q\\UQV^Z```\\VRKYQHUVIRLQQTTK]`^Z]]Y]_Y_Yb`da`^`aaac]eKdddfdefefecffffdaffffffefdedcffdeffff"),
        )
        lengths = (85,100)
        bwsa = bwa.build_bws_array(pair, qtype="fastq-illumina", trim_qual=15)
        self.assertEqual(lengths[0], bwsa[0][0].clip_len)
        self.assertEqual(lengths[1], bwsa[1][0].clip_len)
        for seq_array in bwsa:
            bwa.free_seq(len(pair), seq_array)

    def test_build_bws_array_without_qual_str(self):
        # make tuples substituting None for the quality strings in toy_data
        toy = map(lambda t: (t[0], t[1], None, t[3], None), self.toy_data)
        bwsa = bwa.build_bws_array(toy)
        for i in xrange(len(self.toy_data)):
            # verify name, sequence (which is reversed)
            self.assertEqual(self.toy_data[i][0], bwsa[0][i].get_name())
            self.assertEqual(self.toy_data[i][0], bwsa[1][i].get_name())
            self.assertEqual(self.toy_data[i][1], bwsa[0][i].get_seq()[::-1])
            self.assertEqual(self.toy_data[i][3], bwsa[1][i].get_seq()[::-1])
            # ensure that the returned quality string is empty
            self.assertEqual('', bwsa[0][i].get_qual_string())
            self.assertEqual('', bwsa[1][i].get_qual_string())
        for seq_array in bwsa:
            bwa.free_seq(len(toy), seq_array)

    def test_bwa_seq_t_get_seq(self):
        # sequence in bwa_seq_t is reversed at this stage
        self.assertEqual(self.toy_data[0][1], self.toy_bwsa[0][0].get_seq()[::-1])
        self.assertEqual(self.toy_data[0][3], self.toy_bwsa[1][0].get_seq()[::-1])
        self.assertEqual(self.toy_data[1][1], self.toy_bwsa[0][1].get_seq()[::-1])
        self.assertEqual(self.toy_data[1][3], self.toy_bwsa[1][1].get_seq()[::-1])

    def test_bwa_seq_t_get_rseq(self):
        # sequence in bwa_seq_t is reversed at this stage, but the reverse
        # complement sequence is itself reversed.  We can therefore compute the
        # current reverse complement by complementing each base in the original
        # DNA fragment.
        self.assertEqual(''.join(tu.complement[c] for c in self.toy_data[0][1]), self.toy_bwsa[0][0].get_rseq())
        self.assertEqual(''.join(tu.complement[c] for c in self.toy_data[0][3]), self.toy_bwsa[1][0].get_rseq())
        self.assertEqual(''.join(tu.complement[c] for c in self.toy_data[1][1]), self.toy_bwsa[0][1].get_rseq())
        self.assertEqual(''.join(tu.complement[c] for c in self.toy_data[1][3]), self.toy_bwsa[1][1].get_rseq())

    def test_bwa_seq_t_get_pos_5_raises(self):
        # on new, unaligned sequences it should raise
        self.assertRaises(ValueError, self.toy_bwsa[0][0].get_pos_5)

    def test_make_mmap_sa(self):
        try:
            tu.build_ref_index()
            bwa.make_suffix_arrays_for_mmap(tu.reference)

            sax_name = "%s.sax" % tu.reference
            rsax_name = "%s.rsax" % tu.reference
            self.assertTrue(os.path.exists(sax_name))
            self.assertTrue(os.path.exists(rsax_name))
            # check magic
            with open(sax_name) as f:
                s = f.read(4)
                self.assertEqual(4, len(s))
                self.assertEqual(bwa.mmap_magic().value, struct.unpack("=I", s)[0])

            with open(rsax_name) as f:
                s = f.read(4)
                self.assertEqual(4, len(s))
                self.assertEqual(bwa.mmap_magic().value, struct.unpack("=I", s)[0])
        finally:
            tu.remove_ref_index()

def suite():
    """Get a suite with all the tests from this module"""
    return unittest.TestLoader().loadTestsFromTestCase(TestBwaCore)

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())
