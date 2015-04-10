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

from seal.lib.io.sam_formatter import SamFormatter
from seal.lib.aligner.mapping import SimpleMapping

import tseal.test_utils as tu

class TestSamFormatter(unittest.TestCase):

    def setUp(self):
        self.mapping = SimpleMapping()
        self.f = SamFormatter()

    def test_default(self):
        self.assertEqual("\t".join( map(str, ['', 0, "*", 0, 0, "*", "*", 0, 0, "", "" ])), self.__do_format())

    def test_name(self):
        self.mapping.set_name("my name")
        self.assertEqual("\t".join( map(str, ["my name", 0, "*", 0, 0, "*", "*", 0, 0, "", "" ])), self.__do_format())

    def test_flag(self):
        self.mapping.flag = 163
        self.assertEqual("\t".join( map(str, ['', 163, "*", 0, 0, "*", "*", 0, 0, "", "" ])), self.__do_format())

    def test_reference(self):
        self.mapping.tid = "chr21"
        self.assertEqual("\t".join( map(str, ['', 0, "chr21", 0, 0, "*", "*", 0, 0, "", "" ])), self.__do_format())

    def test_mate_reference(self):
        self.mapping.mtid = "chr21"
        self.assertEqual("\t".join( map(str, ['', 0, "*", 0, 0, "*", "chr21", 0, 0, "", "" ])), self.__do_format())
        self.mapping.mtid = "="
        self.assertEqual("\t".join( map(str, ['', 0, "*", 0, 0, "*", "=", 0, 0, "", "" ])), self.__do_format())

    def test_pos(self):
        self.mapping.pos = 1234
        self.assertEqual("\t".join( map(str, ['', 0, "*", 1234, 0, "*", "*", 0, 0, "", "" ])), self.__do_format())

    def test_mate_pos(self):
        self.mapping.mpos = 1234
        self.assertEqual("\t".join( map(str, ['', 0, "*", 0, 0, "*", "*", 1234, 0, "", "" ])), self.__do_format())

    def test_mapq(self):
        self.mapping.qual = 55
        self.assertEqual("\t".join( map(str, ['', 0, "*", 0, 55, "*", "*", 0, 0, "", "" ])), self.__do_format())

    def test_cigar(self):
        self.mapping.set_cigar([ (100, 'M') ])
        self.assertEqual("\t".join( map(str, ['', 0, "*", 0, 0, "100M", "*", 0, 0, "", "" ])), self.__do_format())
        self.mapping.set_cigar([ (80, 'M'), (20, 'S') ])
        self.assertEqual("\t".join( map(str, ['', 0, "*", 0, 0, "80M20S", "*", 0, 0, "", "" ])), self.__do_format())

    def test_isize(self):
        self.mapping.isize = 273
        self.assertEqual("\t".join( map(str, ['', 0, "*", 0, 0, "*", "*", 0, 273, "", "" ])), self.__do_format())
        self.mapping.isize = -273
        self.assertEqual("\t".join( map(str, ['', 0, "*", 0, 0, "*", "*", 0, -273, "", "" ])), self.__do_format())

    def test_seq(self):
        self.mapping.set_seq_5("AGCTNN")
        self.assertEqual("\t".join( map(str, ['', 0, "*", 0, 0, "*", "*", 0, 0, "AGCTNN", "" ])), self.__do_format())

    def test_qual(self):
        bq = array.array('B', [22, 33, 45])
        self.mapping.set_base_qualities(bq)
        self.assertEqual("\t".join( map(str, ['', 0, "*", 0, 0, "*", "*", 0, 0, "", "7BN" ])), self.__do_format())

    def test_tags(self):
        self.mapping.add_tag( ("XC", 'i', 18) )
        self.mapping.add_tag( ("HI", 'Z', "llo") )
        self.assertEqual("\t".join( map(str, ['', 0, "*", 0, 0, "*", "*", 0, 0, "", "", "XC:i:18", "HI:Z:llo" ])), self.__do_format())

    def __do_format(self):
        return self.f.format(self.mapping)

def suite():
    """Get a suite with all the tests from this module"""
    return tu.disabled_test_msg("TestSamFormatter disabled because SamFormatter is obsolete")
    return unittest.TestLoader().loadTestsFromTestCase(TestSamFormatter)

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())
