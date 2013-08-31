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

import os
import unittest

from seal.lib.io.sam_formatter import SamFormatter
from seal.lib.aligner.bwa.bwa_aligner import BwaAligner

class MappingsCollector(object):
  def __init__(self):
    self.mappings = []
    self.formatter = SamFormatter()

  def process(self, pair):
    self.mappings.extend(map(self.formatter.format, pair))

class TestBwaAligner(unittest.TestCase):

  def setUp(self):
    self.aligner = BwaAligner()
    test_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
    self.aligner.reference = os.path.join(test_dir, 'seal', 'mini_ref_fixture', 'mini_ref.fasta')
    self.aligner.hit_visitor = MappingsCollector()
    self.aligner.qformat = "fastq-sanger"

  def test_pair(self):
    pair = (
      "HWI-ST301L:236:C0EJ5ACXX:1:1101:18292:2904",
      "GGGAGGTGTTAGGGACAAGCCTGGAGGCAGCATGCGTCACTCCCATGCAGAGTCCATTGGCCAATGCTGGCTCCGATGGCCACATCTCACTCCAGGGGCAG",
      "?@@B?<=AADFCFH@FB?EFEGAAFGEEGEGHCGEGIGH?B?CGEFHGIIGAEEEEHEAEEEH937;;@3=;>@8;?8;9A:<A#################",
      "AATAGAATGTAATATAATATATGTAAAACACCAGGTGCCTAACCTGGCACAGAGCAGGAGGGCTAAGCATGACATCCAGCACGTGGTCAGTGGAATCCAGT",
      "@@@DFDDDBHDD<EHEHIFEEB<IHIEGHDFEH?B:CBEHICEGCGGIIGFGCFCE@FAFEGAAGHIIHF;A?DBDFB);@@35;?,;@35(:5:ACCC<>")
    results = self._align_pair(pair)
    self.assertEqual(
     "HWI-ST301L:236:C0EJ5ACXX:1:1101:18292:2904	133	chr1	24762	0	*	=	24762	0	AATAGAATGTAATATAATATATGTAAAACACCAGGTGCCTAACCTGGCACAGAGCAGGAGGGCTAAGCATGACATCCAGCACGTGGTCAGTGGAATCCAGT	@@@DFDDDBHDD<EHEHIFEEB<IHIEGHDFEH?B:CBEHICEGCGGIIGFGCFCE@FAFEGAAGHIIHF;A?DBDFB);@@35;?,;@35(:5:ACCC<>",
     results[0])
    self.assertEqual(
      "HWI-ST301L:236:C0EJ5ACXX:1:1101:18292:2904	73	chr1	24762	37	101M	=	24762	0	GGGAGGTGTTAGGGACAAGCCTGGAGGCAGCATGCGTCACTCCCATGCAGAGTCCATTGGCCAATGCTGGCTCCGATGGCCACATCTCACTCCAGGGGCAG	?@@B?<=AADFCFH@FB?EFEGAAFGEEGEGHCGEGIGH?B?CGEFHGIIGAEEEEHEAEEEH937;;@3=;>@8;?8;9A:<A#################	XT:A:U	NM:i:2	SM:i:37	AM:i:0	X0:i:1	X1:i:0	XM:i:2	XO:i:0	XG:i:0	MD:Z:7T83G9",
      results[1])

  def test_easy_align(self):
    pair = (
      "HWI-ST301L:236:C0EJ5ACXX:1:1101:18292:2904",
      # pos: 361
      "TAACCCTAACCCCTAACCCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAAC",
      "?@@B?<=AADFCFH@FB?EFEGAAFGEEGEGHCGEGIGH?B?CGEFHGIIGAEEEEHEAE",
      # pos: 541
      "AACGCAGCTCCGCCCTCGCGGTGCTCTCCGGGTCTGTGCTGAGGAGAACGCAACTCCGCC",
      "@@@DFDDDBHDD<EHEHIFEEB<IHIEGHDFEH?B:CBEHICEGCGGIIGFGCFCE@FAF")
    results = self._align_pair(pair)
    self.assertEqual(
      ["HWI-ST301L:236:C0EJ5ACXX:1:1101:18292:2904",
        "129","chr1","541","37","60M","=","361","-180",
        "AACGCAGCTCCGCCCTCGCGGTGCTCTCCGGGTCTGTGCTGAGGAGAACGCAACTCCGCC",
        "@@@DFDDDBHDD<EHEHIFEEB<IHIEGHDFEH?B:CBEHICEGCGGIIGFGCFCE@FAF"],
      self._get_sam_fields(results[0]))
    self.assertEqual(
      ["HWI-ST301L:236:C0EJ5ACXX:1:1101:18292:2904",
        "65","chr1","361","37","60M","=","541","180",
        "TAACCCTAACCCCTAACCCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAAC",
        "?@@B?<=AADFCFH@FB?EFEGAAFGEEGEGHCGEGIGH?B?CGEFHGIIGAEEEEHEAE"],
      self._get_sam_fields(results[1]))

  def test_align_pair_of_rev_complements(self):
    # These are the same reads as the above, but reversed
    # and complemented. Remember that the above were taken
    # directly from the mini reference sequence.
    pair = (
      "HWI-ST301L:236:C0EJ5ACXX:1:1101:18292:2904",
      # pos: 361
      "GTTAGGGTTAGGGTTAGGGTTAGGGTTAGGGTTAGGGTTAGGGGTTAGGGGTTAGGGTTA",
      "EAEHEEEEAGIIGHFEGC?B?HGIGEGCHGEGEEGFAAGEFE?BF@HFCFDAA=<?B@@?",
      # pos: 541
      "GGCGGAGTTGCGTTCTCCTCAGCACAGACCCGGAGAGCACCGCGAGGGCGGAGCTGCGTT",
      "FAF@ECFCGFGIIGGCGECIHEBC:B?HEFDHGEIHI<BEEFIHEHE<DDHBDDDFD@@@")
    results = self._align_pair(pair)
    self.assertEqual(
      ['HWI-ST301L:236:C0EJ5ACXX:1:1101:18292:2904',
        '113','chr1','361','37','60M','=','541','180',
        'TAACCCTAACCCCTAACCCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAAC',
        '?@@B?<=AADFCFH@FB?EFEGAAFGEEGEGHCGEGIGH?B?CGEFHGIIGAEEEEHEAE'],
      self._get_sam_fields(results[0])
    )
    self.assertEqual(
      ['HWI-ST301L:236:C0EJ5ACXX:1:1101:18292:2904',
        '177','chr1','541','37','60M','=','361','-180',
        'AACGCAGCTCCGCCCTCGCGGTGCTCTCCGGGTCTGTGCTGAGGAGAACGCAACTCCGCC',
        '@@@DFDDDBHDD<EHEHIFEEB<IHIEGHDFEH?B:CBEHICEGCGGIIGFGCFCE@FAF'],
      self._get_sam_fields(results[1])
    )


  def _align_pair(self, pair):
    self.aligner.load_pair_record(pair)
    self.aligner.run_alignment()
    self.aligner.clear_batch()
    return sorted(self.aligner.hit_visitor.mappings)

  @staticmethod
  def _get_sam_fields(sam_record):
    return sam_record.split('\t')[0:11]


def suite():
  """Get a suite with all the tests from this module"""
  return unittest.TestLoader().loadTestsFromTestCase(TestBwaAligner)

if __name__ == '__main__':
  unittest.TextTestRunner(verbosity=2).run(suite())
