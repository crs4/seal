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
import copy

from seal.lib.aligner.sam_mapping import SAMMapping

class TestSamMapping(unittest.TestCase):
	def setUp(self):
		self.sam1 = "HWI-ST200R_251:5:1208:19924:124635#GCCAAT\t83\t20\t6181935\t60\t5S96M\t=\t6181919\t-112\tAAGTGGAAGATTTGGGAATCTGAGTGGATTTGGTAACAGTAGAGGGGTGGATCTGGCTTGGAAAACAATCGAGGTACCAATATAGGTGGTAGATGAATTTT\t?<?AADADBFBF<EHIGHGGGEAF3AF<CHGGDG9?GHFFACDHH)?@AHEHHIIIIE>A=A:?);B27@;@?>,;;C(5::>>>@5:()4>@@@######\tXC:i:96\tXT:A:U\tNM:i:1\tSM:Z:raw_merged-1.2.3.4.5.6.7.8.bam\tAM:i:37\tX0:i:1\tX1:i:0\tXM:i:1\tXO:i:0\tXG:i:0\tMD:Z:13G82\tRG:Z:raw_merged-1.2.3.4.5.6.7.8.bam\tOQ:Z:######@@@>4)(:5@>>>::5(C;;,>?@;@72B;)?:A=A>EIIIIHHEHA@?)HHDCAFFHG?9GDGGHC<FA3FAEGGGHGIHE<FBFBDADAA?<?"
		self.sam2 = "HWI-ST200R_251:5:1208:19924:124635#GCCAAT\t163\t20\t6181919\t60\t101M\t=\t6181935\t112\tCTGAGCACACCAAAATTCATCTACCACCTATATTGGTACCTCGATTGTTTTCCAAGCCAGATCCACACCTCTACTGTTACCAAATCCACTCAGATTCCCAA\t@@@FFFDDFHG??;EEH>HHGIGHEGCGEGGIGJG31?DDBBD>FGG@HG??DFBBADFAGII3@EH;;CEHECBB7?>CE.;...5>ACDDA:C:;>:>?\tXT:A:U\tNM:i:2\tSM:Z:raw_merged-1.2.3.4.5.6.7.8.bam\tAM:i:37\tX0:i:1\tX1:i:0\tXM:i:2\tXO:i:0\tXG:i:0\tMD:Z:29G36C34\tRG:Z:raw_merged-1.2.3.4.5.6.7.8.bam\tOQ:Z:@@@FFFDDFHG??;EEH>HHGIGHEGCGEGGIGJG31?DDBBD>FGG@HG??DFBBADFAGII3@EH;;CEHECBB7?>CE.;...5>ACDDA:C:;>:>?"
		self.maps = map(SAMMapping, (self.sam1, self.sam2) )

	def test_ref_id(self):
		self.assertEqual(20, self.maps[0].ref_id)

	def test_tid(self):
		self.assertEqual("20", self.maps[0].tid)

	def test_pos(self):
		self.assertEqual(6181935, self.maps[0].pos)

	def test_name(self):
		self.assertEqual("HWI-ST200R_251:5:1208:19924:124635#GCCAAT", self.maps[0].get_name())

	def test_flag(self):
		self.assertEqual(83, self.maps[0].flag)

	def test_mpos(self):
		self.assertEqual(6181919, self.maps[0].mpos)

	def test_mtid(self):
		self.assertEqual("=", self.maps[0].mtid)

	def test_qual(self):
		self.assertEqual(60, self.maps[0].qual)

	def test_isize(self):
		self.assertEqual(-112, self.maps[0].isize)

	def test_is_paired(self):
		self.assertTrue(self.maps[0].is_paired())

	def test_is_properly_paired(self):
		self.assertTrue(self.maps[0].is_properly_paired())

	def test_is_mapped(self):
		self.assertTrue(self.maps[0].is_mapped())

	def test_is_unmapped(self):
		self.assertFalse(self.maps[0].is_unmapped())

	def test_is_mate_mapped(self):
		self.assertTrue(self.maps[0].is_mate_mapped())

	def test_is_mate_unmapped(self):
		self.assertFalse(self.maps[0].is_mate_unmapped())

	def test_is_on_reverse(self):
		self.assertTrue(self.maps[0].is_on_reverse())
		self.assertFalse(self.maps[1].is_on_reverse())

	def test_is_mate_on_reverse(self):
		self.assertFalse(self.maps[0].is_mate_on_reverse())
		self.assertTrue(self.maps[1].is_mate_on_reverse())

	def test_is_read1(self):
		self.assertTrue(self.maps[0].is_read1())
		self.assertFalse(self.maps[1].is_read1())

	def test_is_read2(self):
		self.assertFalse(self.maps[0].is_read2())
		self.assertTrue(self.maps[1].is_read2())

	def test_is_secondary_align(self):
		self.assertFalse(self.maps[0].is_secondary_align())

	def test_get_untrimmed_pos_unclipped_forward(self):
		# hit has not been clipped and it's on the forward strand
		self.assertEqual(self.maps[1].pos, self.maps[1].get_untrimmed_pos())

	def test_get_untrimmed_pos_clipped_reverse(self):
		# hit has been clipped to 96 and it's on the reverse strand
		self.assertEqual(self.maps[0].pos - 5, self.maps[0].get_untrimmed_pos())

	def test_get_seq_len(self):
		self.assertEqual(101, self.maps[0].get_seq_len())

	def test_difficult_tag(self):
		self.assertEqual("######@@@>4)(:5@>>>::5(C;;,>?@;@72B;)?:A=A>EIIIIHHEHA@?)HHDCAFFHG?9GDGGHC<FA3FAEGGGHGIHE<FBFBDADAA?<?", self.maps[0].tag_value("OQ"))

def suite():
	"""Get a suite with all the tests from this module"""
	return unittest.TestLoader().loadTestsFromTestCase(TestSamMapping)

if __name__ == '__main__':
	unittest.TextTestRunner(verbosity=2).run(suite())
