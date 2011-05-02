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


######################################################
# Unit test for BwaMapping class
# To run:  SRC_DIR=<path to>/seqal/bl/lib/seq/aligner/bwa/libbwa  python test_bwa_mapping.py
# 

import unittest
import ctypes as ct
import pickle
from itertools import izip

import bl.lib.seq.aligner.bwa.bwa_core as bwa
from bl.lib.seq.aligner.bwa.bwa_mapping import BwaMapping
from bl.lib.seq.aligner.sam_flags import *

import testing_utilities as utils

class FakeOpts(object):
	def __init__(self):
		# set the two options used by BwaMapping to their default values
		self.mode = 0x01 | 0x02 # BWA_MODE_GAPE | BWA_MODE_COMPREAD
		self.max_top2 = 30 

class FakeReference(object):

	class Annotation(object):
		def __init__(self, name, offset, length):
			self.name = name
			self.offset = offset
			self.len = length
	
	def __init__(self):
		self.canned_results = {  # for bwa.get_seq_id
		  (1840870115L, 99L): (10L, 0L),
		  (1840869941L, 100L): (10L, 0L),
		  (1840869941L, 90L): (10L, 0L),
		  (1840870115L, 100L): (10L, 0L),
		  (406515435L, 56L): (1L, 0L),
		  (406515557L, 100L): (1L, 0L),
		  (406515557L, 35L): (1L, 0L),
		  (406515435L, 100L): (1L, 0L),
		  (867800532L, 100L): (3L, 0L),
		  (867800321L, 100L): (3L, 0L),
		  (867800321L, 100L): (3L, 0L),
		  (867800532L, 100L): (3L, 0L),
		  (414924637L, 100L): (1L, 0L),
		  (414924404L, 100L): (1L, 0L),
		  (414924404L, 80L): (1L, 0L),
		  (414924637L, 100L): (1L, 0L),
		  (556634939L, 91L): (2L, 0L),
		  (556634729L, 100L): (2L, 0L),
		  (556634729L, 99L): (2L, 0L),
		  (556634939L, 100L): (2L, 0L)
		}

		self.hg18_index_annotations = [
		  ('chr1', 0, 247249719L, 39L),
		  ('chr2', 247249719L, 242951149L, 25),
		  ('chr3', 490200868L, 199501827L, 10),
		  ('chr4', 689702695L, 191273063L, 14),
		  ('chr5', 880975758L, 180857866L, 7),
		  ('chr6', 1061833624L, 170899992L, 11),
		  ('chr7', 1232733616L, 158821424L, 12),
		  ('chr8', 1391555040L, 146274826L, 10),
		  ('chr9', 1537829866L, 140273252L, 38),
		  ('chr10', 1678103118L, 135374737L, 26),
		  ('chr11', 1813477855L, 134452384L, 9),
		  ('chr12', 1947930239L, 132349534L, 13),
		  ('chr13', 2080279773L, 114142980L, 6),
		  ('chr14', 2194422753L, 106368585L, 2),
		  ('chr15', 2300791338L, 100338915L, 11),
		  ('chr16', 2401130253L, 88827254L, 6),
		  ('chr17', 2489957507L, 78774742L, 10),
		  ('chr18', 2568732249L, 76117153L, 4),
		  ('chr19', 2644849402L, 63811651L, 5),
		  ('chr20', 2708661053L, 62435964L, 6),
		  ('chr21', 2771097017L, 46944323L, 11),
		  ('chr22', 2818041340L, 49691432L, 28),
		  ('chrX', 2867732772L, 154913754L, 16),
		  ('chrY', 3022646526L, 57772954L, 14),
		  ('chrM', 3080419480L, 16571L, 0)
		]

		# here we hold fake "anns" objects that respond to 'name'
		self.anns = [ FakeReference.Annotation(name, offset, length) for name, offset, length, unknown in self.hg18_index_annotations ]

	def get_seq_id(self, bns, hit_pos, hit_len):
		# We we return hard-coded values for (position, length) combinations.
		# The returned values were calculated using the hg18 reference
		return self.canned_results[(hit_pos, hit_len)]



class TestBwaMapping(unittest.TestCase):

	def setUp(self):
		self.gap_opts = FakeOpts()
		self.reference = FakeReference()

		# To fake having a reference, we have to overwrite the bwa.get_seq_id function 
		# called by BwaMapping so that it'll use our FakeReference instead of the real one.
		bwa.get_seq_id = lambda bns, pos, len: self.reference.get_seq_id(bns, pos, len)

		# now create the hits
		# With have a file with 10 serialized dictionaries containing values most fields of
		# the bwa_seq_t struct.  The items are in the order read1, mate1, read2, mate2, ...
		with open(utils.get_fixture_path("hit_wrapper_hits.pickle")) as f:
			hits_data = pickle.load(f) # load an array of dictionaries

		self.bwa_seqs = map(utils.build_bwa_seq_t, hits_data)
		# finally, create the BwaMapping objects for each bwa_seq_t
		self.hits = []
		for s1, s2 in izip( self.bwa_seqs[0::2], self.bwa_seqs[1::2] ):
			self.hits.append( BwaMapping(self.gap_opts, self.reference, s1, s2) )
			self.hits.append( BwaMapping(self.gap_opts, self.reference, s2, s1) )


	def test_is_paired(self):
		# all our hits are paired
		self.assertTrue(self.hits[0].is_paired())
		self.assertTrue(self.hits[1].is_paired())
		# modify a flag to unset any flags referencing a mate
		self.hits[0].flag = self.hits[0].flag & ~SAM_FPD & ~SAM_FPP & ~SAM_FMU & ~SAM_FMR & ~SAM_FR1 & ~SAM_FR2
		self.assertFalse(self.hits[0].is_paired())


	def test_is_properly_paired(self):
		self.assertTrue(self.hits[0].is_properly_paired())
		self.assertTrue(self.hits[1].is_properly_paired())
		self.hits[0].flag = self.hits[0].flag & ~SAM_FPP
		self.assertFalse(self.hits[0].is_properly_paired())


	def test_is_isnt_mapped(self):
		self.assertTrue(self.hits[0].is_mapped())
		self.assertTrue(self.hits[1].is_mapped())
		# fake flag
		self.hits[0].flag = self.hits[0].flag | SAM_FSU
		self.assertFalse(self.hits[0].is_mapped())
		self.assertTrue(self.hits[0].is_unmapped())

	def test_is_mate_mapped_unmapped(self):
		self.assertTrue(self.hits[0].is_mate_mapped())
		self.assertTrue(self.hits[1].is_mate_mapped())
		# fake flag
		self.hits[0].flag = self.hits[0].flag | SAM_FMU
		self.assertFalse(self.hits[0].is_mate_mapped())
		self.assertTrue(self.hits[0].is_mate_unmapped())

	def test_is_on_reverse(self):
		# indices [0, 3, 4, 6, 8] are on reverse strand
		hit = self.hits[0]; self.assertTrue( hit.is_on_reverse())
		hit = self.hits[3]; self.assertTrue( hit.is_on_reverse())
		hit = self.hits[4]; self.assertTrue( hit.is_on_reverse())
		hit = self.hits[6]; self.assertTrue( hit.is_on_reverse())
		hit = self.hits[1]; self.assertFalse( hit.is_on_reverse())
		hit = self.hits[2]; self.assertFalse( hit.is_on_reverse())


	def test_is_mate_on_reverse(self):
		# indices [0, 3, 4, 6, 8] are on reverse strand
		hit = self.hits[1]; self.assertTrue( hit.is_mate_on_reverse())
		hit = self.hits[2]; self.assertTrue( hit.is_mate_on_reverse())
		hit = self.hits[5]; self.assertTrue( hit.is_mate_on_reverse())
		hit = self.hits[7]; self.assertTrue( hit.is_mate_on_reverse())
		hit = self.hits[0]; self.assertFalse( hit.is_mate_on_reverse())
		hit = self.hits[3]; self.assertFalse( hit.is_mate_on_reverse())
		hit = self.hits[4]; self.assertFalse( hit.is_mate_on_reverse())


	def test_is_read1(self):
		self.assertTrue( self.hits[0].is_read1())
		self.assertFalse( self.hits[1].is_read1())
		self.assertTrue( self.hits[2].is_read1())
		self.assertFalse( self.hits[3].is_read1())

	def test_is_read2(self):
		self.assertFalse( self.hits[0].is_read2())
		self.assertTrue( self.hits[1].is_read2())
		self.assertFalse( self.hits[2].is_read2())
		self.assertTrue( self.hits[3].is_read2())

	def test_is_secondary_align(self):
		# we don't have any secondary alignments
		self.assertTrue( all(map(lambda hit: not hit.is_secondary_align(), self.hits)))
		self.hits[0].flag = self.hits[0].flag | SAM_FSC
		self.assertTrue(self.hits[0].is_secondary_align())

	def test_tag_value(self):
		# we should have these flags on hits[0]:
		# XC:i:99 XT:A:U  NM:i:0  SM:i:37 AM:i:37 X0:i:1  X1:i:0  XM:i:0  XO:i:0  XG:i:0  MD:Z:99
		hit = self.hits[0]
		self.assertEqual(99, hit.tag_value("XC"))
		self.assertEqual('U', hit.tag_value("XT"))
		self.assertEqual(0, hit.tag_value("NM"))
		self.assertEqual(37, hit.tag_value("SM"))
		self.assertEqual(37, hit.tag_value("AM"))
		self.assertEqual(1, hit.tag_value("X0"))
		self.assertEqual(0, hit.tag_value("X1"))
		self.assertEqual(0, hit.tag_value("XM"))
		self.assertEqual(0, hit.tag_value("XO"))
		self.assertEqual(0, hit.tag_value("XG"))
		# we can't check the MD tag because our struct doesn't include the md data
		# self.assertEqual('99', hit.tag_value("MD"))
		# what happens with a missing tag?
		self.assertTrue(hit.tag_value("lallala") is None) # assertIsNone isn't in the version of python we're using

	def test_flag_string(self):
		self.assertEqual("pPr1", self.hits[0].flag_string())
		self.assertEqual("pPR2", self.hits[1].flag_string())
		# make up some flags
		hit = self.hits[0]
		hit.flag = SAM_FPD | SAM_FSU | SAM_FR1
		self.assertEqual("pu1", hit.flag_string())
		hit.flag = SAM_FPD | SAM_FMU | SAM_FR1
		self.assertEqual("pU1", hit.flag_string())

	def __format_cigar(self, cigar_array):
		return "".join(['%d%s' % t for t in cigar_array]) or "*"

	def test_cigar(self):
		for i, cigar in (0, "1S99M"), (1, "90M10S"), (2, "56M44S"), (3, "65S35M"), (4, "100M"), (5, "100M"):
			self.assertEqual(cigar, self.__format_cigar(self.hits[i].get_cigar()), "%s != %s -- %s" % (cigar, self.__format_cigar(self.hits[i].get_cigar()), str(self.hits[i])))

	def test_isize(self):
		for i, isize in (0, -273), (1, 273), (2, 157), (3, -157), (4, -311), (5, 311):
			self.assertEqual(isize, self.hits[i].isize)

	def test_pos(self):
		for i, pos in (0, 27392261), (1, 27392087), (2, 159265717), (3, 159265839), (4, 178097838), (5, 178097627):
			self.assertEqual(pos, self.hits[i].pos)

	def test_mpos(self):
		for i, pos in (0, 27392087), (1, 27392261), (2, 159265839), (3, 159265717), (4, 178097627), (5, 178097838):
			self.assertEqual(pos, self.hits[i].mpos)

	def test_tid(self):
		for i, tid in (0, "chr11"), (1, "chr11"), (2, "chr2"), (3, "chr2"), (4, "chr4"), (5, "chr4"):
			self.assertEqual(tid, self.hits[i].tid)

	def test_mtid(self):
		for i, mtid in (0, "="), (1, "="), (2, "="), (3, "="), (4, "="), (5, "="):
			self.assertEqual(mtid, self.hits[i].mtid)


	def test_name(self):
		names = ['CRESSIA:1:1:10000:101092#0/1',
		'CRESSIA:1:1:10000:101092#0/2',
		'CRESSIA:1:1:10000:104835#0/1',
		'CRESSIA:1:1:10000:104835#0/2',
		'CRESSIA:1:1:10000:105809#0/1',
		'CRESSIA:1:1:10000:105809#0/2',
		'CRESSIA:1:1:10000:112583#0/1',
		'CRESSIA:1:1:10000:112583#0/2',
		'CRESSIA:1:1:10000:72482#0/1',
		'CRESSIA:1:1:10000:72482#0/2']

		for i in xrange(len(names)):
			self.assertEqual(names[i], self.hits[i].get_name())

	def test_qual(self):
		## we know that our version gives mapq values that are different from the command line version
		## BWA.  We'll test to ensure that the values in our BwaAlignments correspond to the values
		## in the bwa structure.  Arguably, to write a pure unit test for BwaMapping, we should be
		## only considering the incoming bwa_seq_t values for all the other tests as well...
		#for i in xrange(len(self.hits)):
		#	self.assertEqual(self.bwa_seqs[i].mapQ, self.hits[i].qual)
		for i in xrange(len(self.hits)):
			self.assertEqual(60, self.hits[i].qual)


	def test_get_seq_5(self):
		# indices [0, 3, 4, 6, 8] are on reverse strand
		expected_answers = [
				(0, "CRESSIA:1:1:10000:101092#0/1", "TTAAAAAAGCTTCCCCAAGACCATTCAATAAGCAGTGTAACAGAAAACACCAGAGCCCATGACCTTAAGTGCAAACGATGAGTCTTCTTAAGTATTATTT"),
				(1, "CRESSIA:1:1:10000:101092#0/2", "TACTAGCCTTTCCCAAAGCAGGTTTGGAAAAGTTTTTGGCTTGGGAATAACAGTAACAGTAGCAGCAACAAAAACAGCAGGAGCAGCAGCTAAGATGCAG"),
				(4, "CRESSIA:1:1:10000:105809#0/1", "TTCCATTTTTGCCAGCAGACTAACCCCAGACTCTAAATCCCAGTAGCAATGTCTTCAGCTTGTGGGTGGTACTGGCTTCTTGCTGTTAGTATTCTGTGGG"),
				(5, "CRESSIA:1:1:10000:105809#0/2", "AAACGAAATCTTCATGCCAAAACTAACACTTGTCAGTTGGACCTCTCTATAGTCTGTAACGTATTTTATTTCCTCTCCCTTTAGTGAAGTTCATCTGACA")
		]

		for idx, name, seq in expected_answers:
			self.assertEqual(name, self.hits[idx].get_name())
			self.assertEqual(seq, self.hits[idx].get_seq_5())

	def test_qualseq(self):
		expected_answers = [
				(0, "CRESSIA:1:1:10000:101092#0/1", "##EEEEEE=EF:DDDFGGGGEEEBEFDGEFBFG=FEEDBGGGGGGGEGDGGGGGFFGFGGGGDGGGFGGEGEGGGGGGEGGGGGGGGEGFGGGGFGGGGG"),
				(1, "CRESSIA:1:1:10000:101092#0/2", "GGGGGGDBGGGFGGGGGDEGGGGGGEGGGGFGGGGGEGGFGGDGA<BAAAEEEEDBDDDBGEGADDDDD:GGGGGEGDD?BA::B??AA###########"),
				(4, "CRESSIA:1:1:10000:105809#0/1", ":=DD=ECECAAA?>ADBD@DEDCEEEDGGEGDFEGFECGGGFGFFGGGGGGBFGGEGGGGGFGGGGGEGGGGGGGGGGGGGDGGGGGGGGFGGGFGGGGG"),
				(5, "CRESSIA:1:1:10000:105809#0/2", "GGGGGGGGGGGGGGGGGGGGGGGGFGGGGGGGGGFGFGGGGGGGGGGGDGFFFFFGFGFGFEDFFGGFGGDGGGGGGGFCGDGGF?FFFEGGGFGGGGG?")
		]
		for idx, name, qseq in expected_answers:
			self.assertEqual(name, self.hits[idx].get_name())
			self.assertEqual(qseq, ''.join([ chr(q+33) for q in self.hits[idx].get_base_qualities() ]) )
			self.assertEqual(qseq, self.hits[idx].get_ascii_base_qual())

	def test_tags(self):
		clip_tags = map(lambda h: filter(lambda tple: tple[0] == "XC", h.each_tag()), self.hits)
		for i in xrange(len(self.hits)):
			xc_tag = filter(lambda tpl: tpl[0] == "XC", self.hits[i].each_tag())
			if self.bwa_seqs[i].full_len > self.bwa_seqs[i].clip_len:
				self.assertTrue(len(xc_tag) > 0) # ensure we have an XC tag
				xc_value = xc_tag[0][2]
				self.assertEqual(self.bwa_seqs[i].clip_len, xc_value)

	def test_get_seq_len(self):
		for h in self.hits:
			self.assertEqual(100, h.get_seq_len())

	"""
	0 XC: 99 on reverse? True
	1 XC: 90 on reverse? False
	2 XC: 56 on reverse? False
	3 XC: 35 on reverse? True
	4 XC: None on reverse? True
	5 XC: None on reverse? False
	6 XC: None on reverse? True
	7 XC: 80 on reverse? False
	8 XC: 91 on reverse? True
	9 XC: 99 on reverse? False
	"""
	def test_get_untrimmed_pos_unclipped_forward(self):
		# hits[5] has not been clipped and it's on the forward strand
		self.assertEqual(self.hits[5].pos, self.hits[5].get_untrimmed_pos())

	def test_get_untrimmed_pos_clipped_forward(self):
		# hits[2] has been clipped to length 90, and it's not on the reverse strand
		self.assertEqual(self.hits[2].pos, self.hits[2].get_untrimmed_pos())

	def test_get_untrimmed_pos_unclipped_reverse(self):
		# hits[4] has not been clipped and it's on the reverse strand
		self.assertEqual(self.hits[4].pos, self.hits[4].get_untrimmed_pos())

	def test_get_untrimmed_pos_clipped_reverse(self):
		# hits[3] has been clipped to 35 and it's on the reverse strand
		self.assertEqual(self.hits[3].pos - 65, self.hits[3].get_untrimmed_pos())

	def test_base_qualities_out_of_range(self):
		bwa_seq = self.bwa_seqs[0]
		for i in xrange(0, bwa_seq.len):
			bwa_seq.qual[i] = 1 # insert a bad encoded quality value
		mapping = BwaMapping(self.gap_opts, self.reference, bwa_seq, self.bwa_seqs[1])
		self.assertRaises(ValueError, mapping.get_base_qualities)

def suite():
	"""Get a suite with all the tests from this module"""
	return unittest.TestLoader().loadTestsFromTestCase(TestBwaMapping)

if __name__ == '__main__':
	unittest.TextTestRunner(verbosity=2).run(suite())
