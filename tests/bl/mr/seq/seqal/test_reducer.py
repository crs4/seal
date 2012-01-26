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

from pydoop.utils import make_input_split
from pydoop._pipes import get_JobConf_object

from bl.mr.test_utils import reduce_context
from bl.mr.seq.seqal.reducer import reducer
from bl.mr.seq.seqal.seqal_app import PAIR_STRING
import bl.lib.seq.aligner.io.protobuf_mapping as proto
import bl.lib.seq.aligner.sam_flags as sam_flags
import test_utils # specific to seqal

class TestSeqalReducer(unittest.TestCase):

	def setUp(self):
		self.__jc = get_JobConf_object({})
		self.__ctx = reduce_context(self.__jc, [])
		self.__reducer = reducer(self.__ctx)
		self.__reducer.discard_duplicates = True
		self.__clean_reducer = reducer(self.__ctx) # unmodified

	def test_emit_on_left_key(self):
		# load pair 1
		p = test_utils.pair1()
		# use the first read to create the map-reduce key
		self.__ctx.add_value(test_utils.make_key(p[0]), proto.serialize_pair(p))
		self.__reducer.reduce(self.__ctx)
		self.__ensure_only_pair1_emitted()

	def test_no_emit_on_right_key(self):
		# load pair 1
		p = test_utils.pair1()
		# use the SECOND read to create the map-reduce key
		self.__ctx.add_value(test_utils.make_key(p[1]), PAIR_STRING)
		self.__reducer.reduce(self.__ctx)
		# we should have no output
		self.assertEqual(0, len(self.__ctx.emitted.keys()))

	def test_duplicate_pairs(self):
		# Two identical pairs.  Ensure only one is emitted
		p = test_utils.pair1()
		# use the first read to create the map-reduce key
		self.__ctx.add_value(test_utils.make_key(p[0]), proto.serialize_pair(p))
		self.__ctx.add_value(test_utils.make_key(p[0]), proto.serialize_pair(p)) # add it twice
		self.__reducer.reduce(self.__ctx)
		self.assertEqual(1, len(self.__ctx.emitted.keys()))
		self.assertEqual(2, len(self.__ctx.emitted.values()[0])) # two SAM records associated with the same key
		self.__ensure_only_pair1_emitted()
		# check counter
		if self.__ctx.counters.has_key(self.__frag_counter_name()):
			self.assertEqual(0, self.__ctx.counters[self.__frag_counter_name()])
		self.assertTrue(self.__ctx.counters.has_key(self.__pair_counter_name()))
		self.assertEqual(1, self.__ctx.counters[self.__pair_counter_name()])

	def test_duplicate_pairs_no_discard(self):
		# Two identical pairs.  Ensure only one is emitted
		p = test_utils.pair1()
		# use the first read to create the map-reduce key
		self.__ctx.add_value(test_utils.make_key(p[0]), proto.serialize_pair(p))
		self.__ctx.add_value(test_utils.make_key(p[0]), proto.serialize_pair(p)) # add it twice
		self.__reducer.discard_duplicates = False
		self.__reducer.reduce(self.__ctx)
		self.assertEqual(1, len(self.__ctx.emitted.keys()))
		self.assertEqual(4, len(self.__ctx.emitted.values()[0])) # four SAM records associated with the same key
		flags = map(lambda sam: int(*re.match("(\d+).*", sam).groups(1)), self.__ctx.emitted.values()[0])
		# ensure we have two marked as duplicates 
		self.assertEqual(2, len(filter(lambda flag: flag & sam_flags.SAM_FDP, flags)) )
		# ensure we have two NOT marked as duplicates 
		self.assertEqual(2, len(filter(lambda flag: flag & sam_flags.SAM_FDP == 0, flags)) )
		# check counter
		if self.__ctx.counters.has_key(self.__frag_counter_name()):
			self.assertEqual(0, self.__ctx.counters[self.__frag_counter_name()])
		self.assertTrue(self.__ctx.counters.has_key(self.__pair_counter_name()))
		self.assertEqual(1, self.__ctx.counters[self.__pair_counter_name()])


	def test_duplicate_pairs_right_key(self):
		# Two identical pairs on the right key
		# Ensure nothing is emitted
		p = test_utils.pair1()
		# use the first read to create the map-reduce key
		self.__ctx.add_value(test_utils.make_key(p[1]), PAIR_STRING)
		self.__ctx.add_value(test_utils.make_key(p[1]), PAIR_STRING) # add it twice
		self.__reducer.reduce(self.__ctx)
		self.assertEqual(0, len(self.__ctx.emitted.keys()))
		# check counter
		if self.__ctx.counters.has_key(self.__pair_counter_name()):
			self.assertEqual(0, self.__ctx.counters[self.__pair_counter_name()])
		if self.__ctx.counters.has_key(self.__frag_counter_name()):
			self.assertEqual(0, self.__ctx.counters[self.__frag_counter_name()])

	def test_duplicate_fragments_read1(self):
		# load pair 1
		p = list(test_utils.pair1())
		p = test_utils.erase_read2(p)
		p0 = p[0]
		# insert the pair into the context, twice
		self.__ctx.add_value(test_utils.make_key(p[0]), proto.serialize_pair(p))
		self.__ctx.add_value(test_utils.make_key(p[0]), proto.serialize_pair(p))
		self.__reducer.reduce(self.__ctx)
		self.assertEqual(1, len(self.__ctx.emitted.keys()))
		self.assertEqual(1, len(self.__ctx.emitted.values()[0])) # only one SAM record associated with the key
		short_name = p0.get_name()[0:-2]
		self.assertEqual(short_name, self.__ctx.emitted.keys()[0])
		self.assertTrue( re.match("\d+\s+%s\s+%d\s+.*" % (p0.tid, p0.pos), self.__ctx.emitted[short_name][0]) )
		# check counter
		self.assertFalse(self.__ctx.counters.has_key(self.__pair_counter_name()))
		self.assertTrue(self.__ctx.counters.has_key(self.__frag_counter_name()))
		self.assertEqual(1, self.__ctx.counters[self.__frag_counter_name()])

	def test_duplicate_fragments_read1_no_discard(self):
		# load pair 1 and erase its second read
		p = list(test_utils.pair1())
		p = test_utils.erase_read2(p)
		p0 = p[0]
		# insert the pair into the context, twice
		self.__ctx.add_value(test_utils.make_key(p[0]), proto.serialize_pair(p))
		self.__ctx.add_value(test_utils.make_key(p[0]), proto.serialize_pair(p))
		self.__reducer.discard_duplicates = False
		self.__reducer.reduce(self.__ctx)
		self.assertEqual(1, len(self.__ctx.emitted.keys()))
		self.assertEqual(2, len(self.__ctx.emitted.values()[0])) # Two SAM records associated with the key
		short_name = p0.get_name()[0:-2]
		self.assertEqual(short_name, self.__ctx.emitted.keys()[0])
		flags = map(lambda sam: int(*re.match("(\d+).*", sam).groups(1)), self.__ctx.emitted.values()[0])
		# ensure we have one marked as duplicate
		self.assertEqual(1, len(filter(lambda flag: flag & sam_flags.SAM_FDP, flags)) )
		# and ensure we have one NOT marked as duplicates 
		self.assertEqual(1, len(filter(lambda flag: flag & sam_flags.SAM_FDP == 0, flags)) )

		# check counter
		self.assertFalse(self.__ctx.counters.has_key(self.__pair_counter_name()))
		self.assertTrue(self.__ctx.counters.has_key(self.__frag_counter_name()))
		self.assertEqual(1, self.__ctx.counters[self.__frag_counter_name()])

	def test_empty_read1(self):
		# Ensure the reducer raises an exception if the pair[0] is None
		p = test_utils.erase_read1(list(test_utils.pair1()))
		self.__ctx.add_value(test_utils.make_key(p[1]), proto.serialize_pair(p))
		self.assertRaises(ValueError, self.__reducer.reduce, self.__ctx)
	
	def test_fragment_with_duplicate_in_pair_1(self):
		# Ensure the reducer catches a fragment duplicate of pair[0]
		p = list(test_utils.pair1())
		self.__ctx.add_value(test_utils.make_key(p[0]), proto.serialize_pair(p))
		test_utils.erase_read2(p)
		self.__ctx.add_value(test_utils.make_key(p[0]), proto.serialize_pair(p))
		self.__reducer.reduce(self.__ctx)
		# now ensure that the pair was emitted, but not the fragment
		self.__ensure_only_pair1_emitted()
		self.assertEqual(1, len(self.__ctx.emitted.keys()))
		self.assertEqual(2, len(self.__ctx.emitted.values()[0])) # two SAM records associated with the key (for the pair)
		# check counter
		self.assertFalse(self.__ctx.counters.has_key(self.__pair_counter_name()))
		self.assertTrue(self.__ctx.counters.has_key(self.__frag_counter_name()))
		self.assertEqual(1, self.__ctx.counters[self.__frag_counter_name()])

	def test_fragment_with_duplicate_in_pair_2(self):
		# Ensure the reducer catches a fragment duplicate of pair[1].
		p = list(test_utils.pair1())
		# Insert the pair into the context
		self.__ctx.add_value(test_utils.make_key(p[1]), PAIR_STRING)
		# Remove the first read from the pair, reorder so that the None is at index 1,
		# the serialize and insert into the context.
		test_utils.erase_read1(p)
		self.__ctx.add_value(test_utils.make_key(p[1]), proto.serialize_pair( (p[1], None) ))
		self.__reducer.reduce(self.__ctx)
		# now ensure that nothing was emitted.  The pair isn't emitted because
		# the key refers to read2, and the fragment isn't emitted because it's a duplicate of
		# the one in the pair.
		self.assertEqual(0, len(self.__ctx.emitted.keys()))
		# check counter
		self.assertFalse(self.__ctx.counters.has_key(self.__pair_counter_name()))
		self.assertTrue(self.__ctx.counters.has_key(self.__frag_counter_name()))
		self.assertEqual(1, self.__ctx.counters[self.__frag_counter_name()])
	
	def test_fragment_with_duplicate_in_pair_1_no_discard(self):
		# Ensure the reducer catches a fragment duplicate of pair[0]
		p = list(test_utils.pair1())
		self.__ctx.add_value(test_utils.make_key(p[0]), proto.serialize_pair(p))
		p = test_utils.erase_read2(p)
		self.__ctx.add_value(test_utils.make_key(p[0]), proto.serialize_pair(p))
		self.__reducer.discard_duplicates = False
		self.__reducer.reduce(self.__ctx)
		# now ensure that both were emitted, but the fragment is marked as duplicate
		self.__ensure_pair1_emitted()
		self.assertEqual(1, len(self.__ctx.emitted.keys()))
		self.assertEqual(3, len(self.__ctx.emitted.values()[0])) # 3 SAM records associated with the key (for the pair)

		# make sure we have a read with the duplicate flag set
		regexp = "(\d+)\s+.*"
		flags = [ int(re.match(regexp, value).group(1)) for value in self.__ctx.emitted.values()[0] ]
		dup_flags = [ flag for flag in flags if flag & sam_flags.SAM_FDP ]
		self.assertEqual(1, len(dup_flags))
		f = dup_flags[0]
		self.assertTrue( f & sam_flags.SAM_FR1 > 0 ) # ensure the duplicate read is r1
		self.assertTrue( f & sam_flags.SAM_FPD == 0 ) # ensure the duplicate read is unpaired
 
		# check counter
		self.assertFalse(self.__ctx.counters.has_key(self.__pair_counter_name()))
		self.assertTrue(self.__ctx.counters.has_key(self.__frag_counter_name()))
		self.assertEqual(1, self.__ctx.counters[self.__frag_counter_name()])


	def test_default_discard_duplicates_setting(self):
		self.assertFalse(self.__clean_reducer.discard_duplicates)

	def test_unmapped2(self):
		p = test_utils.pair1()
		p[1].set_mapped(False)
		p[0].set_mate_mapped(False)
		self.__ctx.add_value(test_utils.make_key(p[0]), proto.serialize_pair(p))
		self.__reducer.reduce(self.__ctx)
		self.assertEqual(2, len(self.__ctx.emitted.values()[0]))

	def test_unmapped1(self):
		p = test_utils.pair1()
		p[0].set_mapped(False)
		p[1].set_mate_mapped(False)
		# Having an unmapped read before a mapped read is not allowed.  This should
		# raise an exception
		# The key is meaningless
		self.__ctx.add_value(test_utils.make_key(p[1]), proto.serialize_pair(p))
		self.assertRaises(ValueError, self.__reducer.reduce, self.__ctx)

	def test_unmapped_pair(self):
		p = test_utils.pair1()
		for i in 0,1:
			p[i].set_mapped(False)
			p[i].set_mate_mapped(False)
		self.__ctx.add_value(test_utils.make_key(p[0]), proto.serialize_pair(p))
		self.__reducer.reduce(self.__ctx)
		self.assertEqual(1, len(self.__ctx.emitted.keys()))
		self.assertEqual(2, len(self.__ctx.emitted.values()[0]))

	

	def __ensure_pair1_emitted(self):
		p = test_utils.pair1()
		# Now we expect a SAM entry for each of the two pairs.
		# At the moment, the SAM formatter emits the read name as the key, and the
		# rest of the SAM record as the value.  Remember that the protobuff serialization
		# removes the read number ("/1" or "/2") from the read name.
		short_name = p[0].get_name()[0:-2] # mapping name without the read number
		self.assertTrue( self.__ctx.emitted.has_key(short_name)  )
		self.assertTrue(len(self.__ctx.emitted[short_name]) >= 2 ) # at least two SAM records emitted


	def __ensure_only_pair1_emitted(self):
		self.__ensure_pair1_emitted()
		p = test_utils.pair1()
		short_name = p[0].get_name()[0:-2] # mapping name without the read number
		self.assertEqual( [short_name], self.__ctx.emitted.keys() )
		self.assertEqual(2, len(self.__ctx.emitted[short_name])) # two SAM records emitted
		regexp = "\d+\s+%s\s+(\d+)\s+.*" % p[0].tid # all reads have the same tid.  Match the position
		emitted_positions = map(lambda sam: int(*re.match(regexp, sam).groups(1)), self.__ctx.emitted[short_name])
		self.assertEqual( [p[0].pos, p[1].pos], sorted(emitted_positions) ) # ensure we have both positions
		emitted_flags = map(lambda sam: int(*re.match("(\d+).*", sam).groups(1)), self.__ctx.emitted[short_name])
		# ensure all the reads we found are flagged as mapped
		self.assertTrue( all(map(lambda flag: flag & (sam_flags.SAM_FSU | sam_flags.SAM_FMU) == 0, emitted_flags)) )

	def __pair_counter_name(self):
		return ':'.join( (self.__reducer.COUNTER_CLASS, "DUPLICATE PAIRS") )

	def __frag_counter_name(self):
		return ':'.join( (self.__reducer.COUNTER_CLASS, "DUPLICATE FRAGMENTS") )


def suite():
	return unittest.TestLoader().loadTestsFromTestCase(TestSeqalReducer)

if __name__ == '__main__':
	unittest.TextTestRunner(verbosity=2).run(suite())
