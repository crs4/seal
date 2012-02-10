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

import re
import unittest
from bl.mr.test_utils import map_context, SavingLogger

import bl.lib.seq.aligner.io.protobuf_mapping as proto
from bl.mr.lib.hit_processor_chain_link import HitProcessorChainLink
from bl.mr.lib.hadoop_event_monitor import HadoopEventMonitor
from bl.mr.seq.seqal.mapper import MarkDuplicatesEmitter
from bl.mr.seq.seqal.seqal_app import PAIR_STRING, UNMAPPED_STRING
from bl.lib.seq.aligner.sam_mapping import SAMMapping
import test_utils # specific to seqal

class TestMarkDuplicatesEmitter(unittest.TestCase):

	# mini object to let us peek at what the filter forwards to the next link
	class Receiver(HitProcessorChainLink):
		def __init__(self, *args):
			super(type(self), self).__init__(*args)
			self.received = None

		def process(self, pair):
			self.received = pair

	def setUp(self):
		self.ctx = map_context(None, None)
		self.count_group = "Test"
		self.logger = SavingLogger()
		self.monitor = HadoopEventMonitor(self.count_group, self.logger, self.ctx)
		self.link = MarkDuplicatesEmitter(self.ctx, self.monitor)
		self.pair1 = test_utils.pair1()
		self.pair2 = test_utils.pair2()

	def test_forward(self):
		# see whether it forwads the pair to the next link in the chain
		receiver = type(self).Receiver()
		self.link.set_next(receiver)
		self.link.process(self.pair1)
		self.assertFalse(receiver.received is None)
		self.assertEqual(self.pair1, receiver.received)

	def test_forward_reversed(self):
		# see whether a "reversed" pair is emitted unmodified to the next link
		receiver = type(self).Receiver()
		self.link.set_next(receiver)
		self.link.process(self.pair2)
		self.assertFalse(receiver.received is None)
		self.assertEqual(self.pair2, receiver.received)

	def test_emit_forward_pair(self):
		# We expect to get the pair emitted with the key generated from
		# read 1 (the left read).  On the other hand, we expect to get
		# PAIR_STRING with the key generated from read 2
		self.link.process(self.pair1)
		expected_keys = map(test_utils.make_key, self.pair1)
		for i in 0,1:
			self.assertTrue( self.ctx.emitted.has_key(expected_keys[i]) )
			self.assertEqual(1, len(self.ctx.emitted[expected_keys[i]]))

		unserialized = proto.unserialize_pair(self.ctx.emitted[expected_keys[0]][0])
		for j in 0,1:
			self.assertEqual(self.pair1[j].tid, unserialized[j].tid)
			self.assertEqual(self.pair1[j].pos, unserialized[j].pos)

		second_value = self.ctx.emitted[expected_keys[1]][0]
		self.assertEqual(PAIR_STRING, second_value)

	def test_emit_backward_pair(self):
		# Similar to test_emit_forward_pair, but here we expect to have the reads
		# reordered.  So,
		#   key2 => reversed and serialized pair
		#   key1 => PAIR_STRING
		self.link.process(self.pair2)
		expected_keys = map(test_utils.make_key, self.pair2)
		for i in 0,1:
			self.assertTrue( self.ctx.emitted.has_key(expected_keys[i]) )
			self.assertEqual(1, len(self.ctx.emitted[expected_keys[i]]))

		unserialized = proto.unserialize_pair(self.ctx.emitted[expected_keys[1]][0])
		for j in 0,1:
			self.assertEqual(self.pair2[j].tid, unserialized[j^1].tid)
			self.assertEqual(self.pair2[j].pos, unserialized[j^1].pos)

		second_value = self.ctx.emitted[expected_keys[0]][0]
		self.assertEqual(PAIR_STRING, second_value)

	def test_count_emitted_records(self):
		# we expect to get the pair emitted twice, once with the key generated from
		# read 1 and once with the read generated from read 2
		self.link.process(self.pair1)
		self.assertTrue(self.ctx.counters.has_key("Test:MAPPED COORDINATES"))
		self.assertEqual(2, self.ctx.counters["Test:MAPPED COORDINATES"])

	def test_emit_forward_fragment1(self):
		# None in pair[0]. Fragment in pair[1].
		self.pair1 = test_utils.erase_read1(list(self.pair1))
		self.link.process(self.pair1)
		self.assertEqual(1, len(self.ctx.emitted.keys()))
		expected_key = test_utils.make_key(self.pair1[1])
		self.assertEqual(1, len(self.ctx.emitted[expected_key]))
		unserialized = proto.unserialize_pair(self.ctx.emitted[expected_key][0])
		self.assertTrue(unserialized[1] is None)
		self.assertEqual(self.pair1[1].tid, unserialized[0].tid)
		self.assertEqual(self.pair1[1].pos, unserialized[0].pos)
		self.assertTrue(self.ctx.counters.has_key("Test:MAPPED COORDINATES"))
		self.assertEqual(1, self.ctx.counters["Test:MAPPED COORDINATES"])

	def test_emit_forward_fragment2(self):
		# Fragment in pair[0].  None in pair[1]
		self.pair1 = test_utils.erase_read2(list(self.pair1))
		self.link.process(self.pair1)
		self.assertEqual(1, len(self.ctx.emitted.keys()))
		expected_key = test_utils.make_key(self.pair1[0])
		self.assertEqual(1, len(self.ctx.emitted[expected_key]))
		unserialized = proto.unserialize_pair(self.ctx.emitted[expected_key][0])
		self.assertTrue(unserialized[1] is None)
		self.assertEqual(self.pair1[0].tid, unserialized[0].tid)
		self.assertEqual(self.pair1[0].pos, unserialized[0].pos)
		self.assertTrue(self.ctx.counters.has_key("Test:MAPPED COORDINATES"))
		self.assertEqual(1, self.ctx.counters["Test:MAPPED COORDINATES"])

	def test_emit_reverse_fragment1(self):
		# None in pair[0]. Fragment in pair[1].
		self.pair1 = test_utils.erase_read1(list(self.pair1))
		self.pair1[1].set_on_reverse(True)
		self.link.process(self.pair1)
		self.assertEqual(1, len(self.ctx.emitted.keys()))
		expected_key = test_utils.make_key(self.pair1[1])
		self.assertEqual(1, len(self.ctx.emitted[expected_key]))
		unserialized = proto.unserialize_pair(self.ctx.emitted[expected_key][0])
		self.assertTrue(unserialized[1] is None)
		self.assertEqual(self.pair1[1].tid, unserialized[0].tid)
		self.assertEqual(self.pair1[1].pos, unserialized[0].pos)
		self.assertTrue(unserialized[0].is_on_reverse())

	def test_unmapped1(self):
		self.pair1[0].set_mapped(False)
		self.pair1[1].set_mate_mapped(False)
		self.link.process(self.pair1)

		self.assertEqual(1, len(self.ctx.emitted.keys()))
		self.assertTrue( test_utils.make_key(self.pair1[1]) in self.ctx.emitted.keys() )
		self.assertEqual(1, len(self.ctx.emitted.values()[0]))
		unserialized = proto.unserialize_pair(self.ctx.emitted.values()[0][0])
		self.assertFalse(unserialized[0] is None)
		self.assertFalse(unserialized[1] is None)
		self.assertEqual(self.pair1[1].tid, unserialized[0].tid)
		self.assertEqual(self.pair1[1].pos, unserialized[0].pos)
		self.assertEqual(1, self.ctx.counters["Test:UNMAPPED READS"])

	def test_unmapped2(self):
		self.pair1[1].set_mapped(False)
		self.pair1[0].set_mate_mapped(False)
		self.link.process(self.pair1)
		self.assertEqual(1, len(self.ctx.emitted.keys()))
		self.assertEqual(0, len(filter(lambda k: re.match(UNMAPPED_STRING + ":\d+", k), self.ctx.emitted.keys())))
		self.assertTrue( test_utils.make_key(self.pair1[0]) in self.ctx.emitted.keys() )
		self.assertEqual(1, self.ctx.counters["Test:UNMAPPED READS"])

	def test_both_unmapped(self):
		for i in 0,1:
			self.pair1[i].set_mapped(False)
			self.pair1[i].set_mate_mapped(False)
		self.link.process(self.pair1)
		self.assertEqual(1, len(self.ctx.emitted.keys()))
		self.assertEqual(1, len(filter(lambda k: re.match(UNMAPPED_STRING + ":\d+", k), self.ctx.emitted.keys())))
		self.assertEqual(2, self.ctx.counters["Test:UNMAPPED READS"])

	def test_rmdup_bug(self):
		test_case_data = [
"HWI-ST200R_251:5:1208:19924:124635#GCCAAT\t83\t20\t6181935\t60\t5S96M\t=\t6181919\t-112\tAAGTGGAAGATTTGGGAATCTGAGTGGATTTGGTAACAGTAGAGGGGTGGATCTGGCTTGGAAAACAATCGAGGTACCAATATAGGTGGTAGATGAATTTT\t?<?AADADBFBF<EHIGHGGGEAF3AF<CHGGDG9?GHFFACDHH)?@AHEHHIIIIE>A=A:?);B27@;@?>,;;C(5::>>>@5:()4>@@@######\tXC:i:96\tXT:A:U\tNM:i:1\tSM:Z:raw_merged-1.2.3.4.5.6.7.8.bam\tAM:i:37\tX0:i:1\tX1:i:0\tXM:i:1\tXO:i:0\tXG:i:0\tMD:Z:13G82\tRG:Z:raw_merged-1.2.3.4.5.6.7.8.bam\tOQ:Z:######@@@>4)(:5@>>>::5(C;;,>?@;@72B;)?:A=A>EIIIIHHEHA@?)HHDCAFFHG?9GDGGHC<FA3FAEGGGHGIHE<FBFBDADAA?<?",
"HWI-ST200R_251:5:1208:19924:124635#GCCAAT\t163\t20\t6181919\t60\t101M\t=\t6181935\t112\tCTGAGCACACCAAAATTCATCTACCACCTATATTGGTACCTCGATTGTTTTCCAAGCCAGATCCACACCTCTACTGTTACCAAATCCACTCAGATTCCCAA\t@@@FFFDDFHG??;EEH>HHGIGHEGCGEGGIGJG31?DDBBD>FGG@HG??DFBBADFAGII3@EH;;CEHECBB7?>CE.;...5>ACDDA:C:;>:>?\tXT:A:U\tNM:i:2\tSM:Z:raw_merged-1.2.3.4.5.6.7.8.bam\tAM:i:37\tX0:i:1\tX1:i:0\tXM:i:2\tXO:i:0\tXG:i:0\tMD:Z:29G36C34\tRG:Z:raw_merged-1.2.3.4.5.6.7.8.bam\tOQ:Z:@@@FFFDDFHG??;EEH>HHGIGHEGCGEGGIGJG31?DDBBD>FGG@HG??DFBBADFAGII3@EH;;CEHECBB7?>CE.;...5>ACDDA:C:;>:>?",
"HWI-ST200R_251:6:2207:18561:163438#GCCAAT\t83\t20\t6181938\t60\t8S93M\t=\t6181919\t-112\tAAAATTCATCTACCACCTATATTGGTACCTCGATTGTTTTCCAAGCCAGATCCACCCCTCTACTGTTACCAAATCCACTCAGATTCCCAAATCTTCCACTT\t@@@DDFDFHHHHHJJJEHGGHIHHAEGHJJIJJFGGHGIDIGIJJ?BBGGGIIIJJIJGFHGIJEC(=3?C;?B9?@C>CECECAA(;;3>C#########\tXC:i:93\tXT:A:U\tNM:i:2\tSM:Z:raw_merged-1.2.3.4.5.6.7.8.bam\tAM:i:37\tX0:i:1\tX1:i:0\tXM:i:2\tXO:i:0\tXG:i:0\tMD:Z:10G36C45\tRG:Z:raw_merged-1.2.3.4.5.6.7.8.bam\tOQ:Z:#########C>3;;(AACECEC>C@?9B?;C?3=(CEJIGHFGJIJJIIIGGGBB?JJIGIDIGHGGFJJIJJHGEAHHIHGGHEJJJHHHHHFDFDD@@@",
"HWI-ST200R_251:6:2207:18561:163438#GCCAAT\t163\t20\t6181919\t60\t101M\t=\t6181938\t112\tCTGAGCACACCAAAATTCATCTACCACCTATATTGGTACCTCGATTGTTTTCCAAGCCAGATCCACACCTCTACTGTTACCAAATCCACTCAGATTCCCAA\t@CCFFDDFHHHHHIJJJIIJJJIJJIIJGJIIIJII?DGHIGHDGHIIIJIJIJIIDCHGIJIIGGHIFEHHHHFFFFFDC.6.66;@CCCDCCDC>CCCA\tXT:A:U\tNM:i:2\tSM:Z:raw_merged-1.2.3.4.5.6.7.8.bam\tAM:i:37\tX0:i:1\tX1:i:0\tXM:i:2\tXO:i:0\tXG:i:0\tMD:Z:29G36C34\tRG:Z:raw_merged-1.2.3.4.5.6.7.8.bam\tOQ:Z:@CCFFDDFHHHHHIJJJIIJJJIJJIIJGJIIIJII?DGHIGHDGHIIIJIJIJIIDCHGIJIIGGHIFEHHHHFFFFFDC.6.66;@CCCDCCDC>CCCA",
		]
		sams = map(SAMMapping, test_case_data)
		pair1 = sams[0:2]
		pair2 = sams[2:]
		self.link.process(pair1)

		self.assertEqual(2, len(self.ctx.emitted.keys()))
		key_list = list(sorted(self.ctx.emitted.keys()))
		self.assertEqual("0020:000006181919:F", key_list[0])
		self.assertEqual("0020:000006181930:R", key_list[1])

		self.link.process(pair2)

		self.assertEqual(2, len(self.ctx.emitted.keys()))
		for k,value_list in self.ctx.emitted.iteritems():
			self.assertEqual(2, len(value_list)) # each key should have two pairs at its position

		value_list = self.ctx.emitted["0020:000006181919:F"]
		for value in value_list:
			unserialized = proto.unserialize_pair(value)
			self.assertTrue(unserialized[0].pos < unserialized[1].pos)
		value_list = self.ctx.emitted["0020:000006181930:R"]
		for value in value_list:
			self.assertEqual(PAIR_STRING, value)

def suite():
	return unittest.TestLoader().loadTestsFromTestCase(TestMarkDuplicatesEmitter)

if __name__ == '__main__':
	unittest.TextTestRunner(verbosity=2).run(suite())
