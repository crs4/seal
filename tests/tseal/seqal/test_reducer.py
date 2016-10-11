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
import re
import sys

from pydoop.mapreduce.api import JobConf

from seal.lib.aligner.sam_mapping import SAMMapping
from seal.lib.mr.test_utils import reduce_context
from seal.seqal.reducer import reducer
from seal.seqal import PAIR_STRING
import seal.lib.io.protobuf_mapping as proto
import seal.lib.aligner.sam_flags as sam_flags
import test_utils # specific to seqal

class TestSeqalReducer(unittest.TestCase):

    def setUp(self):
        self.__jc = JobConf([])
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

    def test_rmdup_bug(self):
        test_case_data = [
"HWI-ST200R_251:5:1208:19924:124635#GCCAAT\t83\t20\t6181935\t60\t5S96M\t=\t6181919\t-112\tAAGTGGAAGATTTGGGAATCTGAGTGGATTTGGTAACAGTAGAGGGGTGGATCTGGCTTGGAAAACAATCGAGGTACCAATATAGGTGGTAGATGAATTTT\t?<?AADADBFBF<EHIGHGGGEAF3AF<CHGGDG9?GHFFACDHH)?@AHEHHIIIIE>A=A:?);B27@;@?>,;;C(5::>>>@5:()4>@@@######\tXC:i:96\tXT:A:U\tNM:i:1\tSM:Z:raw_merged-1.2.3.4.5.6.7.8.bam\tAM:i:37\tX0:i:1\tX1:i:0\tXM:i:1\tXO:i:0\tXG:i:0\tMD:Z:13G82\tRG:Z:raw_merged-1.2.3.4.5.6.7.8.bam\tOQ:Z:######@@@>4)(:5@>>>::5(C;;,>?@;@72B;)?:A=A>EIIIIHHEHA@?)HHDCAFFHG?9GDGGHC<FA3FAEGGGHGIHE<FBFBDADAA?<?",
"HWI-ST200R_251:5:1208:19924:124635#GCCAAT\t163\t20\t6181919\t60\t101M\t=\t6181935\t112\tCTGAGCACACCAAAATTCATCTACCACCTATATTGGTACCTCGATTGTTTTCCAAGCCAGATCCACACCTCTACTGTTACCAAATCCACTCAGATTCCCAA\t@@@FFFDDFHG??;EEH>HHGIGHEGCGEGGIGJG31?DDBBD>FGG@HG??DFBBADFAGII3@EH;;CEHECBB7?>CE.;...5>ACDDA:C:;>:>?\tXT:A:U\tNM:i:2\tSM:Z:raw_merged-1.2.3.4.5.6.7.8.bam\tAM:i:37\tX0:i:1\tX1:i:0\tXM:i:2\tXO:i:0\tXG:i:0\tMD:Z:29G36C34\tRG:Z:raw_merged-1.2.3.4.5.6.7.8.bam\tOQ:Z:@@@FFFDDFHG??;EEH>HHGIGHEGCGEGGIGJG31?DDBBD>FGG@HG??DFBBADFAGII3@EH;;CEHECBB7?>CE.;...5>ACDDA:C:;>:>?",
"HWI-ST200R_251:6:2207:18561:163438#GCCAAT\t83\t20\t6181938\t60\t8S93M\t=\t6181919\t-112\tAAAATTCATCTACCACCTATATTGGTACCTCGATTGTTTTCCAAGCCAGATCCACCCCTCTACTGTTACCAAATCCACTCAGATTCCCAAATCTTCCACTT\t@@@DDFDFHHHHHJJJEHGGHIHHAEGHJJIJJFGGHGIDIGIJJ?BBGGGIIIJJIJGFHGIJEC(=3?C;?B9?@C>CECECAA(;;3>C#########\tXC:i:93\tXT:A:U\tNM:i:2\tSM:Z:raw_merged-1.2.3.4.5.6.7.8.bam\tAM:i:37\tX0:i:1\tX1:i:0\tXM:i:2\tXO:i:0\tXG:i:0\tMD:Z:10G36C45\tRG:Z:raw_merged-1.2.3.4.5.6.7.8.bam\tOQ:Z:#########C>3;;(AACECEC>C@?9B?;C?3=(CEJIGHFGJIJJIIIGGGBB?JJIGIDIGHGGFJJIJJHGEAHHIHGGHEJJJHHHHHFDFDD@@@",
"HWI-ST200R_251:6:2207:18561:163438#GCCAAT\t163\t20\t6181919\t60\t101M\t=\t6181938\t112\tCTGAGCACACCAAAATTCATCTACCACCTATATTGGTACCTCGATTGTTTTCCAAGCCAGATCCACACCTCTACTGTTACCAAATCCACTCAGATTCCCAA\t@CCFFDDFHHHHHIJJJIIJJJIJJIIJGJIIIJII?DGHIGHDGHIIIJIJIJIIDCHGIJIIGGHIFEHHHHFFFFFDC.6.66;@CCCDCCDC>CCCA\tXT:A:U\tNM:i:2\tSM:Z:raw_merged-1.2.3.4.5.6.7.8.bam\tAM:i:37\tX0:i:1\tX1:i:0\tXM:i:2\tXO:i:0\tXG:i:0\tMD:Z:29G36C34\tRG:Z:raw_merged-1.2.3.4.5.6.7.8.bam\tOQ:Z:@CCFFDDFHHHHHIJJJIIJJJIJJIIJGJIIIJII?DGHIGHDGHIIIJIJIJIIDCHGIJIIGGHIFEHHHHFFFFFDC.6.66;@CCCDCCDC>CCCA",
        ]
        self.__reducer.discard_duplicates = False
        sams = map(SAMMapping, test_case_data)
        left_key = "0020:000006181919:F"
        pairs = ( (sams[1], sams[0]), (sams[3], sams[2]) )

        # add the pairs to the context
        self.__ctx.add_value(left_key, proto.serialize_pair(pairs[0]))
        self.__ctx.add_value(left_key, proto.serialize_pair(pairs[1]))
        self.__reducer.reduce(self.__ctx)

        self.assertEqual(2, len(self.__ctx.emitted))
        self.assertEqual([2,2], map(len, self.__ctx.emitted.itervalues()))
        key = "HWI-ST200R_251:6:2207:18561:163438#GCCAAT"
        good_pair_sam = self.__ctx.emitted[key]
        for read in good_pair_sam:
            mapping = SAMMapping("\t".join( (key,read) ))
            self.assertFalse(mapping.is_duplicate())

        key = "HWI-ST200R_251:5:1208:19924:124635#GCCAAT"
        dup_pair_sam = self.__ctx.emitted[key]
        for read in dup_pair_sam:
            mapping = SAMMapping("\t".join( (key,read) ))
            self.assertTrue(mapping.is_duplicate())

    def test_fw_rev_missed_dup_pair(self):
        """
        Here we have two duplicate pairs, as detected by Picard.
        The reads 10364 have flags 'pPR1', 'pPr2'.
        The reads 138222 have flags 'pPR2d', 'pPr1d'.
        10364/1 has no trimming and is on the fw strand.  It should have a key position 88888399:F.
        10364/2 is also on the rev strand.  We expect a key position of 88888421+53 = 88888474:R.
        138222/1 is on the rev strand.  We expect a key position of 88888404+70 = 88888474:R.
        138222/2 has been trimmed but is on the fw strand. It should have a key position 88888399:F.
        """
        test_case_data = [
            "HWI-ST332_97:3:66:16214:10364#0\t99\t10\t88888399\t46\t76M\t=\t88888421\t76\tTGATTTTGCTCCATTGTCTTCTAGCTTGTGTTATGCCTGTTGAAAGTACAAAATCATTCTGGAAGCTTATCTATTG\tHGHHHHHHFHEHHHHGHHFHHHGGHHHHHHHHHHEHDHGEHFHHHHHHHGGHHHHHHHHHHBHBBEGG=GFFFF@F",
            "HWI-ST332_97:3:66:16214:10364#0\t147\t10\t88888421\t46\t22S54M\t=\t88888399\t-76\tTGATTCCGCTCCATGTGCCTCGAGCTTGTGTTATGCCTGTTGAAAGTACAAAATCATTCTGGAAGCTTATCTATTG\t#######################AA@EHGHEHHHHHHHHHHHHHHHEHHHHHHFHFHHHHHHHHHHHHHHHGHHFH",
            "HWI-ST332_97:3:7:10556:138222#0\t1107\t10\t88888404\t23\t5S71M\t=\t88888399\t-76\tTGATGTTGCTCCATTGTCTTCTAGCTTGTGTTATGCCTGTTGAAAGTACAAAATCATTCTGGAAGCTTATCTATTG\t######C<BCC:B,DDDC=BD3@CB8B?DBD@E@EEEEECED@CDB=8C7A@D=DEDEDCDBECDE<>;;,17,45",
            "HWI-ST332_97:3:7:10556:138222#0\t1187\t10\t88888399\t15\t50M26S\t=\t88888404\t76\tTGATTTTGCTCCATTGTCTTCTAGCTTGTGTTATGCCTGTTGAAAGTACAAAATCCGTCTGGTTGCTTCTATTTTG\tCC7EEFFF@FHHFHHG?GGF:>4.7GD8DC@D>CCFGG@GGG5GG4<CB###########################",
        ]
        sams = map(SAMMapping, test_case_data)
        pair1 = sams[0:2]
        pair2 = sams[2:]

        left_key = "0010:000088888399:F"
        self.__ctx.add_value(left_key, proto.serialize_pair(pair1))
        self.__ctx.add_value(left_key, proto.serialize_pair(pair2))
        self.__reducer.reduce(self.__ctx)

        # verify emitted data.  2 keys (one per read name) and 2 reads for each key
        self.assertEqual(2, len(self.__ctx.emitted))
        self.assertEqual([2,2], map(len, self.__ctx.emitted.itervalues()))
        key = "HWI-ST332_97:3:66:16214:10364#0"
        good_pair_sam = self.__ctx.emitted[key]
        for read in good_pair_sam:
            mapping = SAMMapping("\t".join( (key,read) ))
            self.assertFalse(mapping.is_duplicate())
        key = "HWI-ST332_97:3:7:10556:138222#0"
        dup_pair_sam = self.__ctx.emitted[key]
        for read in dup_pair_sam:
            mapping = SAMMapping("\t".join( (key,read) ))
            self.assertTrue(mapping.is_duplicate())

        self.setUp() # clean-up and repeat for the right key
        right_key = "0010:000088888474:R"
        self.__ctx.add_value(right_key, PAIR_STRING)
        self.__ctx.add_value(right_key, PAIR_STRING)
        self.__reducer.reduce(self.__ctx)
        # verify no data emitted
        self.assertEqual(0, len(self.__ctx.emitted))


    def test_rmdup_clipped_unpaired(self):
        test_case_data = [
"HWI-ST200R_251:5:1208:19924:124635#GCCAAT\t82\t20\t6181935\t60\t5S96M\t*\t0\t0\tAAGTGGAAGATTTGGGAATCTGAGTGGATTTGGTAACAGTAGAGGGGTGGATCTGGCTTGGAAAACAATCGAGGTACCAATATAGGTGGTAGATGAATTTT\t?<?AADADBFBF<EHIGHGGGEAF3AF<CHGGDG9?GHFFACDHH)?@AHEHHIIIIE>A=A:?);B27@;@?>,;;C(5::>>>@5:()4>@@@######\tXC:i:96",
"HWI-ST200R_251:6:2207:18561:163438#GCCAAT\t82\t20\t6181930\t60\t101M\t*\t0\t0\tAAGTGGAAGATTTGGGAATCTGAGTGGATTTGGTAACAGTAGAGGGGTGGATCTGGCTTGGAAAACAATCGAGGTACCAATATAGGTGGTAGATGAATTTT\t@CCFFDDFHHHHHIJJJIIJJJIJJIIJGJIIIJII?DGHIGHDGHIIIJIJIJIIDCHGIJIIGGHIFEHHHHFFFFFDC.6.66;@CCCDCCDC>CCCA",
        ]
        self.__reducer.discard_duplicates = False
        sams = map(SAMMapping, test_case_data)
        left_key = "0020:000006181930:R"
        pairs = ( (sams[0], None), (sams[1], None) )

        # add the pairs to the context
        self.__ctx.add_value(left_key, proto.serialize_pair(pairs[0]))
        self.__ctx.add_value(left_key, proto.serialize_pair(pairs[1]))
        self.__reducer.reduce(self.__ctx)

        self.assertEqual(2, len(self.__ctx.emitted))
        self.assertEqual([1,1], map(len, self.__ctx.emitted.itervalues()))

        key = "HWI-ST200R_251:6:2207:18561:163438#GCCAAT"
        good_read_sam = self.__ctx.emitted[key][0]
        mapping = SAMMapping("\t".join( (key,good_read_sam) ))
        self.assertFalse(mapping.is_duplicate())

        key = "HWI-ST200R_251:5:1208:19924:124635#GCCAAT"
        dup_read_sam = self.__ctx.emitted[key][0]
        mapping = SAMMapping("\t".join( (key,dup_read_sam) ))
        self.assertTrue(mapping.is_duplicate())


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
    #return unittest.TestLoader().loadTestsFromTestCase(TestSeqalReducer)
    print >> sys.stderr, "###############################################################################"
    print >> sys.stderr, "TestSeqalReducer disabled until SeqalReducer code is updated"
    print >> sys.stderr, "###############################################################################"
    return unittest.TestSuite()

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())
