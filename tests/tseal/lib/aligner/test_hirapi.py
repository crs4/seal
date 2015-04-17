#!/usr/bin/env python

import os
from StringIO import StringIO
import sys
import unittest

import pyrapi
import tseal.test_utils as test_utils
from seal.lib.aligner.hirapi import HiRapiAligner, HiRapiOpts

class TestHiRapiProperties(unittest.TestCase):

    def setUp(self):
        self.hi = HiRapiAligner('rapi_bwa')

    def tearDown(self):
        self.hi.release_resources()

    def test_defaults(self):
        self.assertTrue(self.hi.paired)
        self.assertEqual(pyrapi.rapi.QENC_SANGER, self.hi.q_offset)

    def test_get_plugin_info(self):
        self.assertEquals('bwa-mem', self.hi.aligner_name)
        self.assertTrue(self.hi.aligner_version)
        self.assertTrue(self.hi.plugin_version)

    def test_set_some_options(self):
        self.hi.opts.n_threads = 11
        self.assertEquals(11, self.hi.opts.n_threads)

        self.hi.opts.mapq_min = 5
        self.assertEquals(5, self.hi.opts.mapq_min)

        self.hi.opts.isize_min = 250
        self.assertEquals(250, self.hi.opts.isize_min)

        self.hi.opts.isize_max = 500
        self.assertEquals(500, self.hi.opts.isize_max)

class TestHiRapiAlignments(unittest.TestCase):

    def setUp(self):
        self.hi = HiRapiAligner('rapi_bwa')
        self._align_mini_ref_seqs()

    def tearDown(self):
        self.hi.release_resources()

    def test_load_reference_again(self):
        # should "just work"
        self.hi.load_ref(test_utils.MiniRefMemPath)

    def test_sam(self):
        io = StringIO()
        self.hi.write_sam(io, include_header=False)
        sam = io.getvalue()
        expected_sam = test_utils.rapi_mini_ref_seqs_sam_no_header()
        self.assertEquals(expected_sam, sam)

    def _align_mini_ref_seqs(self):
        self.hi.load_ref(test_utils.MiniRefMemPath)
        reads = test_utils.get_mini_ref_seqs()
        for row in reads:
            if len(row) != 5:
                raise RuntimeError("Unexpected number of fields in mini_ref read record")
            self.hi.load_pair(*row)
        self.hi.align_batch()

    def test_multiple_batches(self):
        io = StringIO()
        # we clear the batch created by setUp and align more reads using the same instance.
        # For each pair we clear the batch, load it, align it and generate sam.
        reads = test_utils.get_mini_ref_seqs()
        for row in reads:
            if len(row) != 5:
                raise RuntimeError("Unexpected number of fields in mini_ref read record")
            self.hi.clear_batch()
            self.hi.load_pair(*row)
            self.hi.align_batch()
            self.hi.write_sam(io, include_header=False)
            io.write('\n')
        sam = io.getvalue().rstrip('\n')
        expected_sam = test_utils.rapi_mini_ref_seqs_sam_no_header()
        self.assertEquals(expected_sam, sam)


class TestHiRapiBatch(unittest.TestCase):

    def setUp(self):
        self.hi = HiRapiAligner('rapi_bwa')
        self.reads = test_utils.get_mini_ref_seqs()
        for row in self.reads:
            if len(row) != 5:
                raise RuntimeError("Unexpected number of fields in mini_ref read record")
            self.hi.load_pair(*row)

    def tearDown(self):
        self.hi.release_resources()

    @unittest.skip("haven't decided whether we should support unicode input")
    def test_unicode_strings(self):
        self.hi.clear_batch()
        self.hi.load_pair(
            u'my_read_id',
            u'AAAACTGACCCACACAGAAAAACTAATTGTGAGAACCAATATTATACTAAATTCATTTGA',
            u'EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE',
            u'CAAAAGTTAACCCATATGGAATGCAATGGAGGAAATCAATGACATATCAGATCTAGAAAC',
            u'EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE')
        frag = next(f for f in self.hi.ifragments())
        reads = [ r for r in frag ]
        self.assertEquals('my_read_id', reads[0].id)
        self.assertEquals('my_read_id', reads[1].id)
        self.assertEquals('AAAACTGACCCACACAGAAAAACTAATTGTGAGAACCAATATTATACTAAATTCATTTGA', reads[0].seq)
        self.assertEquals('EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE', reads[0].qual)


    def test_fragment_iteration(self):
        read_id_counts = dict()
        for frag in self.hi.ifragments():
            for read in frag:
                read_id = read.id
                read_id_counts[read_id] = 1 + read_id_counts.get(read_id, 0)

        # 5 pairs
        self.assertEquals(5, len(read_id_counts))
        unique_counts = set(read_id_counts.values())
        # all ids appearing twice
        self.assertEquals(1, len(unique_counts))
        self.assertEquals(2, unique_counts.pop())

    def test_batch_management(self):
        self.assertEquals(10, self.hi.batch_size)
        self.hi.clear_batch()
        self.assertEquals(0, self.hi.batch_size)
        self.hi.load_ref(test_utils.MiniRefMemPath)
        self.hi.align_batch() # should not raise just because it's empty

        for _ in self.hi.ifragments():
            self.fail("iterating over an empty batch!")

    def test_base_quality(self):
        hi = HiRapiAligner('rapi_bwa', paired=False)
        one_read = self.reads[0][0:3]
        hi.q_offset = self.hi.Qenc_Sanger
        hi.load_read('sanger_read', one_read[1], one_read[2])

        # 64:  Illumina base quality offset
        # 33:  Sanger base quality offset
        ill_quality = ''.join( chr(ord(c) + (64-33)) for c in one_read[2] )
        hi.q_offset = self.hi.Qenc_Illumina
        hi.load_read('illumina_read', one_read[1], ill_quality)

        loaded_qualities = [ frag[0].qual for frag in hi.ifragments() ]
        self.assertEquals(2, len(loaded_qualities))
        self.assertEquals(loaded_qualities[0], loaded_qualities[1])


def suite():
    """Get a suite with all the tests from this module"""
    s = unittest.TestLoader().loadTestsFromTestCase(TestHiRapiProperties)
    s.addTests(unittest.TestLoader().loadTestsFromTestCase(TestHiRapiAlignments))
    s.addTests(unittest.TestLoader().loadTestsFromTestCase(TestHiRapiBatch))
    return s

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())
