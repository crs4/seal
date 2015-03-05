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
        expected_fname = os.path.join(os.path.dirname(__file__), 'rapi_mini_ref_seqs_sam_no_header.sam')
        with open(expected_fname) as f:
            expected_sam = f.read()
        self.assertEquals(expected_sam, sam)

    def _align_mini_ref_seqs(self):
        self.hi.load_ref(test_utils.MiniRefMemPath)
        reads = test_utils.get_mini_ref_seqs()
        for row in reads:
            if len(row) != 5:
                raise RuntimeError("Unexpected number of fields in mini_ref read record")
            self.hi.load_pair(*row)
        self.hi.align_batch()


class TestHiRapiBatch(unittest.TestCase):

    def setUp(self):
        self.hi = HiRapiAligner('rapi_bwa')
        reads = test_utils.get_mini_ref_seqs()
        for row in reads:
            if len(row) != 5:
                raise RuntimeError("Unexpected number of fields in mini_ref read record")
            self.hi.load_pair(*row)

    def tearDown(self):
        self.hi.release_resources()

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

def suite():
    """Get a suite with all the tests from this module"""
    s = unittest.TestLoader().loadTestsFromTestCase(TestHiRapiProperties)
    s.addTests(unittest.TestLoader().loadTestsFromTestCase(TestHiRapiAlignments))
    s.addTests(unittest.TestLoader().loadTestsFromTestCase(TestHiRapiBatch))
    return s

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())
