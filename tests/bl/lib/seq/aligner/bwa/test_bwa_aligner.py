#!/usr/bin/env python
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


# Usage: test_bwa_aligner.py <genome reference - part common to all files>

from itertools import izip
import fileinput
import os
import StringIO
import sys
import unittest

from bl.lib.seq.aligner.bwa.bwa_aligner import BwaAligner
from bl.lib.seq.aligner.io.sam_formatter import SamFormatter
import bl.lib.seq.aligner.bwa.bwa_core as bwa
import testing_utilities as utils


class TestBwaAligner(unittest.TestCase):
  REFERENCE = os.path.join(os.path.dirname(__file__), 'fixtures/foobar.fa')
  BUILD_REFERENCE = True

  class SimpleVisitor(object):
    def __init__(self):
      self.sam = SamFormatter()
      self.output = StringIO.StringIO()

    def process(self, pair):
      for hit in pair:
        print >>self.output, self.sam.format(hit)

  def setUp(self):
    utils.build_ref_index()
    self.aligner = BwaAligner()
    self.aligner.reference = utils.reference
    self.aligner.hit_visitor = type(self).SimpleVisitor()

    self.pairs = []
    with open(utils.get_fixture_path("pairs.txt")) as f:
      for line in f:
        if not line.startswith("#"): # leave #-lines for comments
          self.pairs.append(line.rstrip("\r\n").split("\t"))

  def tearDown(self):
    utils.remove_ref_index()

  def test_load_clear_batch(self):
    for row in self.pairs:
      self.aligner.load_pair_record(row)
    self.assertEqual(len(self.pairs), self.aligner.get_batch_size())
    self.aligner.clear_batch()
    self.assertEqual(0, self.aligner.get_batch_size())

  def test_defaults(self):
    self.assertEqual("fastq-illumina", self.aligner.qformat)
    self.assertEqual(1000, self.aligner.max_isize)
    self.assertEqual(1, self.aligner.nthreads)
    self.assertEqual(0, self.aligner.trim_qual)

  def test_alignment(self):
    for row in self.pairs:
      self.aligner.load_pair_record(row)
    self.aligner.run_alignment()
    # TODO:  write a more useful test, but for that we'll need a complete test fixture
    self.assertTrue( len(self.aligner.hit_visitor.output.getvalue()) > 0 )

  def test_alignment_mmap(self):
    self.aligner.mmap_enabled = True
    # Generate the .sax and .rsax indices. They will be removed by tearDown.
    bwa.make_suffix_arrays_for_mmap(utils.reference)
    for row in self.pairs:
      self.aligner.load_pair_record(row)
    self.aligner.run_alignment()
    # TODO:  write a more useful test, but for that we'll need a complete test fixture
    self.assertTrue( len(self.aligner.hit_visitor.output.getvalue()) > 0 )

  def test_missing_mmap_index(self):
    self.aligner.mmap_enabled = True
    for row in self.pairs:
      self.aligner.load_pair_record(row)
    self.assertRaises(ValueError, self.aligner.run_alignment)


def suite():
  """Get a suite with all the tests from this module"""
  return unittest.TestLoader().loadTestsFromTestCase(TestBwaAligner)

if __name__ == '__main__':
  #if len(sys.argv) > 1:
  #  TestBwaAligner.REFERENCE = sys.argv[1]
  #  TestBwaAligner.BUILD_REFERENCE = False
  unittest.TextTestRunner(verbosity=2).run(suite())
