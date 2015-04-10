# Copyright (C) 2011-2015 CRS4.
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

import logging
import os
import shutil
import sys
import tarfile
import tempfile
import unittest

from pydoop.mapreduce.api import JobConf

from seal.seqal.mapper import mapper
from seal.lib.mr.test_utils import map_context
import tseal.test_utils as tseal_utils

class sam_map_context(map_context):
    def __init__(self, jc, input_split):
        super(sam_map_context, self).__init__(jc, input_split)
        self._sam_lines = []

    @property
    def sam_lines(self):
        return self._sam_lines

    def emit(self, k, v):
        if not isinstance(k, str):
          raise TypeError("key must be a string (it's a %s)" % type(k))
        if not isinstance(v, str):
          raise TypeError("value must be a string (it's a %s)" % type(v))
        self._sam_lines.append("%s\t%s" % (k, v))

class TestSeqalMapper(unittest.TestCase):

    def setUp(self):
        self._map_batch_size = 6
        self._log = logging.getLogger(__name__)
        self._old_cwd = os.getcwd()
        self._jc = JobConf([
            'seal.seqal.log.level', 'DEBUG',
            'seal.seqal.fastq-subformat', 'fastq-sanger',
            'seal.input-format', 'prq',
            'seal.output-format', 'sam',
            #'seal.seqal.alignment.max.isize', None,
            #'seal.seqal.alignment.min.isize', None,
            'seal.seqal.pairing.batch.size', self._map_batch_size,
            'seal.seqal.min_hit_quality', 0,
            'seal.seqal.remove_unmapped', False,
            'seal.seqal.nthreads', 1,
            'seal.seqal.trim.qual', 0,
            'mapred.reduce.tasks', 0,
            'mapred.cache.archives',
                (os.path.join("file://", tseal_utils.MiniRefMemDir, "mini_ref_bwamem_0.7.8.tar") + "#reference"),
         ])
        self._things_to_clean_up = []

        workdir = tempfile.mkdtemp("seqal_mapper_test_workdir")
        self._things_to_clean_up.append(workdir)
        os.chdir(workdir)
        self._ctx = sam_map_context(self._jc, [])

        try:
            self._setup_ref(self._jc.get('mapred.cache.archives'))
            self._mapper = mapper(self._ctx)
        except StandardError:
            # call tearDown ourselves because unittest doesn't call it if setUp fails
            self.tearDown()
            raise

    def tearDown(self):
        try:
            for item in self._things_to_clean_up:
                try:
                    shutil.rmtree(item)
                except StandardError as e:
                    self._log.info("Failed to remove %s", item)
                    self._log.info("Error: %s", e)
        finally:
            os.chdir(self._old_cwd)

    def _setup_ref(self, mr_cache_archives):
        self._log.info("Setting up reference using property value '%s'", mr_cache_archives)
        archive, link = mr_cache_archives.split('#')
        ar = tarfile.TarFile(archive)
        ar.extractall('.')
        os.symlink('.', link)
        self._log.info("Here is the listing of the extraction directory: %s", ', '.join(os.listdir('.')))

    def test_simple_map(self):
        # get input data and expected output.  We keep exactly one map batch of reads
        # (as per self._map_batch_size)
        reads = tseal_utils.get_mini_ref_seqs()[0:(self._map_batch_size / 2)]
        expected_output = sorted(tseal_utils.rapi_mini_ref_seqs_sam_no_header().split('\n')[0:2*len(reads)])
        self._log.info("loaded %s fragments and %s lines of expected output", len(reads), len(expected_output))
        if len(reads) * 2 < self._mapper.batch_size:
            self.fail("batch size for test (%s) is set larger than the number of available "
                "reads (%s). Aligner won't run" % (self._mapper.batch_size, len(reads) * 2))
        for idx, fragment in enumerate(reads):
            self._ctx.set_input_key(idx * 100)
            self._ctx.set_input_value('\t'.join(fragment))
            self._mapper.map(self._ctx)

        produced_sam = sorted(self._ctx.sam_lines)
        self.assertEquals(len(expected_output), len(produced_sam))
        self.assertEquals(expected_output, produced_sam)
        self.assertEquals(len(reads) * 2, self._ctx.counters["SEQAL:EMITTED SAM RECORDS"])

def suite():
    return unittest.TestLoader().loadTestsFromTestCase(TestSeqalMapper)

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())
