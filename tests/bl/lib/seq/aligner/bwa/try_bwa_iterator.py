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

import sys, itertools as it
import Bio.SeqIO
import logging
logging.basicConfig(level=logging.DEBUG)

import bl.lib.seq.aligner.bwa as bwa
from bl.lib.seq.aligner.bwa.bwa_iterator import BWAIterator, MRVisitor


def get_counters():
  counter_names = [
    "restore_index",
    "restore_reference",
    "cal_sa_reg_gap",
    "cal_pac_pos_pe",
    "paired_sw",
    "paired_sw_batch",
    "refine_gapped"]
  counters = {}
  for cn in counter_names:
    counters[cn] = CounterStub()
  return counters


class CounterStub(object):

  def __init__(self, value=0):
    self.value = value

  def increment(self, inc):
    self.value += inc


class ContextStub(object):

  def incrementCounter(self, counter, inc):
    counter.increment(inc)
    
  def setStatus(self, status):
    sys.stderr.write("Status set to '%s'\n" % status)


def main(argv):

  try:
    refseq_fname = argv[1]
    read_fname = argv[2]
    mate_fname = argv[3]
  except IndexError:
    sys.exit("Usage: %s REFSEQ_FN READ_FN MATE_FN" % sys.argv[0])

  seq_list_len = 5000
  max_isize = pairing_batch_size = 1000
  gopt, popt = bwa.gap_init_opt(), bwa.pe_init_opt()

  read_flow = Bio.SeqIO.parse(open(read_fname), 'fastq-illumina')
  mate_flow = Bio.SeqIO.parse(open(mate_fname), 'fastq-illumina')
  pairs_flow = it.izip(read_flow, mate_flow)
  res = []
  while 1:
    pairs = list(it.islice(pairs_flow, 0, seq_list_len))
    if len(pairs) == 0:
      break
    bwts = bwa.restore_index(refseq_fname)
    bnsp, pacseq = bwa.restore_reference(refseq_fname)

    l = len(pairs)
    bwsa = bwa.build_bws_array(pairs)

    logger = logging.getLogger("test")
    logger.setLevel(logging.DEBUG)
    counters = get_counters()
    ctx = ContextStub()
    visitor = MRVisitor(logger, ctx, counters)
    
    bwa_iterator = BWAIterator(refseq_fname, gopt, popt, max_isize,
                               pairing_batch_size, visitor)
    for read, mate in bwa_iterator.analyze(bwsa, l):
      print read.get_name(), mate.get_name()

    for j in 0, 1:
      bwa.free_seq(l, bwsa[j])
    bwa.bns_destroy(bwa_iterator.bnsp)

  for cn, c in counters.iteritems():
    sys.stderr.write("%s = %d\n" % (cn, c.value))
    

if __name__ == "__main__":
  main(sys.argv)
