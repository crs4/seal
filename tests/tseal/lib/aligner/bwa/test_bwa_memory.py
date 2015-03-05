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

"""
How to use this test:

You need to provide as command line arguments:
  1. The fasta reference sequence file name
  2, 3. The two reads and mates sequence files

use LOG_LEVEL = logging.DEBUG; N_ITER = 1 to see details.
use LOG_LEVEL = logging.INFO; N_ITER = 10 to check for leaks.
"""

import sys, gc, logging
#print gc.isenabled()
gc.set_debug(gc.DEBUG_UNCOLLECTABLE)

import itertools as it
import seal.lib.aligner.bwa as bwa

import Bio.SeqIO
from seal.lib.util.meminfo import meminfo


MB = float(2**20)


def print_meminfo(status, log_function=logging.debug):
  minfo = meminfo()
  msg = "%s: size = %.1fm; resident = %.1fm" % (
    status, minfo["size"]/MB, minfo["resident"]/MB)
  log_function(msg)


def run_bwa_py_sampe_alloc_only(refseq_fname, read_fname, mate_fname):
  read_flow = Bio.SeqIO.parse(open(read_fname), 'fastq-illumina')
  mate_flow = Bio.SeqIO.parse(open(mate_fname), 'fastq-illumina')
  pairs = [x for x in it.izip(read_flow, mate_flow)]
  print_meminfo("AFTER READING PAIRS")
  bwsa = bwa.build_bws_array(pairs)
  print_meminfo("AFTER BUILDING BWSA")
  bwts = bwa.restore_index(refseq_fname)
  print_meminfo("AFTER RESTORING INDEX")
  bnsp, pacseq = bwa.restore_reference(refseq_fname)
  print_meminfo("AFTER RESTORING REFERENCE")
  gopt, popt = bwa.gap_init_opt(), bwa.pe_init_opt()
  ii, last_ii = bwa.isize_info_t(), bwa.isize_info_t()
  last_ii.avg = -1.0
  l = len(pairs)
  print_meminfo("AFTER INIT OPT & II")
  # deallocate seq & ref data
  for i in 0, 1:
    bwa.free_seq(l, bwsa[i])
    bwa.bwt_destroy(bwts[i])
  bwa.bns_destroy(bnsp)
  print_meminfo("AFTER DEALLOC")
  del pacseq
  n_unreachable = gc.collect()
  logging.debug("n_unreachable = %d" % n_unreachable)
  print_meminfo("AFTER DEL PACSEQ")
  del pairs
  n_unreachable = gc.collect()
  logging.debug("n_unreachable = %d" % n_unreachable)
  print_meminfo("AFTER DEL PAIRS")


def run_bwa_py_sampe(refseq_fname, read_fname, mate_fname):
  read_flow = Bio.SeqIO.parse(open(read_fname), 'fastq-illumina')
  mate_flow = Bio.SeqIO.parse(open(mate_fname), 'fastq-illumina')
  pairs = [x for x in it.izip(read_flow, mate_flow)]
  print_meminfo("AFTER READING PAIRS")
  bwsa = bwa.build_bws_array(pairs)
  print_meminfo("AFTER BUILDING BWSA")
  bwts = bwa.restore_index(refseq_fname)
  print_meminfo("AFTER RESTORING INDEX")
  bnsp, pacseq = bwa.restore_reference(refseq_fname)
  print_meminfo("AFTER RESTORING REFERENCE")
  gopt, popt = bwa.gap_init_opt(), bwa.pe_init_opt()
  ii, last_ii = bwa.isize_info_t(), bwa.isize_info_t()
  last_ii.avg = -1.0
  l = len(pairs)
  print_meminfo("AFTER INIT OPT & II")
  bwa.cal_sa_reg_gap(0, bwts, l, bwsa[0], gopt)
  bwa.cal_sa_reg_gap(0, bwts, l, bwsa[1], gopt)
  print_meminfo("AFTER CAL_SA_REG_GAP")
  cnt_chg = bwa.cal_pac_pos_pe(bwts, l, bwsa, ii, popt, gopt, last_ii)
  print_meminfo("AFTER CAL_PAC_POS_PE")
  bwa.paired_sw(bnsp, pacseq, l, bwsa, popt, ii)
  print_meminfo("AFTER PAIRED_SW")
  bwa.refine_gapped(bnsp, l, bwsa[0], pacseq)
  bwa.refine_gapped(bnsp, l, bwsa[1], pacseq)
  print_meminfo("AFTER REFINE_GAPPED")
  for k in xrange(l):
    v1 = bwa.analyze_hit(gopt[0], bnsp, bwsa[0][k], bwsa[1][k])
    v2 = bwa.analyze_hit(gopt[0], bnsp, bwsa[1][k], bwsa[0][k])
  print_meminfo("AFTER ANALYZE_HIT")
  # deallocate seq & ref data
  for i in 0, 1:
    bwa.free_seq(l, bwsa[i])
    bwa.bwt_destroy(bwts[i])
  bwa.bns_destroy(bnsp)
  print_meminfo("AFTER DEALLOC")
  del pacseq
  n_unreachable = gc.collect()
  logging.debug("n_unreachable = %d" % n_unreachable)
  print_meminfo("AFTER DEL PACSEQ")
  del pairs
  n_unreachable = gc.collect()
  logging.debug("n_unreachable = %d" % n_unreachable)
  print_meminfo("AFTER DEL PAIRS")


def main(argv):
  LOG_LEVEL = logging.DEBUG; N_ITER = 1
  #LOG_LEVEL = logging.INFO; N_ITER = 10
  logging.basicConfig(level=LOG_LEVEL)
  #fun = run_bwa_py_sampe_alloc_only
  fun = run_bwa_py_sampe

  print_meminfo("START", logging.info)

  try:
    refseq_fn = argv[1]
    read_fn = argv[2]
    mate_fn = argv[3]
  except IndexError:
    sys.exit("Usage: %s REFSEQ_FN READ_FN MATE_FN" % sys.argv[0] + __doc__)
  #u.build_index(refseq_fn)
  for i in xrange(N_ITER):
    fun(refseq_fn, read_fn, mate_fn)
    print_meminfo("END ITERATION %d" % i, logging.info)


if __name__ == "__main__":
  main(sys.argv)
