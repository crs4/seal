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

Change SEQ_LIST_LEN_RANGE as needed -- the test measures peak memory
and number of failed inferences of insert size in function of the
length of the sequence list.
"""

import sys, gc, logging
#print gc.isenabled()
gc.set_debug(gc.DEBUG_UNCOLLECTABLE)

import itertools as it
import bl.lib.seq.aligner.bwa as bwa

import Bio.SeqIO
from bl.lib.util.meminfo import meminfo


MB = float(2**20)


def print_meminfo(status, log_function=logging.debug):
  minfo = meminfo()
  size = minfo["size"]/MB
  resident = minfo["resident"]/MB
  msg = "%s: size = %.1fm; resident = %.1fm" % (status, size, resident)
  log_function(msg)
  return size, resident


def run_bwa_py_sampe(refseq_fname, read_fname, mate_fname, seq_list_len=None):
  size_list = []
  resident_list = []
  failed_ii = 0

  read_flow = Bio.SeqIO.parse(open(read_fname), 'fastq-illumina')
  mate_flow = Bio.SeqIO.parse(open(mate_fname), 'fastq-illumina')

  #pairs = [x for x in it.izip(read_flow, mate_flow)]
  pairs_flow = it.izip(read_flow, mate_flow)

  while 1:
    pairs = list(it.islice(pairs_flow, 0, seq_list_len))
    if len(pairs) == 0:
      break
    size, resident = print_meminfo("AFTER READING PAIRS")
    size_list.append(size)
    resident_list.append(resident)

    bwsa = bwa.build_bws_array(pairs)
    size, resident = print_meminfo("AFTER BUILDING BWSA")
    size_list.append(size)
    resident_list.append(resident)

    bwts = bwa.restore_index(refseq_fname)
    size, resident = print_meminfo("AFTER RESTORING INDEX")
    size_list.append(size)
    resident_list.append(resident)

    bnsp, pacseq = bwa.restore_reference(refseq_fname)
    size, resident = print_meminfo("AFTER RESTORING REFERENCE")
    size_list.append(size)
    resident_list.append(resident)

    gopt, popt = bwa.gap_init_opt(), bwa.pe_init_opt()
    ii, last_ii = bwa.isize_info_t(), bwa.isize_info_t()
    last_ii.avg = -1.0
    l = len(pairs)
    size, resident = print_meminfo("AFTER INIT OPT & II")
    size_list.append(size)
    resident_list.append(resident)

    bwa.cal_sa_reg_gap(0, bwts, l, bwsa[0], gopt)
    bwa.cal_sa_reg_gap(0, bwts, l, bwsa[1], gopt)
    size, resident = print_meminfo("AFTER CAL_SA_REG_GAP")
    size_list.append(size)
    resident_list.append(resident)

    cnt_chg = bwa.cal_pac_pos_pe(bwts, l, bwsa, ii, popt, gopt, last_ii)
    size, resident = print_meminfo("AFTER CAL_PAC_POS_PE")
    size_list.append(size)
    resident_list.append(resident)

    #sys.stderr.write("ii=%f\n" % ii.avg)
    if ii.avg < 0.0:
      failed_ii += 1

    bwa.paired_sw(bnsp, pacseq, l, bwsa, popt, ii)
    size, resident = print_meminfo("AFTER PAIRED_SW")
    size_list.append(size)
    resident_list.append(resident)

    bwa.refine_gapped(bnsp, l, bwsa[0], pacseq)
    bwa.refine_gapped(bnsp, l, bwsa[1], pacseq)
    size, resident = print_meminfo("AFTER REFINE_GAPPED")
    size_list.append(size)
    resident_list.append(resident)

    for k in xrange(l):
      v1 = bwa.analyze_hit(gopt[0], bnsp, bwsa[0][k], bwsa[1][k])
      v2 = bwa.analyze_hit(gopt[0], bnsp, bwsa[1][k], bwsa[0][k])
    size, resident = print_meminfo("AFTER ANALYZE_HIT")
    size_list.append(size)
    resident_list.append(resident)

    # deallocate seq & ref data
    for i in 0, 1:
      bwa.free_seq(l, bwsa[i])
      bwa.bwt_destroy(bwts[i])
    bwa.bns_destroy(bnsp)
    size, resident = print_meminfo("AFTER DEALLOC")
    size_list.append(size)
    resident_list.append(resident)

    del pacseq
    n_unreachable = gc.collect()
    logging.debug("n_unreachable = %d" % n_unreachable)
    size, resident = print_meminfo("AFTER DEL PACSEQ")
    size_list.append(size)
    resident_list.append(resident)

    del pairs
    n_unreachable = gc.collect()
    logging.debug("n_unreachable = %d" % n_unreachable)
    size, resident = print_meminfo("AFTER DEL PAIRS")
    size_list.append(size)
    resident_list.append(resident)

  return max(size_list), max(resident_list), failed_ii


def main(argv):
  LOG_LEVEL = logging.INFO
  logging.basicConfig(level=LOG_LEVEL)
  SEQ_LIST_LEN_RANGE = xrange(1000, 10001, 1000)

  try:
    refseq_fn = argv[1]
    read_fn = argv[2]
    mate_fn = argv[3]
  except IndexError:
    sys.exit("Usage: %s REFSEQ_FN READ_FN MATE_FN" % sys.argv[0] + __doc__)
  #u.build_index(refseq_fn)
  for seq_list_len in SEQ_LIST_LEN_RANGE:
    print_meminfo("\nSTART seq_list_len=%d" % seq_list_len, logging.info)
    max_size, max_resident, failed_ii = run_bwa_py_sampe(
      refseq_fn, read_fn, mate_fn, seq_list_len=seq_list_len
      )
    print_meminfo("END seq_list_len=%d" % seq_list_len, logging.info)
    logging.info(
      "PEAK seq_list_len=%d: size=%.1f, resident=%.1f, failed_ii=%d" %
                 (seq_list_len, max_size, max_resident, failed_ii))


if __name__ == "__main__":
  main(sys.argv)
