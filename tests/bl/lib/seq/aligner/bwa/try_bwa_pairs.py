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

import sys, os, itertools
import numpy as np
import Bio.SeqIO
import bl.lib.seq.aligner.bwa as bwa
import testing_data as d
import testing_utilities as u


BWA_EXE = "../../../../../../bl/lib/seq/aligner/bwa/libsrc-0.5.7/bwa"


def generate_ref(root):
  f = open(root, 'w')
  f.write(d.refseq)
  f.close()
  os.system("%s index %s" % (BWA_EXE, root))


def read_seq_pairs(seq_reader, N):
  pairs = []
  i = 0
  while i < N:
    p = tuple(itertools.islice(seq_reader, 2))
    if len(p) == 0:
      break
    if len(p) == 1:
      raise ValueError("Uneven number of sequences in input stream!")
    pairs.append(p)
    i += 1
  return pairs

def print_sams(gopt, bns, n_seq, bwsa):
  for i in range(n_seq):
    print bwa.analyze_hit(gopt, bns, bwsa[0][i], bwsa[1][i])
    print bwa.analyze_hit(gopt, bns, bwsa[1][i], bwsa[0][i])

def process_sequences(bwts, bns, pacseq, seq_reader, N, analyze_seqs=None):
  gopt = bwa.gap_init_opt()
  popt = bwa.pe_init_opt()

  ii      = bwa.isize_info_t()
  last_ii = bwa.isize_info_t()
  last_ii.avg = -1.0

  while 1:
    pairs = read_seq_pairs(seq_reader, N)
    seq_pairs_read = len(pairs)
    if seq_pairs_read == 0:
      break
    bwsa = bwa.build_bws_array(pairs)
    bwa.cal_sa_reg_gap(0, bwts, seq_pairs_read, bwsa[0], gopt)
    bwa.cal_sa_reg_gap(0, bwts, seq_pairs_read, bwsa[1], gopt)
    cnt_chg = bwa.cal_pac_pos_pe(bwts, seq_pairs_read, bwsa,
                                 ii, popt, gopt, last_ii)
    sys.stderr.write('ii: %r\n' %[ii.avg, ii.std, ii.low, ii.high])
    bwa.paired_sw(bns, pacseq, seq_pairs_read, bwsa, popt, ii)
    bwa.refine_gapped(bns, seq_pairs_read, bwsa[0], pacseq)
    bwa.refine_gapped(bns, seq_pairs_read, bwsa[1], pacseq)
    analyze_seqs(gopt[0], bns, seq_pairs_read, bwsa)
    bwa.free_seq(N, bwsa[0])
    bwa.free_seq(N, bwsa[1])


def main(argv):
  try:
    hit_fn = argv[1]
  except IndexError:
    hit_fn = 'hit_pairs.fastq'
  N = 10
  root_name = 'refseq'
  generate_ref(root_name)
  bwts = bwa.restore_index(root_name)
  bnsp, pacseq = bwa.restore_reference(root_name)
  sys.stderr.write(str(bnsp[0]))
  seq_reader = Bio.SeqIO.parse(open(hit_fn), 'fastq-illumina')
  process_sequences(bwts, bnsp, pacseq, seq_reader, N, print_sams)
  u.clean_up_aux_files(root_name)

if __name__ == "__main__":
  main(sys.argv)
