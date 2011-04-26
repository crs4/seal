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

import sys, itertools as it
import Bio.SeqIO

import bl.lib.seq.aligner.bwa as bwa
from bl.lib.seq.aligner.bwa.bwa_iterator import BWAIterator


def main(argv):
  try:
    refseq_fname = argv[1]
    read_fname = argv[2]
    mate_fname = argv[3]
  except IndexError:
    sys.exit("Usage: %s REFSEQ_FN READ_FN MATE_FN" % sys.argv[0])

  seq_list_len = 10000
  max_isize = pairing_batch_size = 10000
  gopt, popt = bwa.gap_init_opt(), bwa.pe_init_opt()

  read_flow = Bio.SeqIO.parse(open(read_fname), 'fastq-illumina')
  mate_flow = Bio.SeqIO.parse(open(mate_fname), 'fastq-illumina')
  pairs_flow = it.izip(read_flow, mate_flow)
  pairs = list(it.islice(pairs_flow, 0, seq_list_len))
  bwts = bwa.restore_index(refseq_fname)
  bnsp, pacseq = bwa.restore_reference(refseq_fname)

  l = len(pairs)
  bwsa = bwa.build_bws_array(pairs)

  bwa_iterator = BWAIterator(refseq_fname, gopt, popt, max_isize,
                             pairing_batch_size)
  pairs = [p for p in bwa_iterator.analyze(bwsa, l)]
  print "READ POS GAPO GAPE MM STRAND SCORE CIGAR"
  for read, mate in pairs:
    if read.n_multi > 0:
      print
      multi_list = [m for m in read.itermulti()]
      for m in multi_list:
        print read.get_name(), m.pos, m.n_gapo, m.n_gape, m.n_mm, m.strand, \
               m.score, m.get_cigar(read.len)

  for j in 0, 1:
    bwa.free_seq(l, bwsa[j])
  bwa.bns_destroy(bwa_iterator.bnsp)


if __name__ == "__main__":
  main(sys.argv)
