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

import logging
logging.basicConfig(level=logging.DEBUG)
import unittest, time, math, itertools as it
import os
import re

import Bio.SeqIO
from Bio.Alphabet import single_letter_alphabet
from Bio.Seq import Seq
from Bio.SeqRecord import SeqRecord
import bl.lib.seq.generator as sg

import bl.lib.seq.aligner.bwa as bwa
from bl.lib.seq.generator import reverse_complement
import testing_utilities as u
import testing_data as d


class bwa_base_tc(unittest.TestCase):

  def setUp(self):
    self.refseq_fn, self.read_fn, self.mate_fn = 'full_match', 'read', 'mate'

  def tearDown(self):
    for fn in [self.refseq_fn, self.read_fn, self.mate_fn]:
      u.clean_up_aux_files(fn)

  def __do_pair_align_and_comparison(self, log_level=logging.WARNING,
                                     pairing_batch_size=None,
                                     seq_list_len=None,
                                     fastq_subfmt="fastq-sanger"):
    u.build_index(self.refseq_fn)
    #--
    start = time.time()
    bwa_res = u.run_bwa_sampe(self.refseq_fn, self.read_fn, self.mate_fn)
    print 'BWA_SAMPE: %f secs' % (time.time() - start)
    #--
    start = time.time()
    py_res =  u.run_bwa_py_sampe(self.refseq_fn, self.read_fn, self.mate_fn,
                                 log_level, pairing_batch_size, seq_list_len,
                                 fastq_subfmt)
    print 'PYBWA_SAMPE: %f secs' % (time.time() - start)
    #--
    self.assertEqual(len(bwa_res), len(py_res))
    for o, n in it.izip(bwa_res, py_res):
      for k in o.keys():
        if k == 'name':
          nn = n[k].rsplit("/", 1)[0]  # we are dropping the '/[12]' extension
          self.assertEqual(o[k], nn)
        elif k == 'aux':
          self.assertEqual(len(o[k]), len(n[k]))
          for x,y in it.izip(o[k], n[k]):
            self.assertEqual(x, y)
        else:
          self.assertEqual(o[k],n[k])

  def hand_picked(self):
    u.create_seq_file(self.refseq_fn, d.refseq, qtype="fasta")
    u.create_seq_file(self.read_fn, d.hit_1_seq, qtype="fastq-sanger")
    u.create_seq_file(self.mate_fn, d.hit_2_seq, qtype="fastq-sanger")
    self.__do_pair_align_and_comparison()

  def hand_picked_real_1(self):
    u.create_seq_file(self.refseq_fn, d.chr22_chunk, qtype="fasta")
    for t, f in (("read", self.read_fn), ("mate", self.mate_fn)):
      seq = getattr(d, "real_case_1_%s" % t)
      u.create_seq_file(f, seq, qtype="fastq-illumina")
    self.__do_pair_align_and_comparison(fastq_subfmt="fastq-illumina")

  def hand_picked_real_2(self):
    u.create_seq_file(self.refseq_fn, d.mm_chr17_chunk, qtype="fasta")
    for t, f in (("read", self.read_fn), ("mate", self.mate_fn)):
      seq = getattr(d, "real_case_2_%s" % t)
      u.create_seq_file(f, seq, qtype="fastq-sanger")
    self.__do_pair_align_and_comparison(fastq_subfmt="fastq-sanger")

  def auto_generated(self):
    u.create_seq_file(self.refseq_fn, d.refseq, qtype="fasta")
    n_seq, seq_len, span = 1000, 75, 500
    refseq = Bio.SeqIO.parse(open(self.refseq_fn), 'fasta').next()
    pairs = u.generate_pairs(refseq.seq, n_seq, seq_len, span=span)
    for i, fn in enumerate([self.read_fn, self.mate_fn]):
      fp = open(fn, 'w')
      Bio.SeqIO.write([x[i] for x in pairs], fp, 'fastq-illumina')
      fp.close()
      fp = open("%s.sanger" % fn, 'w')
      Bio.SeqIO.write([x[i] for x in pairs], fp, 'fastq-sanger')
      fp.close()
    self.__do_pair_align_and_comparison(fastq_subfmt="fastq-illumina")

  def batch_pairing(self):
    u.create_seq_file(self.refseq_fn, d.refseq, qtype="fasta")
    n_seq, seq_len, span = 1000, 75, 500
    refseq = Bio.SeqIO.parse(open(self.refseq_fn), 'fasta').next()
    pairs = u.generate_pairs(refseq.seq, n_seq, seq_len, span=span)
    for i, fn in enumerate([self.read_fn, self.mate_fn]):
      fp = open(fn, 'w')
      Bio.SeqIO.write([x[i] for x in pairs], fp, 'fastq-illumina')
      fp.close()
      fp = open("%s.sanger" % fn, 'w')
      Bio.SeqIO.write([x[i] for x in pairs], fp, 'fastq-sanger')
      fp.close()
    pairing_batch_size = 101
    self.__do_pair_align_and_comparison(pairing_batch_size=pairing_batch_size,
                                        fastq_subfmt="fastq-illumina")

  def multi_ref(self):
    refseq = d.refseq + d.chr22_chunk
    u.create_seq_file(self.refseq_fn, refseq, qtype="fasta")
    seq_len, span = 75, 500
    f = open(self.refseq_fn)
    raw_seqs = [r.seq.tostring() for r in Bio.SeqIO.parse(f, 'fasta')]
    f.close()
    raw_refseq = "".join(raw_seqs)
    starts = [
      0,                             # start of full refseq
      len(raw_seqs[0]) - seq_len/2,  # bridges the two seqs
      len(raw_refseq) - span         # end of full refseq
      ]
    pairs = u.generate_localized_pairs(raw_refseq, starts, seq_len, span=span)
    for i, fn in enumerate([self.read_fn, self.mate_fn]):
      fp = open(fn, 'w')
      Bio.SeqIO.write([x[i] for x in pairs], fp, 'fastq-illumina')
      fp.close()
      fp = open("%s.sanger" % fn, 'w')
      Bio.SeqIO.write([x[i] for x in pairs], fp, 'fastq-sanger')
      fp.close()
    self.__do_pair_align_and_comparison(fastq_subfmt="fastq-illumina")

  def bioseq_to_bwa_seq(self):
    nseq = 100
    for fmt in "fastq-sanger", "fastq-illumina", "fastq-solexa":
      qkey = "solexa_quality" if fmt == "fastq-solexa" else "phred_quality"
      for unknown in False, True:
        g = u.random_reads_generator(nseq, fmt=fmt, unknown=unknown)
        for i, (seq, q) in enumerate(g):
          name = "foo-%d" % i
          seq_str = "".join(seq)
          bioseq = SeqRecord(Seq(seq_str, single_letter_alphabet),
                             id=name, name=name, description=name)
          bioseq.letter_annotations[qkey] = q
          n, m = len(bioseq), len(bioseq.name)
          bwseq = bwa.alloc_seq(1, n, m)[0]
          bwa.bioseq_to_bwa_seq(bioseq, bwseq, n, m, fmt)  
          self.assertEqual(bioseq.name, bwseq.get_name())
          self.assertEqual(bioseq.seq.data[::-1], bwseq.get_seq())
          self.assertEqual(reverse_complement(bioseq.seq.data),
                           bwseq.get_rseq())
          # check that quality has been converted to sanger
          if fmt == "fastq-solexa":
            exp_q = [int(round(x+10*math.log10(1+10**(-x/10.)))) for x in q]
          else:
            exp_q = q
          exp_qstr = "".join(chr(x+sg.Q_OFFSET["fastq-sanger"]) for x in exp_q)
          self.assertEqual(bwseq.get_qual(), exp_qstr)

  def build_bws_array(self):
    # FIXME: plenty of UGLY code
    nseq = 100
    for fmt in "fastq-sanger", "fastq-illumina", "fastq-solexa":
      qkey = "solexa_quality" if fmt == "fastq-solexa" else "phred_quality"
      for unknown in False, True:
        g = u.random_reads_generator(nseq, fmt=fmt, unknown=unknown, pe=True)
        bioseq_pairs, qseq_pairs = [], []
        for i, read_pair in enumerate(g):
          base_name = "foo-%d" % i
          names = ["%s/%d" % (base_name, j) for j in 1, 2]
          seq_strings = ["".join(read_pair[j][0]) for j in 0, 1]
          q_strings = ["".join(chr(x+sg.Q_OFFSET[fmt]) for x in read_pair[j][1])
                       for j in 0, 1]
          bioseq_p = [SeqRecord(Seq(seq_strings[j], single_letter_alphabet),
                                id=names[j],
                                name=names[j],
                                description=names[j]) for j in 0, 1]
          for j in 0, 1:
            bioseq_p[j].letter_annotations[qkey] = read_pair[j][1]
          bioseq_pairs.append(bioseq_p)
          qseq_pairs.append([base_name])
          for j in 0, 1:
            for l in seq_strings, q_strings:
              qseq_pairs[-1].append(l[j])
        n = len(bioseq_pairs[0][0])
        assert len(qseq_pairs[0][1]) == n
        for src in "bioseq", "qseq":
          seq_pairs = bioseq_pairs if src == "bioseq" else qseq_pairs
          bwsa = bwa.build_bws_array(seq_pairs, qtype=fmt, src=src)
          for j in 0, 1:
            self.assertTrue(type(bwsa[j]) is bwa.bwa_seq_p_t)
            for i in xrange(nseq):
              bwseq = bwsa[j][i]
              self.assertEqual(bwseq.len, n)
              if src == "bioseq":
                exp_name = bioseq_pairs[i][0].name
              else:
                exp_name = "%s/%d" % (qseq_pairs[i][0], (j+1))
              self.assertEqual(len(bwseq.get_name()), len(exp_name))

  # This test simply checks that some sequences have been trimmed.
  # It makes no attempt to verify the correctness of the trimming,
  # since we rely directly on BWA for the implementation of the
  # trimming algorithm.
  def build_bws_array_with_trim_qual(self):
    pairs = self.load_pairs_fixture("pairs.txt")
    bws_array = bwa.build_bws_array(pairs, qtype="fastq-illumina", src="qseq", trim_qual=15)
    sequences = [ bws_array[read][i] for i in xrange(5) for read in 0,1 ]
    clipped = map(lambda seq: seq.full_len > seq.clip_len, sequences)
    self.assertTrue(any(clipped))


  # This test checks that no sequences have been trimmed.
  def build_bws_array_with_trim_qual_zero(self):
    pairs = self.load_pairs_fixture("pairs.txt")
    bws_array = bwa.build_bws_array(pairs, qtype="fastq-illumina", src="qseq", trim_qual=0)
    for read in 0,1:
      for i in xrange(len(pairs)):
        seq = bws_array[read][i]
        self.assertEqual(seq.full_len, seq.clip_len, "unexpected sequence trimming")

  def load_pairs_fixture(self, fixture_name):
    pairs = []
    with open( u.get_fixture_path(fixture_name), 'r' ) as f:
      for line in f:
        if not re.match("\s*#", line): # if line doesn't start with #
          pairs.append(line.rstrip("\n\r").split("\t"))
    return pairs

def suite():
  suite = unittest.TestSuite()
  suite.addTest(bwa_base_tc('hand_picked'))
  suite.addTest(bwa_base_tc('hand_picked_real_1'))
  suite.addTest(bwa_base_tc('hand_picked_real_2'))
  suite.addTest(bwa_base_tc('auto_generated'))
  suite.addTest(bwa_base_tc('multi_ref'))
  suite.addTest(bwa_base_tc('batch_pairing'))
  suite.addTest(bwa_base_tc('bioseq_to_bwa_seq'))
  suite.addTest(bwa_base_tc('build_bws_array'))
  suite.addTest(bwa_base_tc('build_bws_array_with_trim_qual'))
  suite.addTest(bwa_base_tc('build_bws_array_with_trim_qual_zero'))
  return suite


if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
