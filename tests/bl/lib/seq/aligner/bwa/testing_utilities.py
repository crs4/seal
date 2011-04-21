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

import logging
logging.basicConfig(level=logging.DEBUG)
import time, os, glob, math
import itertools as it
import ctypes as ct

import Bio.SeqIO

import bl.lib.seq.aligner.bwa.bwa_core as bwa
from bl.lib.seq.aligner.bwa.bwa_aligner import BwaAligner
#import bl.lib.seq.generator as sg
from bl.lib.seq.fastq_io import FastqBuilder
from bl.lib.seq.aligner.io.sam_formatter import SamFormatter
import bl.lib.seq.aligner.bwa.constants as bwa_const
##---------------------------------##
# find or build the bwa executable
##---------------------------------##
if os.environ.has_key('SRC_DIR'): # provide a way to override the default location
  SRC_DIR = os.environ['SRC_DIR']
else:
  tree_root = os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', '..', '..', '..', '..', '..'))
  SRC_DIR = os.path.join(tree_root, 'bl', 'lib', 'seq', 'aligner', 'bwa', 'libbwa')
if not os.path.exists(SRC_DIR):
  raise ValueError("%r not found: redefine SRC_DIR in %s or in an environment variable" % (SRC_DIR, __name__))
BWA_EXE = os.path.join(SRC_DIR, 'bwa')
logging.debug("BWA_EXE: %r" % BWA_EXE)
if not os.path.exists(BWA_EXE):
  logging.warn("bwa executable not found; building source code")
  cmd = "make -C %s" % SRC_DIR
  ret = os.system(cmd)
  if ret:
    raise RuntimeError("%r failed -- could not make bwa executable" % cmd)
##---------------------------------##

##---------------------------------##
# Fixture helpers
##---------------------------------##
reference = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'fixtures/foobar.fa')

def build_ref_index():
	if not os.path.exists('%s.bwt' % reference):
		os.system('%s index %s >/dev/null 2>&1' % (BWA_EXE, reference))

def remove_ref_index():
	os.system('rm -f %s.*' % reference)

##---------------------------------##

def clean_up_aux_files(fname):
  fs = glob.glob(fname + '.*')
  for f in fs:
    if os.path.splitext(f)[-1] != ".gz":
      os.unlink(f)

def create_seq_file(fname, seq, qtype="fastq-sanger"):
  fp = open(fname, 'w')
  fp_sanger = open("%s.sanger" % fname, 'w')
  if qtype == "fastq-illumina" or qtype == "fastq-solexa":
    hdr1, s, hdr2, q = seq.splitlines(False)
    # FIXME: duplicate code from bwa_core
    q = [ord(x)-FastqBuilder.Q_OFFSET[qtype] for x in q]
    if qtype == "fastq-solexa":
      q = [int(round(x+10*math.log10(1+10**(-x/10.)))) for x in q]
    q = "".join([chr(x+FastqBuilder.Q_OFFSET["fastq-sanger"]) for x in q])
    seq_sanger = "%s\n%s\n%s\n%s\n" % (hdr1, s, hdr2, q)
  else:
    seq_sanger = seq
  fp.write(seq)
  fp_sanger.write(seq_sanger)
  fp.close()
  fp_sanger.close()

"""
 Currently unused.  Commented out since it introduces a dependency to the core lib.
def generate_pairs(refseq, nseq, seq_len, fmt='fastq-illumina', span=500,
                   mu=None, sigma=0):
  if mu is None:
    mu= 0.5*(sg.Q_RANGE[fmt][1] - sg.Q_RANGE[fmt][0])

  sampler = sg.hit_sampler(seq_len, refseq, fmt=fmt, span=span,
                           mu=mu, sigma=sigma)
  fbuilder = FastqBuilder(fmt=fmt)
  def formatting_function(n, s, q):
    return fbuilder.make((n, s, q))
  mapper_tag = '%s:%f20' % ('foo', time.time())
  factory = sg.seq_factory(mapper_tag, sampler, barcode=None,
                           barcode_fraction=1.0,
                           formatter=formatting_function)
  return [factory.make_pe() for _ in xrange(nseq)]

# FIXME: a lot of code is copied from generate_pairs
def generate_localized_pairs(refseq, starts, seq_len, fmt='fastq-illumina',
                             span=500, mu=None, sigma=0):
  if mu is None:
    mu= 0.5*(sg.Q_RANGE[fmt][1] - sg.Q_RANGE[fmt][0])

  sampler = sg.localized_hit_sampler(seq_len, refseq, fmt=fmt, span=span,
                                     mu=mu, sigma=sigma, starts=starts)
  fbuilder = FastqBuilder(fmt=fmt)
  def formatting_function(n, s, q):
    return fbuilder.make((n, s, q))
  mapper_tag = '%s:%f20' % ('foo', time.time())
  factory = sg.seq_factory(mapper_tag, sampler, barcode=None,
                           barcode_fraction=1.0,
                           formatter=formatting_function)
  return [factory.make_pe() for _ in xrange(len(starts))]


def random_reads_generator(nseq, fmt='fastq-illumina', unknown=False, pe=False):
  sampler = sg.full_range_sampler(fmt=fmt, unknown=unknown)
  for _ in xrange(nseq):
    if pe:
      yield sampler.get_paired_reads()
    else:
      yield sampler.get_read()
"""

def read_sam_file(fname):
  formatter = SamFormatter()
  fp = open(fname)
  res = []
  for l in fp:
    l = l.strip()
    if l[0] == "@":  # SAM header line
      continue
    r = formatter.parse(l)
    res.append(r)
  fp.close()
  return res

def build_index(refseq_fname):
  os.system('%s index %s' % (BWA_EXE, refseq_fname))

def run_bwa_samse(refseq_fname, hit_fname):
  hit_fname += ".sanger"
  os.system('%s aln %s %s > %s.aln' % (BWA_EXE, refseq_fname,
                                       hit_fname, hit_fname))
  os.system('%s samse %s %s.aln %s > %s.sam' % (BWA_EXE, refseq_fname,
                                                hit_fname, hit_fname,
                                                hit_fname))
  return read_sam_file(hit_fname + '.sam')

def run_bwa_sampe(refseq_fname, read_fname, mate_fname):
  read_fname += ".sanger"
  mate_fname += ".sanger"
  os.system('%s aln %s %s > %s.aln' % (BWA_EXE, refseq_fname,
                                       read_fname, read_fname))
  os.system('%s aln %s %s > %s.aln' % (BWA_EXE, refseq_fname,
                                       mate_fname, mate_fname))
  os.system('%s sampe %s %s.aln %s.aln %s %s > %s.sam' % (BWA_EXE, refseq_fname,
                                                          read_fname,
                                                          mate_fname,
                                                          read_fname,
                                                          mate_fname,
                                                          read_fname))
  return read_sam_file(read_fname + '.sam')

def run_bwa_py_sampe(refseq_fname, read_fname, mate_fname,
                     log_level=logging.INFO, pairing_batch_size=None,
                     seq_list_len=None, fastq_subfmt="fastq-illumina"):
  logger = logging.getLogger("PY")
  logger.setLevel(log_level)
  logger.info("RUNNING PYTHON VERSION")
  def debug_dump(seq, state):
    logger.debug("%s: name=%s" % (state, seq.get_name()))
    logger.debug("%s: qual=%s" % (state, seq.get_qual_string()))
    logger.debug("%s: strand=%d" % (state, seq.strand))
    logger.debug("%s: pos=%d" % (state, seq.pos))
    logger.debug("%s: mapQ=%d" % (state, seq.mapQ))
  
  read_flow = Bio.SeqIO.parse(open(read_fname), fastq_subfmt)
  mate_flow = Bio.SeqIO.parse(open(mate_fname), fastq_subfmt)
  pairs_flow = it.izip(read_flow, mate_flow)

  class ResultCollector(object):
    def __init__(self):
      self.result = []
    def process(self, pair):
      self.result.append(pair[0])
      self.result.append(pair[1])
  result = ResultCollector()

  while 1:
    start = time.time()
    pairs = list(it.islice(pairs_flow, 0, seq_list_len))
    if len(pairs) == 0:
      break
    # turn the biopython SeqRecords into simple tuples
    tuples = map(lambda t: (t[0].name, t[0].seq.tostring(), None, t[1].seq.tostring(), None), pairs[0:5])
    for t in tuples:
      print t
    logger.info('reading seqs %f sec' % (time.time() - start))

    start = time.time()
    aligner = BwaAligner()
    aligner.reference = refseq_fname
    aligner.hit_visitor = result
    for t in tuples[0:5]:
      aligner.load_pair_record(t)
    aligner.run_alignment()
    aligner.clear_batch()
    logger.info('alignment %f sec' % (time.time() - start))

  # map bwa mappings to dictionaries
  def bwam_to_hash(bwa_m):
    h = dict(
        name=bwa_m.name,
        aux=bwa_m.tags,
        seq=bwa_m.get_seq_5()
        )
    return h

  return map(bwam_to_hash, result.result)
 
def get_fixture_path(fixture_name):
	# get the path to this file's directory, then go into the fixtures directory
	# and finally attach the fixture_name
	return os.path.join(os.path.dirname(os.path.abspath( __file__ )), "fixtures", fixture_name)

base_key = {
    'A':0, 'a':0,
    'C':1, 'c':1,
    'G':2, 'g':2,
    'T':3, 't':3,
    'N':4, 'n':4
}
op_key = {
    'M':0,
    'I':1,
    'D':2,
    'S':3
}
key_base = 'ACGTN'
complement_key_base = 'TGCAN'

complement = {
  'A':'T',
  'C':'G',
  'G':'C',
  'T':'A',
  'N':'N'
}

def build_bwa_seq_t(hit):
  """Create a real bwa_seq_t from a dictionary containing its values"""
  struct = bwa.bwa_seq_t()
  struct.name = ct.c_char_p(hit['name'])
  struct.seq = ct.cast(ct.create_string_buffer(len(hit['seq'])), ct.POINTER(ct.c_uint8))
  struct.rseq = ct.cast(ct.create_string_buffer(len(hit['rseq'])), ct.POINTER(ct.c_uint8))
  struct.qual = ct.cast(ct.c_char_p(hit['qual']), ct.POINTER(ct.c_uint8))
  struct.len = hit['len']
  struct.strand = hit['strand']
  struct.type = hit['type']
  struct.dummy = hit['dummy']
  struct.extra_flag = hit['extra_flag']
  struct.n_mm = hit['n_mm']
  struct.n_gapo = hit['n_gapo']
  struct.n_gape = hit['n_gape']
  struct.mapQ = hit['mapQ']
  struct.score = hit['score']
  struct.clip_len = hit['clip_len']
  struct.n_aln = hit['n_aln']
  struct.aln = None
  struct.n_multi = hit['n_multi']
  struct.multi = None
  struct.sa = hit['sa']
  struct.pos = hit['pos']
  struct.c1 = hit['c1']
  struct.c2 = hit['c2']
  struct.seQ = hit['seQ']
  struct.n_cigar = hit['n_cigar']
  struct.cigar = ct.cast(ct.create_string_buffer( len(hit['cigar'])*ct.sizeof(ct.c_uint16) ), ct.POINTER(ct.c_uint16))
  struct.tid = hit['tid']
  struct.full_len = hit['full_len']
  struct.nm = hit['nm']
  struct.md = None

  for i in xrange(len(hit['seq'])):
    struct.seq[i] = base_key[ hit['seq'][i] ]
    struct.rseq[i] = base_key[ hit['rseq'][i] ]

  # create the CIGAR array
  for i in xrange(len(hit['cigar'])):
    size, op = hit['cigar'][i]
    encoded_v = (op << bwa_const.CIGAR_OP_SHIFT) | (size & bwa_const.CIGAR_LN_MASK)
    struct.cigar[i] = ct.c_uint16(encoded_v)

  return struct
