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

import os
import ctypes as ct
from constants import *

libbwa_path = os.path.join(os.path.split(__file__)[0], 'libbwa.so')
libbwa = ct.CDLL(libbwa_path)

Q_OFFSET = {
  "fastq-sanger": 33,
  "fastq-solexa": 64,
  "fastq-illumina": 64
}

################
# Basic types
################
p_char_p = ct.POINTER(ct.c_char_p)
################

def make_cigar(raw_cigar, n_cigar, read_len):
  if n_cigar == 0:
    return [(read_len, 'M')]
  cigar = []
  for k in xrange(n_cigar):
    size = raw_cigar[k] & CIGAR_LN_MASK
    op = "MIDS"[raw_cigar[k]>>CIGAR_OP_SHIFT]
    cigar.append((size, op))
  return cigar


def get_cigar_pos_end(cigar, pos):
  """Returns the position one beyond the last coordinate of the cigar"""
  x = pos
  for size, op in cigar:
    if op in 'MD':
      x += size
  return x

#-STRUCT-WRAPPERS--------------------------------------------------------------
class gap_opt_t(ct.Structure):
  _fields_ = [("s_mm", ct.c_int32),
              ("s_gapo", ct.c_int32),
              ("s_gape", ct.c_int32),
              ("mode", ct.c_int32),
              ("indel_end_skip", ct.c_int32),
              ("max_del_occ", ct.c_int32),
              ("max_entries", ct.c_int32),
              ("fnr", ct.c_float),
              ("max_diff", ct.c_int32),
              ("max_gapo", ct.c_int32),
              ("max_gape", ct.c_int32),
              ("max_seed_diff", ct.c_int32),
              ("seed_len", ct.c_int32),
              ("n_threads", ct.c_int32),
              ("max_top2", ct.c_int32),
              ("trim_qual", ct.c_int32),
              ]


class pe_opt_t(ct.Structure):
  _fields_ = [("max_isize", ct.c_int),
              ("max_occ", ct.c_int),
              ("n_multi", ct.c_int),
              ("N_multi", ct.c_int),
              ("type", ct.c_int),
              ("is_sw", ct.c_int),
              ("is_preload", ct.c_int),
              ("ap_prior", ct.c_double)]


class bwt_t(ct.Structure):
  _fields_ = [("primary", ct.c_uint32), #S^{-1}(0), or the primary index of BWT
              ("L2", ct.c_uint32 * 5), #C(), cumulative count
              ("seq_len", ct.c_uint32), # sequence length
              ("bwt_size", ct.c_uint32), # size of bwt, about seq_len/4
              ("bwt", ct.POINTER(ct.c_uint32)), # BWT
              ("cnt_table", ct.c_uint32 * 256),  # cnt_table[256]
              ("sa_intv", ct.c_int32),
              ("n_sa", ct.c_uint32),
              ("sa", ct.POINTER(ct.c_uint32))]


class bntann1_t(ct.Structure):
  _fields_ = [("offset", ct.c_int64),
              ("len", ct.c_int32),
              ("n_ambs", ct.c_int32),
              ("gi", ct.c_uint32),
              ("name", ct.c_char_p),
              ("anno", ct.c_char_p)]


class bntamb1_t(ct.Structure):
  _fields_ = [("offset", ct.c_int64),
              ("len", ct.c_int32),
              ("amb", ct.c_char)]


class bntseq_t(ct.Structure):
  _fields_ = [("l_pac", ct.c_int64),
              ("n_seqs", ct.c_int32),
              ("seed", ct.c_uint32),
              ("anns", ct.POINTER(bntann1_t)), # n_seqs elements
              ("n_holes", ct.c_int32),
              ("ambs", ct.POINTER(bntamb1_t)), # n_holes elements
              ("fp_pac", ct.c_void_p)] # this is really a FILE *fp_pac

class bwt_width_t(ct.Structure):
  _fields_ = [('w', ct.c_uint32),
              ('bid', ct.c_int32)]


class bwt_aln1_t(ct.Structure):
  _fields_ = [('n_mm', ct.c_uint32, 8),
              ('n_gapo', ct.c_uint32, 8),
              ('n_gape', ct.c_uint32, 8),
              ('a', ct.c_uint32, 1),
              ('k', ct.c_uint32),
              ('l', ct.c_uint32),
              ('score', ct.c_int32)]

# CRS4 patch: 'gap' split into 'n_gapo' + 'n_gape'; added score; 'mm' => 'n_mm'
class bwt_multi1_t(ct.Structure):
  _fields_ = [('pos', ct.c_uint32),
              ('n_cigar', ct.c_uint32, 15),
              ('n_gapo', ct.c_uint32, 8),
              ('n_gape', ct.c_uint32, 8),
              ('n_mm', ct.c_uint32, 8),
              ('strand', ct.c_uint32, 1),
              ('score', ct.c_int32),
              ('cigar', ct.POINTER(ct.c_uint16))]

  def __init__(self):
    super(bwt_multi1_t, self).__init__()

  def get_cigar(self, read_len):
    if not hasattr(self, "__cigar"):
      self.__cigar = make_cigar(self.cigar, self.n_cigar, read_len)
    return self.__cigar

  def get_pos_end(self, read_len):
    if not hasattr(self, "__pos_end"):
      self.__pos_end = get_cigar_pos_end(self.get_cigar(read_len), self.pos)
    return self.__pos_end


class bwa_seq_t(ct.Structure):
  _fields_ = [('name', ct.c_char_p),
              ('seq', ct.POINTER(ct.c_uint8)),
              ('rseq', ct.POINTER(ct.c_uint8)),
              ('qual', ct.POINTER(ct.c_uint8)),
              ('len', ct.c_uint32, 20),
              ('strand', ct.c_uint32, 1),
              ('type', ct.c_uint32, 2),
              ('dummy', ct.c_uint32, 1),
              ('extra_flag', ct.c_uint32, 8),
              ('n_mm', ct.c_uint32, 8),
              ('n_gapo', ct.c_uint32, 8),
              ('n_gape', ct.c_uint32, 8),
              ('mapQ', ct.c_uint32, 8),
              ('score', ct.c_int32),
              ('clip_len', ct.c_int32),
              ('n_aln', ct.c_int32),
              ('aln', ct.POINTER(bwt_aln1_t)),
              ('n_multi', ct.c_int32),
              ('multi', ct.POINTER(bwt_multi1_t)),
              ('sa', ct.c_uint32),
              ('pos', ct.c_uint32),
              ('c1', ct.c_uint64, 28),
              ('c2', ct.c_uint64, 28),
              ('seQ', ct.c_uint64, 8),
              ('n_cigar', ct.c_int32),
              ('cigar', ct.POINTER(ct.c_uint16)),
              ('tid', ct.c_int32),
							('bc', ct.c_char * 16), # in-struct array, up to 15 bases, null-terminated
              ('full_len', ct.c_uint32, 20),
              ('nm', ct.c_uint32, 12),
              ('md', ct.c_char_p)]

  def __len__(self):
    """
    Return the full length of the sequence.  Note that only
    the section up to clip_len is used for the alignment.  Read
    trimming in fact modifies the value of clip_len."""
    return self.full_len

  def __compute_cigar_ops(self):
    """
    Returns an array of tuples (lenth, op) corresponding to
    the alignment of this sequence.

    Based on bwa_print_sam1 from bwase.c
    """
    if self.type == BWA_TYPE_NO_MATCH:
      return []
    elif self.n_cigar == 0:
      return [(self.len, 'M')]
    else:
      cigar = []
      for k in xrange(self.n_cigar):
        size = self.cigar[k] & CIGAR_LN_MASK
        op = "MIDS"[self.cigar[k]>>CIGAR_OP_SHIFT]
        cigar.append((size, op))
      return cigar

  def __compute_pos_end(self):
    """
    Computes the position on the reference where this aligned
    sequence ends, based on the alignment described by the
    CIGAR operations and the starting position self.pos.
    Based on pos_end in bwase.c.
    NOTE:  pos_end uses p->len instead of full_len or clip_len.
    Raises: ValueError if self is of type BWA_TYPE_NO_MATCH
    """
    if self.type == BWA_TYPE_NO_MATCH:
      raise ValueError("Cannot calculate end position of an unmapped sequence")
    cig = self.get_cigar()
    if len(cig) == 0:
      return self.pos + self.len
    else:
      return get_cigar_pos_end(cig, self.pos)

  def get_cigar(self):
    if not hasattr(self, "__cigar"):
      self.__cigar = self.__compute_cigar_ops()
    return self.__cigar

  def get_pos_end(self):
    if not hasattr(self, "__pos_end"):
      self.__pos_end = self.__compute_pos_end()
    return self.__pos_end

  def get_pos_5(self):
    """
    position of the 5' end.
    Raises: ValueError if self is of type BWA_TYPE_NO_MATCH
    """
    if self.type == BWA_TYPE_NO_MATCH:
      raise ValueError("Cannot calculate 5' position of an unmapped sequence")
    if self.strand:
      return self.get_pos_end()
    else:
      return self.pos

  def get_seq(self):
    """
    Return seq (the original sequence) as a text string.
    """
    if not hasattr(self, "__seq"):
      self.__seq = ''.join(['ACGTN'[self.seq[k]] for k in xrange(self.full_len)])
    return self.__seq

  def get_rseq(self):
    """
    Return complement of seq as a text string.
    """
    # XXX:  note that for in BWA rseq is practically a temporary buffer.
    # It's only correct in the range [0,clip_len).  BWA itself creates the
    # complement sequence for output directly from the seq array.
    if not hasattr(self, "__rseq"):
      self.__rseq = ''.join(['TGCAN'[self.seq[k]] for k in xrange(self.full_len-1, -1, -1)])
    return self.__rseq

  # Get a string with the quality of the bases using an ASCII-33 encoding
  def get_qual_string(self):
    if not hasattr(self, "__qual"):
      ptr = self.qual
      if not ptr:
        self.__qual = ''
      else:
        self.__qual = ''.join([chr(ptr[k]) for k in xrange(self.full_len)])
    return self.__qual

  def get_name(self):
    return self.name

  def itermulti(self):
    """
    Yields alignment-specific info for all hits, including the 'main' one.
    """
    yield self
    for i in xrange(self.n_multi):
      yield self.multi[i]


bwa_seq_p_t = ct.POINTER(bwa_seq_t)


class isize_info_t(ct.Structure):
  _fields_ = [('avg', ct.c_double),
              ('std', ct.c_double),
              ('ap_prior', ct.c_double),
              ('low', ct.c_uint32),
              ('high', ct.c_uint32),
              ('high_bayesian', ct.c_uint32)]


#-END-STRUCT-WRAPPERS----------------------------------------------------------
init_g_log_n = libbwa.init_g_log_n
init_g_log_n.argtypes = []
init_g_log_n()

init_g_hash = libbwa.init_g_hash
init_g_hash.argtypes = []
init_g_hash()

#------------------------------------------------------------
gap_opt_p_t = ct.POINTER(gap_opt_t)
gap_init_opt = libbwa.gap_init_opt
gap_init_opt.argtypes = []
gap_init_opt.restype = gap_opt_p_t

#------------------------------------------------------------
pe_opt_p_t = ct.POINTER(pe_opt_t)
pe_init_opt = libbwa.bwa_init_pe_opt
pe_init_opt.argtypes = []
pe_init_opt.restype = pe_opt_p_t

#------------------------------------------------------------
bntseq_p_t = ct.POINTER(bntseq_t)

bns_restore = libbwa.bns_restore
bns_restore.argtypes = [ct.c_char_p]
bns_restore.restype = bntseq_p_t
restore_bns=bns_restore

bns_destroy_base = libbwa.bns_destroy
bns_destroy_base.argtypes = [bntseq_p_t]

#------------------------------------------------------------
bwt_p_t = ct.POINTER(bwt_t)
bwt_restore_bwt = libbwa.bwt_restore_bwt
bwt_restore_bwt.argtypes = [ct.c_char_p]
bwt_restore_bwt.restype = bwt_p_t
restore_bwt=bwt_restore_bwt

bwt_restore_sa = libbwa.bwt_restore_sa
bwt_restore_sa.argtypes = [ct.c_char_p, bwt_p_t]
restore_sa=bwt_restore_sa

bwt_destroy_base = libbwa.bwt_destroy
bwt_destroy_base.argtypes = [bwt_p_t]

#--
bwt_restore_bwt_mmap = libbwa.bwt_restore_bwt_mmap
bwt_restore_bwt_mmap.argtypes = [ct.c_char_p]
bwt_restore_bwt_mmap.restype = bwt_p_t
restore_bwt_mmap=bwt_restore_bwt_mmap

bwt_restore_sa_mmap = libbwa.bwt_restore_sa_mmap
bwt_restore_sa_mmap.argtypes = [ct.c_char_p, bwt_p_t]
restore_sa_mmap=bwt_restore_sa_mmap

bwt_destroy_mmap = libbwa.bwt_destroy_mmap
bwt_destroy_mmap.argtypes = [bwt_p_t]

ubyte_p_t = ct.POINTER(ct.c_ubyte)
bwt_restore_reference_mmap_helper = libbwa.bwt_restore_reference_mmap_helper
bwt_restore_reference_mmap_helper.argtypes = [ct.c_char_p, ct.c_int32]
bwt_restore_reference_mmap_helper.restype  = ubyte_p_t


#------------------------------------------------------------
bwa_cal_sa_reg_gap_mt = libbwa.bwa_cal_sa_reg_gap_mt
#    (bwt_t *const bwt[2], int n_seqs, bwa_seq_t *seqs, const gap_opt_t *opt) ;
bwa_cal_sa_reg_gap_mt.argtypes = [bwt_p_t * 2, ct.c_int32, bwa_seq_p_t, gap_opt_p_t]
cal_sa_reg_gap_mt = bwa_cal_sa_reg_gap_mt

#------------------------------------------------------------
bwa_cal_sa_reg_gap = libbwa.bwa_cal_sa_reg_gap
#    (int tid, bwt_t *const bwt[2], int n_seqs, bwa_seq_t *seqs, const gap_opt_t *opt) ;
bwa_cal_sa_reg_gap.argtypes = [ct.c_int32, bwt_p_t * 2, ct.c_int32, bwa_seq_p_t, gap_opt_p_t]
cal_sa_reg_gap = bwa_cal_sa_reg_gap


#------------------------------------------------------------
bwa_cal_pac_pos_pe = libbwa.bwa_cal_pac_pos_pe_memory
bwa_cal_pac_pos_pe.argtypes = [bwt_p_t * 2, ct.c_int, bwa_seq_p_t * 2,
                               ct.POINTER(isize_info_t),
                               pe_opt_p_t, gap_opt_p_t, ct.POINTER(isize_info_t)]
bwa_cal_pac_pos_pe.restype  = ct.c_int
#------------------------------------------------------------
bns_coor_pac2real = libbwa.bns_coor_pac2real
#int bns_coor_pac2real(const bntseq_t *bns, int64_t pac_coor, int len, int32_t *real_seq)
bns_coor_pac2real.argtypes = [bntseq_p_t, ct.c_uint64, ct.c_uint,
                               ct.POINTER(ct.c_uint32)]
bns_coor_pac2real.restype = ct.c_int

#------------------------------------------------------------
bwa_trim_read = libbwa.bwa_trim_read
# int bwa_trim_read(int trim_qual, bwa_seq_t *p)
bwa_trim_read.argtypes = [ct.c_int, bwa_seq_p_t]
bwa_trim_read.restype = ct.c_int

#------------------------------------------------------------
bwa_seq_reverse = libbwa.seq_reverse
# void seq_reverse(int len, ubyte_t *seq, int is_comp)
bwa_seq_reverse.argtypes = [ ct.c_int, ct.POINTER(ct.c_ubyte), ct.c_int ]
bwa_seq_reverse.restype = None

#------------------------------------------------------------
init_sequences = libbwa.bwa_init_sequences
# bwa_seq_t* bwa_init_sequences(const char**const data2d, const size_t n_seqs, const int q_offset, const int trim_qual)
init_sequences.argtypes = [p_char_p, ct.c_uint, ct.c_int, ct.c_int]
init_sequences.restype = bwa_seq_p_t

#------------------------------------------------------------
# Start Python functions

def get_seq_id(bns, pac_coordinate, ref_len):
  seq_id = ct.c_uint32()
  nn = bns_coor_pac2real(bns, pac_coordinate, ref_len, ct.byref(seq_id))
  return seq_id.value, nn


def cal_pac_pos_pe(bwts, n_seqs, bwsa, ii, popt, gopt, last_ii):
  bwsa_c = (bwa_seq_p_t * 2)()
  bwsa_c[0] = bwsa[0]
  bwsa_c[1] = bwsa[1]
  res = bwa_cal_pac_pos_pe(bwts, n_seqs, bwsa_c, ct.byref(ii), popt, gopt,
                           ct.byref(last_ii))
  return res

#------------------------------------------------------------
#ubyte_t *bwa_paired_sw(const bntseq_t *bns, const ubyte_t *_pacseq,
#                       int n_seqs, bwa_seq_t *seqs[2],
#                       const pe_opt_t *popt, const isize_info_t *ii);

bwa_paired_sw = libbwa.bwa_paired_sw
bwa_paired_sw.argtypes = [bntseq_p_t, ct.POINTER(ct.c_uint8), ct.c_int,
                          bwa_seq_p_t * 2,
                          pe_opt_p_t,
                          ct.POINTER(isize_info_t)]
bwa_paired_sw.restype = ct.POINTER(ct.c_uint8)

def paired_sw(bns, pacseq, n_seq_pairs, bwsa, popt, ii, offset=0):
  bwsa_c = (bwa_seq_p_t * 2)()
  bwsa_c[0] = ct.cast(ct.byref(bwsa[0][offset]), bwa_seq_p_t)
  bwsa_c[1] = ct.cast(ct.byref(bwsa[1][offset]), bwa_seq_p_t)
  r = bwa_paired_sw(bns, pacseq, n_seq_pairs, bwsa_c, popt, ct.byref(ii))
  return r

#------------------------------------------------------------
bwa_refine_gapped = libbwa.bwa_refine_gapped_memory
bwa_refine_gapped.argtypes = [bntseq_p_t, ct.POINTER(ct.c_uint8), ct.c_int, bwa_seq_p_t]

def refine_gapped(bns, n_seqs, seqs, pacseq):
  seqs_c = ct.cast(seqs, bwa_seq_p_t)
  return bwa_refine_gapped(bns, pacseq,  n_seqs, seqs_c)

#------------------------------------------------------------
bwa_free_read_seq = libbwa.bwa_free_read_seq
bwa_free_read_seq.argtypes = [ct.c_int32, bwa_seq_p_t]
free_seq = bwa_free_read_seq


def build_bws_array(sequence_pairs, qtype="fastq-sanger", trim_qual=0):
  """
  Build an array of BWA seq pairs from a list of sequence pairs.
  A sequence pair is  a (name, read_seq, read_qual, mate_seq, mate_qual).
  """
  # allocate two arrays of bwa_seq_t, one for each set of reads (1 and 2)
  bwsa_t = bwa_seq_p_t * 2
  bwsa = bwsa_t()
  if qtype == "fastq-sanger" or qtype == "fastq-illumina":
    # this is the offset that has been added to the quality scores,
    # which varies depending on the format.
    q_offset = Q_OFFSET[qtype]
  else:
    # XXX: to support fastq-solexa we'd have to rewrite the quality arrays
    # here according to the formula round(q[i] + 10 * log10(1+10**(-q[i]/10.)))
    raise ValueError("sorry. '%s' is not a supported qtype" % qtype)

  row_type = (ct.c_char_p * 3)
  pointers = (row_type*len(sequence_pairs))()
  for read_no in 0,1:
    if read_no == 0:
      row_indices = (0, 1, 2)
    else:
      row_indices = (0, 3, 4)

    for i, row in enumerate(sequence_pairs):
      if len(row) != 5:
        raise ValueError("Invalid row %d.  Expecting (name, read1, qual1, read2, qual2)"
                         " but got a row with length %d\n%s" % (i, len(row), str(row)))
      for j in xrange(3):
        pointers[i][j] = row[ row_indices[j] ]
    bwsa[read_no] = init_sequences( ct.cast(pointers, p_char_p), len(sequence_pairs), q_offset, trim_qual)
    if bwsa[read_no] is None:
      raise RuntimeError("There was an error creating the BWA sequences")
  return bwsa

def restore_index_base(root_name):
  bwts_t = bwt_p_t * 2
  bwts = bwts_t()
  bwts[0] = restore_bwt(root_name + '.bwt')
  bwts[1] = restore_bwt(root_name + '.rbwt')
  restore_sa(root_name + '.sa', bwts[0])
  restore_sa(root_name + '.rsa', bwts[1])
  return bwts


def restore_reference_base(root_name):
  bns = restore_bns(root_name)
  fd = open(root_name + '.pac')
  pacseq_str = fd.read(bns[0].l_pac/4 + 1)
  p = ct.c_char_p(pacseq_str)
  ubyte_p_t = ct.POINTER(ct.c_ubyte)
  pacseq = ct.cast(p, ubyte_p_t)
  return (bns, pacseq)

#--------------------------------------------------------------------


def mmap_magic():
  return ct.c_uint32.in_dll(libbwa, "BWA_MMAP_SA_MAGIC")

import struct
def make_suffix_array_for_mmap(src, dest):
  s = open(src)
  d = open(dest, 'w')
  # Write the magic number in native byte order so that it will only
  # be read correctly by machines that use the same byte ordering
  # (since the data is also in native byte order).
  d.write(struct.pack("=I", mmap_magic().value ))
  d.write(s.read(4*7))
  d.write(struct.pack("=I", 0x0ffffffff)) # uint32_t(-1)
  buf_size = 1000000
  while True:
    c = s.read(buf_size)
    if not c:
      break
    d.write(c)
  d.close()
  s.close()

def make_suffix_arrays_for_mmap(root_name):
  make_suffix_array_for_mmap(root_name + '.sa', root_name + '.sax')
  make_suffix_array_for_mmap(root_name + '.rsa', root_name + '.rsax')

def restore_index_mmap(root_name):
  bwts_t = bwt_p_t * 2
  bwts = bwts_t()
  bwts[0] = restore_bwt_mmap(root_name + '.bwt')
  bwts[1] = restore_bwt_mmap(root_name + '.rbwt')
  restore_sa_mmap(root_name + '.sax', bwts[0])
  restore_sa_mmap(root_name + '.rsax', bwts[1])
  return bwts

def restore_reference_mmap(root_name):
  bns = restore_bns(root_name)
  pac_fname = root_name + '.pac'
  pacseq = bwt_restore_reference_mmap_helper(pac_fname, bns[0].l_pac/4 + 1)
  return (bns, pacseq)

#------------------------------------------------------------

def restore_index(root, mmap=False):
  if mmap:
    return restore_index_mmap(root)
  else:
    return restore_index_base(root)

def restore_reference(root, mmap=False):
  if mmap:
    return restore_reference_mmap(root)
  else:
    return restore_reference_base(root)

def bwt_destroy(bwt, mmap=False):
  if mmap:
    bwt_destroy_mmap(bwt)
  else:
    bwt_destroy_base(bwt)


def bns_destroy(bnsp, mmap=False):
  bns_destroy_base(bnsp)
