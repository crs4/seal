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

from constants import *
from bl.lib.seq.aligner.sam_flags import *
from bl.lib.seq.aligner.mapping import Mapping
import bwa_core as bwa

import sys
import array
from ctypes import *

class BwaMapping(Mapping):
  """
  Specialization of the seq.aligner.Mapping class to work with bwa hits. Note that the constructor expects
  its arguments to be actual objects, not pointers to them. Example:

    >>> gopt = bwa.gap_init_opt()
    >>> bnsp, pacseq = bwa.restore_reference(root)
    >>> hw = BwaMapping(gopt[0], bnsp[0], read, mate)

    >>> res = bwa.analyze_hit(gopt[0], bnsp, read, mate)
    >>> print res['name']
    foo-00000/1
    >>> print hw.name
    foo-00000/1
  """
  def __init__(self, opts, bns, read, mate=None):
    """
    @param opts:  bwa parameter structure gap_opt_t
    @param bns:  reference structure bntseq_t
    @param read: bwa_seq_t
    @param mate: bwa_seq_t

    Added attributes:
    self.bns: reference structure
    self.opts: the BWA options structure provided on construction
    """
    super(BwaMapping, self).__init__()
    #self.print_debug_info(self.__read)
    self.opts = opts
    self.bns = bns
    self.__read = read
    self.__mate = mate
    self.flag = self.__read.extra_flag

    if (read.type != BWA_TYPE_NO_MATCH or (mate and mate.type != BWA_TYPE_NO_MATCH)):
      self.__build_hit_for_match()
    else:
     # both read and mate are NO_MATCH
      self.flag |= SAM_FSU
      if self.__mate:
        self.flag |= SAM_FMU

  def __build_hit_for_match(self):
    """At least one of read or mate are a match"""
    if self.__read.type == BWA_TYPE_NO_MATCH:
      # the read isn't matched, which implies we have a mate
      # and it is matched.  We place the read at the mate's position
      self.pos = self.__mate.pos
      strand = self.__mate.strand
      self.flag |= SAM_FSU # query unmapped
      ref_len = 1 # ref_len is the length of the reference in the alignment
    else:
      self.pos = self.__read.pos
      strand = self.__read.strand
      ref_len = self.__read.get_pos_end() - self.pos

    if strand:
      self.flag |= SAM_FSR # query on the reverse strand

    # Where in the reference is pos?
    self.ref_id, self.__nn = bwa.get_seq_id(self.bns, self.pos, ref_len)
    self.tid = self.bns.anns[self.ref_id].name
    # make the position relative to the start of its reference section
    self.pos = self.pos - self.bns.anns[self.ref_id].offset

    # Check whether the read alignment bridges two adjacent reference sequences
    if (self.__read.type != BWA_TYPE_NO_MATCH and
        (self.pos + ref_len > self.bns.anns[self.ref_id].len)):
      self.flag |= SAM_FSU # it does, so we set it as unmapped

    # presume we can't calculate any of these fields,
    # so we set them to the "default" values"
    self.mtid = None
    self.mpos = 0
    self.isize = 0

    if self.__mate:
      if self.__mate.type != BWA_TYPE_NO_MATCH:
        if self.__mate.strand:
          self.flag |= SAM_FMR
        self.m_ref_id, m_nn = bwa.get_seq_id(self.bns, self.__mate.pos, self.__mate.len)
        if self.m_ref_id == self.ref_id:
          self.mtid = '='
          if self.__read.type != BWA_TYPE_NO_MATCH:
            self.isize = self.__mate.get_pos_5() - self.__read.get_pos_5()
            # else the isize remains at 0
        else:
          self.mtid = self.bns.anns[self.m_ref_id].name
        self.mpos = self.__mate.pos - self.bns.anns[self.m_ref_id].offset
      else: # unmapped mate
        self.flag |= SAM_FMU
        self.mtid = "="
        self.mpos = self.__read.pos - self.bns.anns[self.ref_id].offset

    self.pos = self.pos + 1 # base 1
    self.mpos = self.mpos + 1 # base 1
    self.qual = self.__read.mapQ

  def get_name(self):
    if not hasattr(self, "__name"):
      self.__name = self.__read.get_name()
    return self.__name

  def get_base_qualities(self):
    if not hasattr(self, '__base_qualities'):
      if self.__read.qual:
        # BWA keeps base quality is in ASCII-33
        it = xrange(self.__read.full_len-1,-1,-1) if self.is_on_reverse() else xrange(self.__read.full_len)
        try:
          self.__base_qualities = array.array("B", [ self.__read.qual[i] - 33 for i in it ])
        except OverflowError:
          raise ValueError("Base quality value out of range.  Have you set the base quality format correctly? (property name: bl.seqal.fastq-subformat")
      else:
        self.__base_qualities = None
    return self.__base_qualities

  def get_cigar(self):
    if self.is_mapped():
      return self.__read.get_cigar()
    else:
      return []

  ####################################
  # Tag methods
  ####################################
  def each_tag(self):
    if not hasattr(self, '__tags'):
      self.__compute_tags()
    for tpl in self.__tags:
      yield tpl

  def __find_tag(self, name):
    if not hasattr(self, '__tags'):
      self.__compute_tags()
    for tpl in self.__tags:
      if name == tpl[0]:
        return tpl
    return None

  def tag_value(self, tag_name):
    """
    Get the value for a tag, if present.
    Returns:
      - integer (if tag type is 'i')
      - float (if tag type is 'f')
      - string
      - None, if tag isn't present
    """
    tag = self.__find_tag(tag_name) # calls __compute_tags if necessary
    if tag:
      if tag[1] == 'i':
        value = int(tag[2])
      elif tag[1] == 'f':
        value = float(tag[2])
      else:
        value = tag[2]
      return value
    else:
      return None

  def __compute_tags(self):
    """
    Tags starting with 'X' are specific to BWA.
    
    +-----+---------------------------------------------------------------+
    | Tag |                    Meaning                                    |
    +-----+---------------------------------------------------------------+
    | NM  | Edit distance (number of nucleotide differences)              |
    | MD  | Mismatching positions/bases                                   |
    | AS  | Alignment score                                               |
    | CM  | Number of color differences                                   |
    | SM  | Mapping quality if read is mapped as single read              |
    | AM  | Smaller single-end mapping quality of the two reads in a pair |
    +-----+---------------------------------------------------------------+
    | X0  | Number of best hits                                           |
    | X1  | Number of suboptimal hits found by BWA                        |
    | XN  | Number of ambiguous bases in the referenece                   |
    | XM  | Number of mismatches in the alignment                         |
    | XO  | Number of gap opens                                           |
    | XG  | Number of gap extentions                                      |
    | XT  | Type: Unique/Repeat/N/Mate-sw                                 |
    | XA  | Alternative hits; format: (chr,pos,CIGAR,NM;)*                |
    | XS  | Suboptimal alignment score                                    |
    | XF  | Support from forward/reverse alignment                        |
    | XE  | Number of supporting seeds                                    |
    | XC  | Length of clipped reads                                       |
    +-----+---------------------------------------------------------------+
    
    Note that XO and XG are generated by BWT search while the CIGAR
    string by Smith-Waterman alignment. These two tags may be
    inconsistent with the CIGAR string. This is not a bug.
    """
    self.__tags = []
    if (self.__read.clip_len < self.__read.full_len):
      self.__tags.append(("XC", "i", self.__read.clip_len))

    if self.__read.type != BWA_TYPE_NO_MATCH:
      # min_seQ: the smaller of the two mapping qualities
      if self.__mate and self.__mate.type != BWA_TYPE_NO_MATCH:
        min_seQ = min(self.__read.seQ, self.__mate.seQ)
      else:
        min_seQ = 0 # else the mate isn't mapped and therefore 0

      mode, max_top2 = self.opts.mode, self.opts.max_top2
      XT = "NURM"[self.__read.type] if self.__nn <=10 else 'N'
      self.__tags.append(('XT', 'A', XT))
      self.__tags.append(('NM' if mode & BWA_MODE_COMPREAD else 'CM',
                   'i', self.__read.nm))
      if self.__nn:
        self.__tags.append(('XN', 'i', self.__nn))
      if self.__mate:
        self.__tags.append(('SM', 'i', self.__read.seQ))
        self.__tags.append(('AM', 'i', min_seQ))
      if self.__read.type != BWA_TYPE_MATESW:
        #NOTE this is X0 (digit 0  NOT the letter O)
        self.__tags.append(('X0', 'i', self.__read.c1))
        if self.__read.c1 < max_top2:
          self.__tags.append(('X1', 'i', self.__read.c2))
      self.__tags.append(('XM', 'i', self.__read.n_mm))
      #NOTE this is XO (letter O NOT the digit 0)
      self.__tags.append(('XO', 'i', self.__read.n_gapo))
      self.__tags.append(('XG', 'i', self.__read.n_gapo + self.__read.n_gape))
      if self.__read.md:
        self.__tags.append(('MD', 'Z', self.__read.md))
      # FIXME: for consistency, the third field of the XA tag should be a
      # list of tuples to be converted to a string upon formatting.
      if self.__read.n_multi:
        multi_hit = []
        for i in xrange(self.__read.n_multi):
          multi_hit_chunk = self.__analyze_multi(i)
          multi_hit.append(",".join(multi_hit_chunk))
        self.__tags.append(('XA', 'Z', ";".join(multi_hit)+";"))

  def __analyze_multi(self, i):
    multi_hit_chunk = []
    q = self.__read.multi[i]
    ref_len = q.get_pos_end(len(self.__read)) - q.pos
    ref_id, nn = bwa.get_seq_id(self.bns, q.pos, ref_len)
    sign = '-' if q.strand else '+'
    multi_hit_chunk.append(self.tid)
    multi_hit_chunk.append("%s%d" %
                           (sign, (q.pos - self.bns.anns[ref_id].offset + 1)))
    # FIXME: duplicate from samt_adapter, we should centralize formatting
    cigar_str = "".join(['%d%s' % t for t in q.get_cigar(len(self.__read))]) or "*"
    multi_hit_chunk.append(cigar_str)
    multi_hit_chunk.append("%d" % (q.n_gapo + q.n_gape + q.n_mm))
    return multi_hit_chunk

  def get_seq_5(self):
    if not hasattr(self, "__seq5"):
      if self.is_on_reverse():
        # The sequence self.__read.get_rseq()[::-1] is shifted by one position.  The way to get
        # the correct reverse complement sequence is to build it from seq, which is exactly 
        # what BWA normally does.
        self.__seq5 = ''.join([ "TGCAN"[self.__read.seq[k]] for k in xrange(self.__read.full_len-1, -1, -1)])
      else:
        self.__seq5 = self.__read.get_seq()
    return self.__seq5

  def get_seq_len(self):
    return self.__read.full_len

  def __str__(self):
    return "%s\tflags: %s" % (self.get_name(), self.flag_string())

  def print_debug_info(self, read):
    print >>sys.stderr, "bwa_seq_t for read"
    print >>sys.stderr, 'name:', read.get_name()
    print >>sys.stderr, 'seq:', ''.join(['ACGTN'[read.seq[k]] for k in xrange(read.full_len)])
    print >>sys.stderr, 'rseq:', ''.join(['ACGTN'[read.rseq[k]] for k in xrange(read.full_len)])
    if read.qual:
      print >>sys.stderr, 'qual:', ''.join([chr(read.qual[k]) for k in xrange(read.full_len)])
    else:
      print >>sys.stderr, 'qual: NULL'
    print >>sys.stderr, 'len:', read.len
    print >>sys.stderr, 'strand:', read.strand
    print >>sys.stderr, 'type:', read.type
    print >>sys.stderr, 'dummy:', read.dummy
    print >>sys.stderr, 'extra_flag: 0x%x' % read.extra_flag
    print >>sys.stderr, 'n_mm:', read.n_mm
    print >>sys.stderr, 'n_gapo:', read.n_gapo
    print >>sys.stderr, 'n_gape:', read.n_gape
    print >>sys.stderr, 'mapQ:', read.mapQ
    print >>sys.stderr, 'score:', read.score
    print >>sys.stderr, 'clip_len:', read.clip_len
    print >>sys.stderr, 'n_aln:', read.n_aln
    print >>sys.stderr, 'aln: --'
    print >>sys.stderr, 'n_multi:', read.n_multi
    print >>sys.stderr, 'multi: --'
    print >>sys.stderr, 'sa:', read.sa
    print >>sys.stderr, 'pos:', read.pos
    print >>sys.stderr, 'c1:', read.c1
    print >>sys.stderr, 'c2:', read.c2
    print >>sys.stderr, 'seQ:', read.seQ
    print >>sys.stderr, 'n_cigar:', read.n_cigar
    print >>sys.stderr, 'cigar: ', [ (read.cigar[i] & CIGAR_LN_MASK, read.cigar[i] >> CIGAR_OP_SHIFT) for i in xrange(read.n_cigar) ]
    print >>sys.stderr, 'tid:', read.tid
    print >>sys.stderr, 'full_len:', read.full_len
    print >>sys.stderr, 'nm:', read.nm
    print >>sys.stderr, 'md: --'
