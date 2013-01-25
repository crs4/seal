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

import bwa_core as bwa
from bwa_mapping import BwaMapping

from seal.lib.event_monitor import QuietMonitor

class BWAIterator(object):

  def __init__(self, root, gopt, popt, max_isize, pairing_batch_size,
               visitor=None, mmap_enabled=False):
    if visitor is None:
      self.visitor = QuietMonitor()
    else:
      self.visitor = visitor
    self.root = root
    self.gopt = gopt
    self.popt = popt
    self.max_isize = max_isize
    self.pairing_batch_size = pairing_batch_size
    self.mmap_enabled = mmap_enabled
    # reference structures, loaded on demand
    self.__bwts = None # the reference index
    # the reference itself
    self.__pacseq = None
    self.__bnsp = None
    self.__last_ii = None
    self.clean_isize_statistics()

  def clean_isize_statistics(self):
    self.__last_ii = bwa.isize_info_t()
    self.__last_ii.avg = -1.0

  def reference_loaded(self):
    # we use a single variable (__bwts) to determine whether
    # the ref has been loaded.
    return self.__bwts is not None

  def load_reference(self):
    """
    Load the reference at self.root.
    Remember to deallocate it when unload_reference() once finished!"""
    if self.reference_loaded():
      raise RuntimeError("A reference is already loaded!")
    try:
      with self.visitor.time_block("restore_index"):
        self.__bwts = bwa.restore_index(self.root, self.mmap_enabled)

      with self.visitor.time_block("restore_reference"):
        self.__bnsp, self.__pacseq = bwa.restore_reference(self.root,
                                                           self.mmap_enabled)
    except:
      self.unload_reference()
      raise

  def unload_reference(self):
    """Free an allocated reference, if any.
    Sets __bwts, __pacseq, and __bnsp to None.
    """
    if self.__bwts:
      for j in 0, 1:
        bwa.bwt_destroy(self.__bwts[j], self.mmap_enabled)
      self.__bwts = None

    if self.__pacseq:
      del self.__pacseq
      self.__pacseq = None

    if self.__bnsp:
      bwa.bns_destroy(self.__bnsp, self.mmap_enabled)
      self.__bnsp = None

  def __analyze(self, bwsa, seq_pairs_read):
    ##################################################
    # This method performs the actual analysis work.
    # BWA writes its results directly into the bwsa structures.
    ##################################################
    if not self.reference_loaded():
      self.load_reference()

    ii = bwa.isize_info_t()

    self.visitor.start("cal_sa_reg_gap")
    for i in 0, 1:
      bwa.cal_sa_reg_gap_mt(self.__bwts, seq_pairs_read, bwsa[i], self.gopt)
    self.visitor.stop("cal_sa_reg_gap")

    self.visitor.start("cal_pac_pos_pe")
    bwa.cal_pac_pos_pe(self.__bwts, seq_pairs_read, bwsa, ii,
                                 self.popt, self.gopt, self.__last_ii)
    self.visitor.stop("cal_pac_pos_pe")
    self.__last_ii = ii

    if ii.avg > self.max_isize:
      self.visitor.log_warning("skipping S-W, isize is too big (%.3f)" % ii.avg)
    else:
      self.visitor.start("paired_sw")
      if 0 < self.pairing_batch_size < seq_pairs_read:
        for offset in xrange(0, seq_pairs_read, self.pairing_batch_size):
          nseq = min(self.pairing_batch_size, seq_pairs_read - offset)
          self.visitor.start("paired_sw_batch")
          bwa.paired_sw(self.__bnsp, self.__pacseq, nseq, bwsa, self.popt, ii, offset)
          self.visitor.stop_batch("paired_sw_batch", offset, nseq)
      else:
        bwa.paired_sw(self.__bnsp, self.__pacseq, seq_pairs_read, bwsa, self.popt, ii)
      self.visitor.stop("paired_sw")

    self.visitor.start("refine_gapped")
    for i in 0, 1:
      bwa.refine_gapped(self.__bnsp, seq_pairs_read, bwsa[i], self.__pacseq)
    self.visitor.stop("refine_gapped")


  def analyze(self, bwsa, seq_pairs_read):
    """
    Align the seq_pairs_read sequence pairs in bwsa and iterate
    through the matches one read at a time.  For each read
    in each pair in bwsa, this method will yield
    pairs of BwaMapping objects (mread1, mread2) and (mread2, mread1).
    """
    self.__analyze(bwsa, seq_pairs_read)
    for i in xrange(seq_pairs_read):
      for j in 0, 1:
        yield BwaMapping(self.gopt[0], self.__bnsp[0], bwsa[j][i], bwsa[j^1][i])


  def analyze_pairs(self, bwsa, seq_pairs_read):
    """
    Align the seq_pairs_read sequence pairs in bwsa and iterate
    through the matches two reads at a time.  For each pair
    in bwsa this method will yield a
    pairs of BwaMapping objects (mread1, mread2) and (mread2, mread1).

    To analyze the match objects use bwa.analyze_hit
    """
    self.__analyze(bwsa, seq_pairs_read)
    for i in xrange(seq_pairs_read):
      # XXX LP: if we leave in this check our filtered map counts will be wrong.
      # since it doesn't seem to make much of a difference in speed I'll leave it in.
      #if bwsa[0][i].type == bwa.BWA_TYPE_NO_MATCH or bwsa[1][i].type == bwa.BWA_TYPE_NO_MATCH:
        yield BwaMapping(self.gopt[0], self.__bnsp[0], bwsa[0][i], bwsa[1][i]), BwaMapping(self.gopt[0], self.__bnsp[0], bwsa[1][i], bwsa[0][i])

