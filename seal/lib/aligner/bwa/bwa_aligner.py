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
import glob

import bwa_core as bwa
from bwa_iterator import BWAIterator
from seal.lib.event_monitor import QuietMonitor

BWA_INDEX_MANDATORY_EXT = set(["amb", "ann", "bwt", "pac", "rbwt", "rpac"])
BWA_INDEX_MMAP_EXT = set(["rsax", "sax"])
BWA_INDEX_NORM_EXT = set(["rsa", "sa"])
BWA_INDEX_EXT = BWA_INDEX_MANDATORY_EXT | BWA_INDEX_MMAP_EXT | BWA_INDEX_NORM_EXT

class BwaAligner(object):
  """
  Object oriented interface to perform bwa aln + bwa sampe
  using libbwa and the bwa module.
  """
  def __init__(self):
    self.event_monitor = QuietMonitor()
    self.logger = QuietMonitor()
    self.__gopt = bwa.gap_init_opt()
    self.__popt = bwa.pe_init_opt()
    self.__batch = []

    self.qformat = "fastq-illumina"
    self.max_isize = 1000
    self.nthreads = 1
    self.trim_qual = 0
    self.mmap_enabled = False
    # .reference is the path to the indexed BWA reference to use.  Include the
    # "root name" of the reference files--i.e. the names without the file extensions.
    # The trailing dot, if included, will be removed.
    # e.g. my_references/hg_18.bwt
    #      my_references/hg_18.rsax
    #      my_references/hg_18.sax   => "my_references/hg_18"
    #      my_references/hg_18.pac
    #      ...
    self.reference = None
    self.hit_visitor = None
    self.__iterator = None

  def load_pair_record(self, record):
    """
    Append a tuple of the format (id, seq1, qual1, seq2, qual2) to this aligner's work batch.
    """
    self.__batch.append(record)
    return len(self.__batch)

  def get_batch_size(self):
    return len(self.__batch)

  def clear_batch(self):
    self.__batch = []

  def run_alignment(self):
    if not self.reference:
      raise ValueError("You must set the reference path before calling run_alignment")
    if not self.hit_visitor:
      raise ValueError("You must set the hit_visitor before calling run_alignment (else you'll lose the alignment results)")

    self.__check_reference()

    # update __gopt to reflect any changes to our public attributes.
    self.__gopt.contents.n_threads = self.nthreads
    self.event_monitor.log_debug("BWA using %d threads", self.__gopt.contents.n_threads)

    if self.__iterator is None:
      self.__iterator = BWAIterator(self.reference, self.__gopt, self.__popt,
                                    self.max_isize, len(self.__batch),
                                    visitor=self.event_monitor,
                                    mmap_enabled=self.mmap_enabled)

    status = "aligning against %s" % os.path.basename(self.reference)
    self.event_monitor.new_status(status)
    self.event_monitor.log_debug(status)

    self.event_monitor.count("reads processed", 2*len(self.__batch))

    with self.event_monitor.time_block("build_bwsa"):
      # we need to rebuild sequences each time, bwa functions modify them.
      bwsa = bwa.build_bws_array(self.__batch, qtype=self.qformat, trim_qual=self.trim_qual)

    self.__count_bases(bwsa, len(self.__batch))

    # bwa_iterator.analyze_pairs performs the alignment, then lets you iterate
    # over the results
    with self.event_monitor.time_block("analyze_pairs (cal_+sw+refgap+process)"):
      # The python speed optimization tips suggest removing non-local variable look-ups
      # from loops.
      # http://wiki.python.org/moin/PythonSpeed/PerformanceTips
      h_proc = self.hit_visitor.process
      for hit1, hit2 in self.__iterator.analyze_pairs(bwsa, len(self.__batch)):
        h_proc( (hit1, hit2) )

    with self.event_monitor.time_block("destroy_sequences"):
      for j in 0, 1:
        bwa.free_seq(len(self.__batch), bwsa[j])

  def release_resources(self):
    if self.__iterator:
      self.__iterator.unload_reference()

  def __count_bases(self, bwsa, bwsa_size):
    self.event_monitor.start("count_bases")
    for r in 0,1:
      for i in xrange(bwsa_size):
        seq = bwsa[r][i]
        self.event_monitor.count("total bases", seq.full_len)
        self.event_monitor.count("trimmed bases", seq.full_len - seq.clip_len)
    self.event_monitor.stop("count_bases")


  def __check_reference(self):
    """
    Checks that the indexed referenced at path self.reference includes
    files with all the expected extensions.
    Raises ValueError if something is missing.
    """
    if self.reference[-1] == '.':
      self.reference = self.reference[0:-1] # remove the trailing '.', if any
    ref_extensions = set([ os.path.splitext(path)[1].lstrip('.') for path in glob.iglob(self.reference + ".*") ])
    index_extensions = set([ e for e in ref_extensions if e in BWA_INDEX_EXT ]) # only extensions pertaining to index
    missing = BWA_INDEX_MANDATORY_EXT - index_extensions
    if missing:
      raise ValueError("Missing BWA index file types: %s" % ', '.join(missing))
    if self.mmap_enabled and (BWA_INDEX_MMAP_EXT - index_extensions):
      raise ValueError("Missing BWA mmap index files: %s" % ', '.join(BWA_INDEX_MMAP_EXT - index_extensions))
    elif not self.mmap_enabled and (BWA_INDEX_NORM_EXT - index_extensions):
      raise ValueError("Missing BWA index files: %s" % ', '.join(BWA_INDEX_NORM_EXT - index_extensions))

# vim: set et:ai:ts=2:sw=2
