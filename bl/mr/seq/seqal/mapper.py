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

import struct
import logging
from itertools import izip
import os
import random

from pydoop.pipes import Mapper, InputSplit
from pydoop.utils import jc_configure, jc_configure_int, jc_configure_bool

from bl.lib.seq.aligner.bwa.bwa_aligner import BwaAligner, BWA_INDEX_EXT
import bl.lib.seq.aligner.io.protobuf_mapping as protobuf_mapping
import bl.mr.seq.utils as utils
from bl.mr.lib.hit_processor_chain_link import HitProcessorChainLink
from bl.mr.lib.emit_sam_link import EmitSamLink
from bl.mr.lib.filter_link import FilterLink
from bl.mr.lib.hadoop_event_monitor import HadoopEventMonitor
import seqal_app


class MarkDuplicatesEmitter(HitProcessorChainLink):
	def __init__(self, context, event_monitor, next_link = None):
		super(type(self), self).__init__(next_link)
		self.ctx = context
		self.event_monitor = event_monitor

	def __order_pair(self, pair):
		# Order pair such that left-most read is at pos 0.
		# Unmapped reads come after all positions.  None values are last.
		
		if not any(pair): # if they're all None it makes no difference
			return pair

		if pair[1] is None:
			ordered_pair = pair
		elif pair[0] is None:
			ordered_pair = (pair[1], pair[0])
		elif pair[1].is_unmapped(): # there are no None values
			ordered_pair = pair
		elif pair[0].is_unmapped():
			ordered_pair = (pair[1], pair[0])
		#else there are no unmapped reads
		elif (pair[0].ref_id, pair[0].get_untrimmed_pos()) <= (pair[1].ref_id, pair[1].get_untrimmed_pos()):
			ordered_pair = pair
		else:
			ordered_pair = (pair[1], pair[0])

		return ordered_pair

	def process(self, pair):
		if any(pair):
			# order pair such that left-most read is at pos 0
			ordered_pair = self.__order_pair(pair)

			record = protobuf_mapping.serialize_pair(ordered_pair)
			# emit with the left coord
			key = self.get_hit_key(ordered_pair[0])
			self.ctx.emit(key, record)
			if ordered_pair[0].is_mapped():
				self.event_monitor.count("mapped coordinates", 1)
				# since we ordered the pair, if ordered_pair[0] is unmapped
				# ordered_pair[1] will not be mapped.
				if ordered_pair[1]:
					if ordered_pair[1].is_mapped():
						# a full pair. We emit the coordinate, but with PAIR_STRING as the value
						key = self.get_hit_key(ordered_pair[1])
						self.ctx.emit(key, seqal_app.PAIR_STRING)
						self.event_monitor.count("mapped coordinates", 1)
					else:
						self.event_monitor.count("unmapped reads", 1)
			else:
				self.event_monitor.count("unmapped reads", len(pair))

		# in all cases, forward the original pair to the link in the chain
		if self.next_link:
			self.next_link.process(pair)

	def get_hit_key(self, hit):
		"""Build a key to identify a read hit. 
		   To get the proper order in the lexicographic sort, we use a 12-digit
			 field for the position, padding the left with 0s.  12 digits should
			 be enough for any genome :-)

			 We do the same thing for the contig, using its id instead of its name (the tid field).
			 This gives us almost a sorted order in the reducer output; the only entries
			 out of place are the reversed and trimmed reads, since their untrimmed position
			 (used in the key) is different from the reference position.

			 On the other hand, if the read is unmapped, we make a key that starts with the string
			 'unmapped:' and then has a 10-digit random number.  The randomness is inserted so that
			 the unmapped reads may be distributed to the various reducers, instead of having them
			 all send to the same one (since they would all have the same key).
		"""
		if hit.is_mapped():
			values = ("%04d" % hit.ref_id, "%012d" % hit.get_untrimmed_pos(), 'R' if hit.is_on_reverse() else 'F')
		else:
			# 
			values = (seqal_app.UNMAPPED_STRING, "%010d" % random.randint(0, 9999999999))
		return ':'.join( values )

class mapper(Mapper):
	"""
	Aligns sequences to a reference genome.

	@input-record: C{key} does not matter (standard LineRecordReader);
	C{value} is a tab-separated text line with 5 fields: ID, read_seq,
	read_qual, mate_seq, mate_qual.

	@output-record: protobuf-serialized mapped pairs (map-reduce job) or alignment 
	records in SAM format (map-only job).

	@jobconf-param: C{mapred.reduce.tasks} number of Hadoop reduce tasks to launch.
	If the value of this property is set to 0, then the mapper will directly output
	the mappings in SAM format, like BWA.  If set to a value > 0 the mapper will output
	mappings in the protobuf serialized format for the rmdup reducer.

	@jobconf-param: C{bl.seqal.log.level} logging level,
	specified as a logging module literal.

	@jobconf-param: C{mapred.cache.archives} distributed
	cache entry for the bwa index archive. The entry
	is of the form HDFS_PATH#LINK_NAME. The archive for a given
	chromosome must contain (at the top level, i.e., no directories) all
	files generated by 'bwa index' for that chromosome.

	@jobconf-param: C{bl.seqal.alignment.max.isize}: if the
	inferred isize is greater than this value, Smith-Waterman alignment
	for unmapped reads will be skipped.

	@jobconf-param: C{bl.seqal.pairing.batch.size}: how many
	sequences should be processed at a time by the pairing
	function. Status will be updated at each new batch: therefore,
	lowering this value can help avoid timeouts.

	@jobconf-param: C{bl.seqal.fastq-subformat} Specifies base quality 
	score encoding.  Supported types are: 'fastq-sanger' and 'fastq-illumina'.

	@jobconf-param: C{mapred.create.symlink} must be set to 'yes'.

	@jobconf-param: C{bl.seqal.min_hit_quality} mapping quality
	threshold below which the mapping will be discarded.
	"""
	SUPPORTED_FORMATS = "fastq-illumina", "fastq-sanger"
	DEFAULT_FORMAT = "fastq-illumina"
	COUNTER_CLASS = "SEQAL"

	def __get_configuration(self, ctx):
		# TODO:  refactor settings common to mapper and reducer
		jc = ctx.getJobConf()
		jc_configure(self, jc, 'bl.seqal.log.level', 'log_level', 'INFO')
		try:
			self.log_level = getattr(logging, self.log_level)
		except AttributeError:
			raise ValueError("Unsupported log level: %r" % self.log_level)
		jc_configure(self, jc, "bl.seqal.fastq-subformat", "format", self.DEFAULT_FORMAT)
		if self.format not in self.SUPPORTED_FORMATS:
			raise_pydoop_exception(
			  "fastq_reader: bl.mr.fastq-subformat must be one of %r" %
			  (self.SUPPORTED_FORMATS,)
			  )
		jc_configure_int(self, jc, 'bl.seqal.alignment.max.isize', 'max_isize', 1000)
		if self.max_isize <= 0:
			raise ValueError("'bl.seqal.alignment.max.isize' must be > 0, if specified [1000]")

		jc_configure_int(self, jc, 'bl.seqal.pairing.batch.size', 'batch_size', 10000)
		if self.batch_size <= 0:
			raise ValueError("'bl.seqal.pairing.batch.size' must be > 0, if specified [10000]")

		# minimum qual value required for a hit to be kept.  By default outputs all the
		# hits BWA returns.
		jc_configure_int(self, jc, 'bl.seqal.min_hit_quality', 'min_hit_quality', 0)
		if self.min_hit_quality < 0:
			raise ValueError("'bl.seqal.min_hit_quality' must be >= 0, if specified [0]")

		# remove unmapped reads
		jc_configure_bool(self, jc, 'bl.seqal.remove_unmapped', 'remove_unmapped', False)

		# number of concurrent threads for main alignment operation
		jc_configure_int(self, jc, 'bl.seqal.nthreads', 'nthreads', 1)
		if self.nthreads <= 0:
			raise ValueError("'bl.seqal.nthreads' must be > 0, if specified [1]")

		# trim quality parameter used by BWA from read trimming.  Equivalent to 
		# the -q parameter for bwa align
		jc_configure_int(self, jc, 'bl.seqal.trim.qual', 'trim_qual', 0)
		if self.trim_qual < 0:
			raise ValueError("'bl.seqal.trim.qual' must be >= 0, if specified [0]")

		if jc.hasKey('mapred.reduce.tasks') and jc.getInt('mapred.reduce.tasks') > 0:
			self.__map_only = False
		else:
			self.__map_only = True

	def __is_last_record(self, k, v):
		return k + len(v) + 2 >= self.split_end


	def get_reference_root(self, ref_dir):
		"""
		Given a directory containing a BWA indexed reference,
		such that all its files have a common name (except the extension),
		this method find the path to the reference including the common name.
		 e.g. my_reference/hg_18.bwt
		      my_reference/hg_18.rsax
		      my_reference/hg_18.sax   => "my_references/hg_18"
		      my_reference/hg_18.pac
		      my_reference/irrelevant_file
		"""
		index_paths = filter(lambda tpl: tpl[1].lstrip('.') in BWA_INDEX_EXT, 
		                        map(os.path.splitext, os.listdir(ref_dir)))
		roots = set( zip(*index_paths)[0] )
		if len(roots) == 0:
			raise(ValueError, "Missing references.  Didn't find any files with required extensions (%s) at path %s" % (BWA_INDEX_EXT, ref_dir))
		elif len(roots) != 1:
			raise(ValueError, "multiple references? Found reference roots %s" % (roots,))

		return os.path.join(ref_dir, tuple(roots)[0])

	def __init__(self, ctx):
		super(type(self), self).__init__(ctx)
		self.__get_configuration(ctx)
		logging.basicConfig(level=self.log_level)
		self.event_monitor = HadoopEventMonitor(self.COUNTER_CLASS, logging.getLogger("mapper"), ctx)

		self.aligner = BwaAligner()
		self.aligner.event_monitor = self.event_monitor
		self.aligner.qformat = self.format
		self.aligner.max_isize = self.max_isize
		self.aligner.nthreads = self.nthreads
		self.aligner.trim_qual = self.trim_qual
		self.aligner.mmap_enabled = True

		######## assemble hit processor chain
		chain = FilterLink(self.event_monitor)
		chain.remove_unmapped = self.remove_unmapped
		chain.min_hit_quality = self.min_hit_quality
		if self.__map_only:
			chain.set_next( EmitSamLink(ctx, self.event_monitor) )
		else:
			chain.set_next( MarkDuplicatesEmitter(ctx, self.event_monitor) )
		self.aligner.hit_visitor = chain

		######## set the path to the reference index
		self.ref_archive = utils.get_ref_archive(ctx.getJobConf())
		self.aligner.reference = self.get_reference_root(self.ref_archive)

		# part of the code is a workaround for accumulating records, see #331
		isplit = InputSplit(ctx.getInputSplit())
		self.split_end = isplit.offset + isplit.length

	def map(self, ctx):
		# Accumulates reads in self.pairs, until batch size is reached or
		# until the input is finished.	At that point it calls run_alignment
		# and emits the output.
		k = struct.unpack(">q", ctx.getInputKey())[0]
		v = ctx.getInputValue()
		self.aligner.load_pair_record(v.split("\t"))
		is_last_record = self.__is_last_record(k, v)
		if self.aligner.get_batch_size() >= self.batch_size or is_last_record:
			self.aligner.run_alignment()
			self.aligner.clear_batch()

		if is_last_record:
			self.aligner.release_resources()
