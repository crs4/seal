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

from pydoop.pipes import Reducer
from pydoop.utils import jc_configure, jc_configure_bool

import bl.lib.seq.aligner.io.protobuf_mapping as protobuf_mapping
from bl.mr.lib.hadoop_event_monitor import HadoopEventMonitor
from bl.mr.lib.hit_processor_chain_link import HitProcessorChainLink
from bl.mr.lib.emit_sam_link import EmitSamLink
import bl.lib.tools.deprecation_utils as deprecation_utils
import seqal_app

class reducer(Reducer):
	COUNTER_CLASS = "SEQAL" # TODO:  refactor so that mapper and reducer have a common place for things like this constant

	DeprecationMap = {
	  'seal.seqal.log.level': 'bl.seqal.log.level',
	  'seal.seqal.discard_duplicates': 'bl.seqal.discard_duplicates'
	}

	def __init__(self, ctx):
		super(reducer, self).__init__(ctx)

		jc = ctx.getJobConf()
		logger = logging.getLogger("seqal")
		jobconf = deprecation_utils.convert_job_conf(jc, self.DeprecationMap, logger)

		jc_configure(self, jobconf, 'seal.seqal.log.level', 'log_level', 'INFO')
		jc_configure_bool(self, jobconf, 'seal.seqal.discard_duplicates', 'discard_duplicates', False)

		logging.basicConfig(level=self.log_level)

		self.event_monitor = HadoopEventMonitor(self.COUNTER_CLASS, logging.getLogger("reducer"), ctx)
		self.__output_sink = EmitSamLink(ctx, self.event_monitor)

	def __process_unmapped_pairs(self, ctx):
		while ctx.nextValue():
			value = ctx.getInputValue()
			pair = protobuf_mapping.unserialize_pair(value)
			self.__output_sink.process(pair)

	def reduce(self, ctx):
		# create the "workspace"
		self.__pairs = []
		self.__unpaired = []

		# gather input
		key_values = ctx.getInputKey().split(':')
		if key_values[0] == seqal_app.UNMAPPED_STRING:
			# pair of unmapped sequences
			self.__process_unmapped_pairs(ctx)
		else:
			if len(key_values) != 3:
				raise RuntimeError("Unexpected key length %d.  Expected key format is ref_id:pos:orient" % len(key))
			# convert key values and make it a tuple
			key = (int(key_values[0]), int(key_values[1]), key_values[2] == 'R') # last value is True if reverse strand

			have_pairs = False # keep track of whether we have at least one real pair.
			# load mappings
			while ctx.nextValue():
				value = ctx.getInputValue()
				if value == seqal_app.PAIR_STRING:
					have_pairs = True
				else:
					pair = protobuf_mapping.unserialize_pair(value)
					if pair[0] is None or pair[0].is_unmapped():
						# Sanity check. pair[0] should never be None or unmapped here.
						raise ValueError("Error!  Got None or unmapped in first read for key %s.  pair: %s" % (key, pair))

					if pair[1] and pair[1].is_unmapped():
						self.__output_sink.process( (pair[1], None) )
						self.__unpaired.append( (pair[0], None) )
					elif pair[1] is None:
						self.__unpaired.append(pair)
					else:
						# Two mapped reads.
						# pair[0] should never be unmapped.  That case should be handled by
						# __process_unmapped_pairs.
						self.__pairs.append(pair)
						have_pairs = True

			self.__process_pairs()
			self.__process_fragments(have_pairs)

		# clean-up the workspace
		self.__pairs = None
		self.__unpaired = None

	def __process_pairs(self):
		# All pairs whose 5'-most coordinate matches the key,
		# and are not duplicate pairs, will be emitted
		keep_pairs = dict()
		for p in self.__pairs:
			p_key = get_pair_key(p) # makes the key on which we base the comparison between pairs
			# If we already have a pair with this key, then keep the one with the highest score.
			# If we haven't already seen the key, put the pair in the hash.
			if keep_pairs.has_key(p_key):
				if get_map_pair_score(keep_pairs[p_key]) < get_map_pair_score(p):
					dup_pair = keep_pairs[p_key]
					keep_pairs[p_key] = p
				else:
					dup_pair = p
				self.event_monitor.count("duplicate pairs")
				if not self.discard_duplicates: # emit the duplicates if we need to
					for r in dup_pair:
						r.set_duplicate(True)
					self.__output_sink.process(dup_pair)
			else:
				keep_pairs[p_key] = p
		# finally, emit the pairs that we've kept
		self.event_monitor.count("rmdup unique pairs", len(keep_pairs))
		for pair in keep_pairs.itervalues():
			self.__output_sink.process(pair)

	def __process_fragments(self, with_pairs):
		# All fragments that are not the duplicate of another
		# fragment, be it in a pair or alone, will be emitted.
		#
		# All fragments we analyze here will have been emitted for the same coordinate
		# (the one referenced by the key).  Therefore, they automatically have a
		# duplicate in any pairs we have received. As a consequence, we only look at
		# them if we haven't seen any pairs.
		#
		# with_pairs => implies we have proper pairs for the key position,
		# so all lone fragments are to be discarded as duplicates.
		#
		# not with_pairs => we have no proper pairs for the key position.
		# Duplicates will be selected by quality."""
		if with_pairs:
			# all fragments are duplicates
			self.event_monitor.count("duplicate fragments", len(self.__unpaired))
			if not self.discard_duplicates:
				for dup in self.__unpaired: # for each unpaired fragment
					dup[0].set_duplicate(True)
					self.__output_sink.process(dup)
		else:
			fragments = dict()
			for m,none in self.__unpaired: # for each unpaired fragment
				k = get_mapping_key(m)
				if fragments.has_key(k):
					if get_map_score(fragments[k]) < get_map_score(m):
						dup = fragments[k]
						fragments[k] = m
					else:
						dup = m
					self.event_monitor.count("duplicate fragments")
					if not self.discard_duplicates:
						dup.set_duplicate(True)
						self.__output_sink.process((dup,None))
				else:
					fragments[k] = m
			# now emit the remaining fragments
			self.event_monitor.count("rmdup unique fragments", len(fragments))
			for m in fragments.itervalues():
				self.__output_sink.process( (m,None) )

def get_mapping_key(mapping):
	return (mapping.ref_id, mapping.get_untrimmed_pos(), mapping.is_on_reverse())

def get_pair_key(pair):
	return (pair[0].ref_id, pair[0].get_untrimmed_pos(), pair[0].is_on_reverse(), pair[1].ref_id, pair[1].get_untrimmed_pos())

def get_map_score(mapping):
	"""
	Sum of all base quality scores >= 15.
	"""
	bq = mapping.get_base_qualities()
	return sum([value for value in bq if value >= 15]) if bq else 0

def get_map_pair_score(pair):
	return get_map_score(pair[0]) + get_map_score(pair[1])
