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

from bl.mr.lib.hit_processor_chain_link import HitProcessorChainLink

class FilterLink(HitProcessorChainLink):
	def __init__(self, monitor, next_link = None):
		super(type(self), self).__init__(next_link)
		self.min_hit_quality = 1
		self.remove_unmapped = True # if true, all unmapped are removed regardless of hit quality
		self.event_monitor = monitor

	def __remove_i(self, pair, i):
		pair[i] = None
		other_hit = pair[i^1]
		if other_hit:
			other_hit.remove_mate()
		return pair

	def process(self, pair):
		if len(pair) != 2:
			raise ValueError("pair length != 2 (it's %d)" % len(pair))
		pair = list(pair) # tuples can't be modified
		for i in 0,1:
			if self.remove_unmapped and pair[i].is_unmapped():
				pair = self.__remove_i(pair, i)
				self.event_monitor.count("reads filtered: unmapped")
			elif pair[i].qual < self.min_hit_quality:
				pair = self.__remove_i(pair, i)
				self.event_monitor.count("reads filtered: low quality")

		if self.next_link and any(pair):
			self.next_link.process(tuple(pair)) # forward pair to next element in chain
