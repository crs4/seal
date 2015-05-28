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


from seal.lib.mr.hit_processor_chain_link import HitProcessorChainLink
from seal.lib.io.sam_formatter import SamFormatter

class EmitSamLink(HitProcessorChainLink):
    def __init__(self, context, event_monitor, next_link = None):
        super(EmitSamLink, self).__init__(next_link)
        self.ctx = context
        self.output_formatter = SamFormatter(strip_pe_tag=True)
        self.event_monitor = event_monitor

    def process(self, original, pair):
        for hit in pair:
            if hit:
                k, v = self.output_formatter.format(hit).split("\t", 1)
                self.ctx.emit(str(k), str(v))
                self.event_monitor.count("emitted sam records", 1)

        super(EmitSamLink, self).process(original, pair) # forward pair to next element in chain

class RapiEmitSamLink(HitProcessorChainLink):
    def __init__(self, context, event_monitor, hi_rapi_instance, next_link = None):
        super(RapiEmitSamLink, self).__init__(next_link)
        self.ctx = context
        self.event_monitor = event_monitor
        self.hi_rapi = hi_rapi_instance

    def process(self, original, rapi_frag):
        with self.event_monitor.time_block("format sam", write_status=False):
            sam_lines = self.hi_rapi.format_sam_for_fragment(rapi_frag).split('\n')
        with self.event_monitor.time_block("emit alignments", write_status=False):
            for line in sam_lines:
                k, v = line.split("\t", 1)
                self.ctx.emit(str(k), str(v))
                self.event_monitor.count("emitted sam records", 1)

        super(RapiEmitSamLink, self).process(original, rapi_frag) # forward rapi_frag to next element in chain
