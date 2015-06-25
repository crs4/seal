
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

import random

import seal.lib.io.protobuf_mapping as protobuf_mapping
from seal.lib.mr.hit_processor_chain_link import HitProcessorChainLink
from seal.seqal import seqal

class MarkDuplicatesEmitter(HitProcessorChainLink):
    def __init__(self, context, event_monitor, next_link = None):
        super(MarkDuplicatesEmitter, self).__init__(next_link)
        self.ctx = context
        self.event_monitor = event_monitor

    def __order_pair(self, pair):
        # Order pair such that left-most read is at pos 0.
        # Unmapped reads come after all positions.  None values are last.
        raise NotImplementedError("Not updated for RAPI")

        if pair[1] is None:
            ordered_pair = pair
        elif pair[0] is None:
            ordered_pair = (pair[1], pair[0])
        elif pair[1].is_unmapped(): # there are no None values
            ordered_pair = pair
        elif pair[0].is_unmapped():
            ordered_pair = (pair[1], pair[0])
        #else there are no unmapped reads
        elif (pair[0].ref_id, pair[0].get_untrimmed_left_pos(), pair[0].is_on_reverse()) > \
             (pair[1].ref_id, pair[1].get_untrimmed_left_pos(), pair[1].is_on_reverse()):
            ordered_pair = (pair[1], pair[0])
        else:
            ordered_pair = pair

        return ordered_pair

    def process(self, original, aligned_pair):
        if any(aligned_pair):
            # order pair such that left-most read is at pos 0
            ordered_pair = self.__order_pair(aligned_pair)

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
                        self.ctx.emit(key, seqal.PAIR_STRING)
                        self.event_monitor.count("mapped coordinates", 1)
                    else:
                        self.event_monitor.count("unmapped reads", 1)
            else:
                self.event_monitor.count("unmapped reads", len(aligned_pair))

        # in all cases, forward the original pair to the link in the chain
        if self.next_link:
            self.next_link.process(original, aligned_pair)

    @staticmethod
    def get_hit_key(hit):
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
            if hit.is_on_reverse():
                pos = hit.get_untrimmed_right_pos()
                orientation = 'R'
            else:
                pos = hit.get_untrimmed_left_pos()
                orientation = 'F'
            values = ("%04d" % hit.ref_id, "%012d" % pos, orientation)
        else:
            values = (seqal.UNMAPPED_STRING, "%010d" % random.randint(0, 9999999999))
        return ':'.join( values )
