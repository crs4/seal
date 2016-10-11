# Copyright (C) 2011-2016 CRS4.
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


import seal.lib.io.mapping_pb2 as proto
from seal.lib.aligner.mapping import Mapping

import re
import array

def serialize_pair(mapping_pair):
    if len(mapping_pair) != 2:
        raise ValueError("Pair must have 2 mappings. (it had %d)" % len(mapping_pair))
    if not any(mapping_pair):
        raise ValueError("No mappings found in pair")

    message = proto.PairMapping()
    isizes = [0,0]
    names = [None, None]
    for idx, ltr in zip( (0,1), ('A','B') ):
        if mapping_pair[idx]:
            # write mapping_pair[idx] to message.readA or message.readB
            __seq_to_protobuf(getattr(message, "read" + ltr), mapping_pair[idx])
            isizes[idx] = mapping_pair[idx].isize
            names[idx] = re.sub("/[12]$", "", mapping_pair[idx].get_name())

    if all(mapping_pair): # a real pair
        if isizes[0] != -1*isizes[1]:
            raise ValueError("pair insert sizes don't match (%d != -1*%d)" % tuple(isizes))
        message.insert_size = abs(isizes[0])
        if names[0] != names[1]:
            raise ValueError("pair query ids don't match (%s != %s)" % tuple(names))
        message.query_id = names[0]
        message.readA.ClearField("query_id")
        message.readB.ClearField("query_id")
    else: # only one mapping
        message.query_id = names[0] if mapping_pair[0] is not None else names[1] # pick the one that isn't None

    return message.SerializeToString()

def unserialize_pair(message):
    incoming = proto.PairMapping()
    incoming.ParseFromString(message)

    pair = [None, None]
    if incoming.HasField("readA"):
        # We cleared the query_ids when we serialized the pair. Here we restore them
        incoming.readA.query_id = incoming.query_id
        pair[0] = UnserializedMapping()
        pair[0].set_from_memento(incoming.readA)
    if incoming.HasField("readB"):
        incoming.readB.query_id = incoming.query_id
        pair[1] = UnserializedMapping()
        pair[1].set_from_memento(incoming.readB)

    if all(pair): # a real pair
        # The insert size isn't stored in the SeqMapping, so we reset it
        # here.  The mapping that is further up on the chromosome gets
        # the negative insert size
        pair[0].isize = pair[1].isize = incoming.insert_size
        if pair[0].pos < pair[1].pos:
            pair[1].isize *= -1
        else:
            pair[0].isize *= -1
        # set mate position
        for i in 0,1:
            one = pair[i]
            other = pair[i^1]
            one.m_ref_id = other.ref_id
            one.mtid = other.tid
            one.mpos = other.pos

    return tuple(pair)

def serialize_seq(mapping):
    message = proto.SeqMapping()
    return __seq_to_protobuf(message, mapping).SerializeToString()

def unserialize_seq(message):
    incoming = proto.SeqMapping()
    incoming.ParseFromString(message)
    mapping = UnserializedMapping()
    mapping.set_from_memento(incoming)
    return mapping

##
# Theis private function does the actual conversion
# from Mapping object to protobuf SeqMapping message.
def __seq_to_protobuf(proto_map, mapping):
    proto_map.query_id = mapping.get_name()
    proto_map.flags = mapping.flag
    proto_map.reference = mapping.tid or "*"
    proto_map.reference_id = mapping.ref_id if mapping.ref_id is not None else -1
    proto_map.position = mapping.pos
    proto_map.map_q = mapping.qual
    proto_map.cigar = mapping.get_cigar_str()

    proto_map.sequence = mapping.get_seq_5()
    proto_map.sanger_qual = mapping.get_base_qualities().tostring()

    for tag_name, tp, value in mapping.each_tag():
        new_tag = proto_map.tags.add()
        new_tag.name = tag_name
        if tp == "i":
            new_tag.type = proto.SeqMapping.Tag.INTEGER
            new_tag.int_value = int(value)
        elif tp == "f":
            new_tag.type = proto.SeqMapping.Tag.FLOAT
            new_tag.double_value = float(value)
        elif tp == "A":
            new_tag.type = proto.SeqMapping.Tag.CHARACTER
            new_tag.other_value = value
        elif tp == "H":
            new_tag.type = proto.SeqMapping.Tag.HEXSTRING
            new_tag.other_value = value
        elif tp == "Z":
            new_tag.type = proto.SeqMapping.Tag.STRING
            new_tag.other_value = value
        else:
            raise ValueError("Unknown tag type " + tp)
    return proto_map

class UnserializedMapping(Mapping):
    def __init__(self):
        super(UnserializedMapping, self).__init__()

    def get_name(self):
        return self.__name

    def get_base_qualities(self):
        return self.__base_qualities

    def get_cigar(self):
        return self.__cigar

    def each_tag(self):
        for t in self.__tags:
            yield t

    def get_seq_5(self):
        return self.__seq

    def get_seq_len(self):
        return len(self.__seq)

    def set_from_memento(self, protobuf_seq_mapping):
        if protobuf_seq_mapping.HasField("sanger_qual"):
            self.__base_qualities = array.array("B")
            self.__base_qualities.fromstring(protobuf_seq_mapping.sanger_qual)
        self.__cigar = UnserializedMapping.__scan_cigar(protobuf_seq_mapping.cigar)
        self.flag = protobuf_seq_mapping.flags
        self.__name = protobuf_seq_mapping.query_id
        self.qual = protobuf_seq_mapping.map_q
        self.pos = protobuf_seq_mapping.position
        self.__tags = UnserializedMapping.__build_tags(protobuf_seq_mapping.tags)
        self.tid = protobuf_seq_mapping.reference if protobuf_seq_mapping.reference != "*" else None
        self.ref_id = protobuf_seq_mapping.reference_id if protobuf_seq_mapping.reference_id > -1 else None
        self.__seq = protobuf_seq_mapping.sequence

        # sanity checks
        if self.__base_qualities and self.__seq != "=":
            if len(self.__base_qualities) != len(self.__seq):
                raise RuntimeError("sequence length (%d) differs from base_qualities length (%d)" %
                    (len(self.__seq), len(self.__base_qualities)))

    @staticmethod
    def __scan_cigar(cig_str):
        if cig_str == "*":
            return []
        else:
            # verify syntax
            if not re.match("(\d+[MIDS])+", cig_str):
                raise ValueError("Invalid CIGAR syntax: " + cig_str)
            return [ (int(m.group(1)), m.group(2)) for m in re.finditer("(\d+)([MIDS])", cig_str) ]

    @staticmethod
    def __build_tags(serialized):
        tags = []
        for ptag in serialized:
            if ptag.type == proto.SeqMapping.Tag.INTEGER:
                ttype = 'i'
                tvalue = ptag.int_value
            elif ptag.type == proto.SeqMapping.Tag.FLOAT:
                ttype = 'f'
                tvalue = ptag.double_value
            elif ptag.type ==  proto.SeqMapping.Tag.CHARACTER:
                ttype = 'A'
                tvalue = ptag.other_value
            elif ptag.type == proto.SeqMapping.Tag.HEXSTRING:
                ttype = 'H'
                tvalue = ptag.other_value
            elif ptag.type == proto.SeqMapping.Tag.STRING:
                ttype = "Z"
                tvalue = ptag.other_value
            else:
                raise ValueError("Invalid TagType type: " + ptag.type)
            tags.append( (ptag.name, ttype, tvalue) )

        return tags

