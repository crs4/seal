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

from seal.lib.aligner.sam_flags import *

from itertools import izip
import array

class Mapping(object):
    """
    Abstract class defining an interface to describe
    sequence mapping.  A concrete implementation needs
    to set the attributes to appropriate values and
    implement any abstract methods (e.g. get_seq_5).

    Attributes:
    self.flag: SAM flag bits
    self.isize: insert size
    self.mpos: mate's position, or 0
    self.mtid: mate's reference , or '=' if same as query's reference, or None
    self.m_ref_id:  mate's reference id, or None
    self.pos: this hit's position, or 0
    self.qual: mapq value for this alignment
    self.tid: this hit's reference sequence, or None
    self.ref_id:  this hit's reference id, or None
    """

    def __init__(self):
        self.flag = 0
        self.isize = 0
        self.mpos = 0
        self.mtid = None
        self.m_ref_id = None
        self.pos = 0
        self.qual = 0
        self.tid = None
        self.ref_id = None

    def get_name(self):
        """This fragment's id"""
        abstract

    def get_seq_5(self):
        abstract

    def get_seq_len(self):
        return len(self.get_seq_5())

    def get_base_qualities(self):
        """If the base qualities are available, this method
        returns a byte array.array of Phred quality scores,
        one per base, 0-based.  If base qualities aren't
        available it return None"""
        abstract

    def get_ascii_base_qual(self):
        """
        Returns a string containing the Phred base quality score
        encoded in ASCII-33 (ASCII character corresponding to
        quality score + 33)
        """
        if not hasattr(self, '__ascii_base_qual'):
            self.__ascii_base_qual = ''.join([ chr(q+33) for q in self.get_base_qualities() ])
        return self.__ascii_base_qual

    def get_cigar(self):
        """Return the cigar list for this mapping.
        Each element is a pair (length, action),
        where action is a letter for one of the alignment actions
        (e.g. M, I, D, S).  If unmapped return an empty list.
        """
        abstract

    def get_cigar_str(self):
        if not hasattr(self, '__cigar_str'):
            self.__cigar_str = "".join(['%d%s' % t for t in self.get_cigar()]) or "*"
        return self.__cigar_str

    def get_untrimmed_left_pos(self):
        """
        For a reversed, trimmed fragment the 5' coordinate pos refers to a base
        somewhere within the fragment (not at its start or end).  This method
        takes into account the trimming (if any, shown as soft or hard clipping
        in the CIGAR) and calculates the pos of the 5'-most base of the untrimmed
        fragment.
        """
        if not hasattr(self, "__untrimmed_left_pos"):
            if self.is_unmapped():
                raise ValueError("sequence is not mapped")

            upos = self.pos
            for length, op in self.get_cigar():
                if op == 'S' or op == 'H':
                    upos -= length
                else:
                    break
            self.__untrimmed_left_pos = upos
        return self.__untrimmed_left_pos

    def get_right_pos(self):
        """
        This computes the reference position of the right-most base of the sequence.
        """
        if not hasattr(self, "__right_pos"):
            if self.is_unmapped():
                raise ValueError("sequence is not mapped")

            advancement = 0
            advancing_ops = ('M','D','N')
            for length, op in self.get_cigar():
                if op in advancing_ops:
                    advancement += length
            if advancement > 0:
                advancement -= 1
            self.__right_pos = self.pos + advancement
        return self.__right_pos

    def get_untrimmed_right_pos(self):
        """
        This computes the reference position of the right-most base of the sequence,
        including any trimmed bases.
        """
        right_pos = self.get_right_pos()
        for length, op in reversed(self.get_cigar()):
            if op == 'S' or op == 'H':
                right_pos += length
            else:
                break
        return right_pos


    ####################################
    # Tag methods
    ####################################
    def each_tag(self):
        """Generator for this mapping's tags.  Each tag is
        a tuple (tag_name, value_type, value)"""
        abstract

    def tag_value(self, tag_name):
        """
        Get the value for a tag, if present.
        Returns:
            - integer (if tag type is 'i')
            - float (if tag type is 'f')
            - string
            - None, if tag isn't present
        """
        for tag in self.each_tag():
            if tag[0] == tag_name:
                if tag[1] == 'i':
                    value = int(tag[2])
                elif tag[1] == 'f':
                    value = float(tag[2])
                else:
                    value = tag[2]
                return value
        return None

    ####################################
    # Flag methods
    ####################################
    def flag_string(self):
        """
          Returns a string indicating symbolically the flags set for this mapping.
          Symbol legend:
          p: paired                    FPD   0x001
          P: properly paired           FPP   0x002
          u: query unmapped            FSU   0x004
          U: mate unmapped             FMU   0x008
          r: query on reverse strand   FSR   0x010
          R: mate on reverse strand    FMR   0x020
          1: first read in a pair      FR1   0x040
          2: second read in a pair     FR2   0x080
          s: not a primary alignment   FSC   0x100
          f: fails quality checks            0x200
          d: duplicate                       0x400
        """
        names = [ 'p', 'P', 'u', 'U', 'r', 'R', '1', '2', 's', 'f', 'd']
        values = [ 0x0001, 0x0002, 0x0004, 0x0008, 0x0010, 0x0020, 0x0040, 0x0080, 0x0100, 0x0200, 0x0400]
        return ''.join( [ n for n,v in izip(names, values) if self.flag & v != 0 ] )


    def is_paired(self):
        return self.flag & SAM_FPD != 0

    def is_properly_paired(self):
        return self.flag & SAM_FPP != 0

    def is_mapped(self):
        return self.flag & SAM_FSU == 0

    def is_unmapped(self):
        return self.flag & SAM_FSU != 0

    def is_mate_mapped(self):
        return self.flag & SAM_FMU == 0

    def is_mate_unmapped(self):
        return self.flag & SAM_FMU != 0

    def is_on_reverse(self):
        return self.flag & SAM_FSR != 0

    def is_mate_on_reverse(self):
        return self.flag & SAM_FMR != 0

    def is_read1(self):
        return self.flag & SAM_FR1 != 0

    def is_read2(self):
        return self.flag & SAM_FR2 != 0

    def is_secondary_align(self):
        return self.flag & SAM_FSC != 0

    def is_failed_qc(self):
        return self.flag & SAM_FQC != 0

    def is_duplicate(self):
        return self.flag & SAM_FDP != 0

    def set_paired(self, value):
        self.__set_flag(value, SAM_FPD)
        return self

    def set_properly_paired(self, value):
        self.__set_flag(value, SAM_FPP)
        return self

    def set_mapped(self, value):
        self.__set_flag(not value, SAM_FSU)
        return self

    def set_mate_mapped(self, value):
        self.__set_flag(not value, SAM_FMU)
        return self

    def set_on_reverse(self, value):
        self.__set_flag(value, SAM_FSR)
        return self

    def set_mate_on_reverse(self, value):
        self.__set_flag(value, SAM_FMR)
        return self

    def set_read1(self, value):
        self.__set_flag(value, SAM_FR1)
        return self

    def set_read2(self, value):
        self.__set_flag(value, SAM_FR2)
        return self

    def set_secondary_align(self, value):
        self.__set_flag(value, SAM_FSC)
        return self

    def set_failed_qc(self, value):
        self.__set_flag(value, SAM_FQC)
        return self

    def set_duplicate(self, value):
        self.__set_flag(value, SAM_FDP)
        return self

    def __set_flag(self, new_value, flag_mask):
        if new_value:
            self.flag |= flag_mask
        else:
            self.flag &= ~flag_mask

    def remove_mate(self):
        """
        removes all info pertaining to this mapping's mate.
        """
        self.set_properly_paired(False)
        self.set_mate_mapped(True) # turns off the bit
        self.mpos = 0
        self.mtid = None
        self.m_ref_id = None
        self.isize = 0

######################################
# SimpleMapping
# A mapping implementation with setters for  attributes.
# Useful for testing.
######################################
class SimpleMapping(Mapping):
    def __init__(self):
        super(SimpleMapping, self).__init__()
        self.__name = ""
        self.__base_qualities = array.array('B')
        self.__cigar = []
        self.__tags = []
        self.__seq = ""

    def set_name(self, name):
        self.__name = name

    def set_base_qualities(self, bq):
        if bq is not None and not isinstance(bq, array.array):
            raise TypeError("Invalid type for base qualities (%s)" % str(type(bq)))
        self.__base_qualities = bq

    def set_cigar(self, cig):
        if cig is not None and not isinstance(cig, list):
            raise TypeError("Invalid type for cigar (%s)" % str(type(cig)))
        self.__cigar = cig

    def add_tag(self, tag):
        self.__tags.append(tag)

    def set_seq_5(self, s):
        self.__seq = s

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
