# BEGIN_COPYRIGHT
#
# Copyright 2009-2015 CRS4.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy
# of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#
# END_COPYRIGHT

"""
A utility module for converting between Avro-based formats and SAM.
"""

def compute_sam_flag(aln, mate_aln=None):
    """
    Compute the SAM flag value from an alignment dictionary-like object.
    `aln` must define the keys:
        * 'readPaired'
        * 'properPair'
        * 'readMapped'
        * 'readNegativeStrand'
        * 'readNum'
        * 'secondaryAlignment'

    If provided, mate_aln must define the keys:
        * 'readNegativeStrand'
        * 'readMapped'
    """
    # SAM_FPD = 1     # paired
    # SAM_FPP = 2     # properly paired
    # SAM_FSU = 4     # self-unmapped
    # SAM_FMU = 8     # mate-unmapped
    # SAM_FSR = 16    # self on the reverse strand
    # SAM_FMR = 32    # mate on the reverse strand
    # SAM_FR1 = 64    # this is read one
    # SAM_FR2 = 128   # this is read two
    # SAM_FSC = 256   # secondary alignment
    # SAM_FQC = 0x200   # failed quality checks
    # SAM_FDP = 0x400   # PCR or optical duplicate
    flag = 0

    bit_value = 1
    for bit in (
        aln['readPaired'],
        aln['properPair'],
        not aln['readMapped'],
        not mate_aln['readMapped'] if mate_aln else False,
        aln['readNegativeStrand'],
        mate_aln['readNegativeStrand'] if mate_aln else False,
        aln['readNum'] == 1,
        aln['readNum'] == 2,
        aln['secondaryAlignment'],
        # missing vendor quality checks and duplicate marking
        ):
        if bit:
            flag |= bit_value
        bit_value *= 2

    return flag

def format_sam_attribute(label, value, type_char=None):
    if type_char:
        return "{}:{}:{}".format(label, type_char, value)

    if isinstance(value, int):
        tag_fmt = "{}:i:{:d}"
    elif isinstance(value, float):
        tag_fmt = "{}:f:{:f}"
    elif len(value) == 1: # one character?
        tag_fmt = "{}:A:{}"
    else:
        tag_fmt = "{}:Z:{}"
    return tag_fmt.format(label, value)

def value_or(v, otherwise):
    """
    return v if v is not None else otherwise
    """
    return v if v is not None else otherwise

