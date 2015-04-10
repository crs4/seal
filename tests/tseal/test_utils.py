
import os
import seal.lib.aligner.mapping as mapping

MiniRefMemDir = os.path.abspath(os.path.join(os.path.dirname(__file__), 'mini_ref',  'bwamem_0.7.8'))
MiniRefMemPath = os.path.join(MiniRefMemDir, 'mini_ref.fasta')
MiniRefReadsPrq = os.path.join(MiniRefMemDir, '..', 'mini_ref_seqs.prq')

# we cache the list produced by get_mini_ref_seqs
_mini_ref_seqs_prq = None

def read_seqs(filename):
    """
    Read a PRQ file of sequences::

        ID	SEQ1	Q1	SEQ2	Q2
    """
    with open(filename) as f:
        # use a tuple since it's immutable
        seqs = tuple([ line.rstrip('\n').split('\t') for line in f ])
    return seqs

def get_mini_ref_seqs():
    global _mini_ref_seqs_prq
    if _mini_ref_seqs_prq is None:
        _mini_ref_seqs_prq = read_seqs(MiniRefReadsPrq)
    return _mini_ref_seqs_prq

def rapi_mini_ref_seqs_sam_no_header():
    return (
    "read_00	65	chr1	32461	60	60M	=	32581	121	AAAACTGACCCACACAGAAAAACTAATTGTGAGAACCAATATTATACTAAATTCATTTGA	EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE	NM:i:0	AS:i:60	MD:Z:60	XS:i:0\n"
    "read_00	129	chr1	32581	60	60M	=	32461	-121	CAAAAGTTAACCCATATGGAATGCAATGGAGGAAATCAATGACATATCAGATCTAGAAAC	EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE	NM:i:0	AS:i:60	MD:Z:60	XS:i:0\n"
    "del3_read_00	65	chr1	32458	60	11M3D49M	=	32581	124	TAGAAAACTGAACACAGAAAAACTAATTGTGAGAACCAATATTATACTAAATTCATTTGA	EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE	NM:i:3	AS:i:51	MD:Z:11^CCC49	XS:i:0\n"
    "del3_read_00	129	chr1	32581	60	60M	=	32458	-124	CAAAAGTTAACCCATATGGAATGCAATGGAGGAAATCAATGACATATCAGATCTAGAAAC	EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE	NM:i:0	AS:i:60	MD:Z:60	XS:i:0\n"
    "ins3_read_00	65	chr1	32464	60	13M3I44M	=	32581	118	ACTGACCCACACAGGGGAAAAACTAATTGTGAGAACCAATATTATACTAAATTCATTTGA	EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE	NM:i:3	AS:i:48	MD:Z:57	XS:i:0\n"
    "ins3_read_00	129	chr1	32581	60	60M	=	32464	-118	CAAAAGTTAACCCATATGGAATGCAATGGAGGAAATCAATGACATATCAGATCTAGAAAC	EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE	NM:i:0	AS:i:60	MD:Z:60	XS:i:0\n"
    "sub2_read_01	65	chr1	27121	60	60M	=	27241	121	GTGCAGATCCCATATGTCCAATAAAAAGGTAAGATCCAAACTCAGATGTCCTATGAGTAT	EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE	NM:i:2	AS:i:50	MD:Z:15T16C27	XS:i:0\n"
    "sub2_read_01	129	chr1	27241	60	60M	=	27121	-121	TCACTAGTTATTTTTAAAATAGGATTGCATGTTGAAATGATAATCTTTTGGATATATTGG	EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE	NM:i:0	AS:i:60	MD:Z:60	XS:i:0\n"
    "read_00_rev	113	chr1	32461	60	60M	=	32581	121	AAAACTGACCCACACAGAAAAACTAATTGTGAGAACCAATATTATACTAAATTCATTTGA	EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEES	NM:i:0	AS:i:60	MD:Z:60	XS:i:0\n"
    "read_00_rev	177	chr1	32581	60	60M	=	32461	-121	CAAAAGTTAACCCATATGGAATGCAATGGAGGAAATCAATGACATATCAGATCTAGAAAC	EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEES	NM:i:0	AS:i:60	MD:Z:60	XS:i:0")

_complement = {
    'A':'T',
    'G':'C',
    'C':'G',
    'T':'A',
    'N':'N',
    'a':'t',
    'g':'c',
    'c':'g',
    't':'a',
    'n':'n'
}

def rev_complement(seq):
    return ''.join( _complement[base] for base in seq[::-1] )

class FakeContig(object):
    def __init__(self, **kwargs):
        self.name = None
        self.len = None
        self.assembly_identifier = None
        self.species = None
        self.uri = None
        self.md5 = None
        for k, v in kwargs.iteritems():
            if hasattr(self, k):
                setattr(self, k, v)
            else:
                raise KeyError("%s is not an attribute" % k)


class FakeRead(object):
    def __init__(self, **kwargs):
        self.id = None
        self.seq = None
        self.qual = None
        self.alignments = []
        for k, v in kwargs.iteritems():
            if hasattr(self, k):
                setattr(self, k, v)
            else:
                raise KeyError("%s is not an attribute" % k)


    def __len__(self):
        return len(self.seq) if self.seq else 0

    def get_aln(self, idx):
        return self.alignments[idx]

    @property
    def prop_paired(self):
        return self.alignments[0].prop_paired

    @property
    def mapped(self):
        return self.alignments[0].mapped

    @property
    def reverse_strand(self):
        return self.alignments[0].reverse_strand

    @property
    def score(self):
        return self.alignments[0].score

    @property
    def mapq(self):
        return self.alignments[0].mapq


class FakeAlignment(object):
    def __init__(self, **kwargs):
        self.contig = None
        self.pos = None
        self.mapq = None
        self.score = None
        self.n_mismatches = None
        self.n_gap_opens = None
        self.n_gap_extensions = None

        self.paired = None
        self.prop_paired = None
        self.mapped = None
        self.reverse_strand = None
        self.secondary_aln = None
        self.cigar_ops = None # [ (cig, len), ... ]
        self.tags = None # a dictionary
        for k, v in kwargs.iteritems():
            if hasattr(self, k):
                setattr(self, k, v)
            else:
                raise KeyError("%s is not an attribute" % k)

    def get_cigar_ops(self):
        return self.cigar_ops

    def get_cigar_string(self):
        return mapping.cigar_str(self.cigar_ops)

    def get_tags(self):
        return self.tags


import sys
import unittest
def disabled_test_msg(msg="Test disabled", io=sys.stderr):
    print >> io, "#" * len(msg)
    print >> io, msg
    print >> io, "#" * len(msg)
    return unittest.TestSuite()
