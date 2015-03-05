
import os

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
