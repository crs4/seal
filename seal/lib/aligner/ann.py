
from bisect import bisect_right

def read_line_tuples(io):
  return [ line.rstrip("\n").split() for line in io.xreadlines() ]

class Contig(object):
  def __init__(self, startpos, length):
    self.abs_pos = startpos
    self.length = length

class ReferenceAnnotations(object):
  def __init__(self):
    self.contigs = dict()
    self.hole_coord = []
    self.hole_length = []

  def abs_coord(self, chr_name, rel_pos):
    contig = self.contigs[chr_name]
    return contig.abs_pos + rel_pos - 1 # 0 based

  def on_hole(self, chr_name, rel_pos, read_length):
    coord = self.abs_coord(chr_name, rel_pos)
    idx = bisect_right(self.hole_coord, coord) # find the right-most hole that starts before the read
    # idx is actually where we would insert the next item in the list, i.e. one position after the
    # start of the last read to start before the read.

    # if read starts before the hole ends (i.e., on the hole)
    if idx > 0 and coord - self.hole_coord[idx-1] < self.hole_length[idx-1]:
      return True

    # if the read ends after the next hole starts 
    if idx < len(self.hole_coord) and (coord + read_length) > self.hole_coord[idx]:
      return True

    # else, no hole
    return False

  @staticmethod
  def load(prefix):
    ann = ReferenceAnnotations()
    with open(prefix + ".ann") as io:
      lines = read_line_tuples(io)
      if len(lines) <= 1:
        raise RuntimeError("No contigs defined!")
      if len(lines) % 2 == 0:
        raise RuntimeError("Even number of lines in ann file!")
      if int(lines[0][1]) !=  (len(lines) - 1) / 2:
        raise RuntimeError("Inconsistency in ann file!  First line indicates %s contigs but I have %s lines!" % (lines[0][1], len(lines)))
      for i in xrange(1, len(lines), 2):
        name = lines[i][1]
        coords = map(int, lines[i+1]) # offset, length, num gaps
        ann.contigs[name] = Contig(coords[0], coords[1])

    with open(prefix + ".amb") as io:
      lines = read_line_tuples(io)
      n_holes = int(lines[0][2])
      if n_holes != len(lines) - 1:
        raise RuntimeError("Inconsistency in amb file!  First line indicates %s holes but I have %s lines!" % (lines[0][2], len(lines)))
      ann.hole_coord = [0]*n_holes
      ann.hole_length = [0]*n_holes

      for i in xrange(1, len(lines)):
        ann.hole_coord[i-1] = int(lines[i][0])
        ann.hole_length[i-1] =  int(lines[i][1])
    return ann
