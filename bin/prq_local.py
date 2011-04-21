#!/usr/bin/env python

# Copyright (C) 2011 CRS4.
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


"""
Pair qseq reads -- mainly used to check MapReduce implementations.
"""

import sys


def pair_reads(read_fn, mate_fn, out_fn):
  tag = lambda r: "%s_%s:%s:%s:%s:%s#%s" % tuple(r[:7])
  seq = lambda r: r[8].replace(".", "N")
  qual = lambda r: r[9]
  read_f, mate_f, out_f = open(read_fn), open(mate_fn), open(out_fn, "w")
  while 1:
    try:
      read, mate = [f.next().split() for f in (read_f, mate_f)]
    except StopIteration:
      break
    t = tag(read)
    assert tag(mate) == t
    pair = [t, seq(read), qual(read), seq(mate), qual(mate)]
    out_f.write("\t".join(pair)+"\n")
  for f in read_f, mate_f, out_f:
    f.close()


def main(argv):
  try:
    read_fn = sys.argv[1]
    mate_fn = sys.argv[2]
    out_fn = sys.argv[3]
  except IndexError:
    print "Usage: python %s READ_FN MATE_FN OUT_FN" % argv[0]
    sys.exit(2)
  pair_reads(read_fn, mate_fn, out_fn)


if __name__ == "__main__":
  main(sys.argv)
