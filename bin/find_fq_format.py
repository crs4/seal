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


import sys, optparse


SANGER = "sanger"
ILLUMINA = "illumina"
UNKNOWN = "unknown"
# ignoring solexa-illumina 1.0 and illumina 1.5+


def iterq(f, max_reads):
  """
  Iterate over quality lines from a fastq file.
  """
  read_count = line_count = 0
  for line in f:
    line = line.strip()
    if not line:
      continue
    else:
      line_count += 1
    if line_count == 4:
      yield line
      line_count = 0
      read_count += 1
      if read_count >= max_reads:
        break


# very simple -- no estimation, just stop when critical values are found
def find_fq_subformat(q_iterator):  
  for q_line in q_iterator:
    qv = [ord(q) for q in q_line]
    if min(qv) < 59:
      return SANGER
    elif max(qv) > 73:
      return ILLUMINA
  return UNKNOWN
    

def make_parser():
    parser = optparse.OptionParser('%prog [OPTIONS] FQ_FILE')
    parser.add_option(
      "-n", type="int", dest="maxreads", metavar="INT", default=10000,
      help="how many reads will be read from file (%default)",
      )
    return parser


def main(argv):

    parser = make_parser()
    opt, args = parser.parse_args(argv)
    if len(args) < 2:
        parser.print_help()
        sys.exit(2)
    fqfn = args[1]

    f = open(fqfn)
    fmt = find_fq_subformat(iterq(f, opt.maxreads))
    f.close()
    print fmt


if __name__ == "__main__":
  main(sys.argv)
