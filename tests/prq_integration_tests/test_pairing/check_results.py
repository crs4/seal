#!/usr/bin/env python

import fileinput
import os
import re
import sys

# expected output:
# CRESSIA_129:1:1:1003:1171#0	read 1 sequence 1	read 1 quality 1	read 2 sequence 1	read 2 quality 1
# CRESSIA_129:1:1:1010:1209#0	read 1 sequence 3	read 1 quality 3	read 2 sequence 3	read 2 quality 3
# etc etc...

read_pattern = re.compile("read (\d+)")
expected_reads = [ '1', '1', '2', '2' ]

sequence_pattern = re.compile("(?:sequence|quality) (\d+)")

if any( map(lambda s: s == 0, map(os.path.getsize, sys.argv[1:])) ):
	raise StandardError("we have at least one empty output file")

for line in fileinput.input(sys.argv[1:]):
  # ensure we have the two different reads
  reads = read_pattern.findall(line)
  if reads != expected_reads:
    raise StandardError("got unexpected reads pattern '%s' (expected %s)" % (reads, expected_reads))

  # ensure the line only references one sequence
  matches = sequence_pattern.findall(line)

  if len(matches) != 4:
    raise StandardError("expected 4 sequence ids by found %d" % len(matches))

  if any(matches[0] != matches[i] for i in range(len(matches))):
    raise StandardError("output record contains mismatched sequence fragments" % matches)
