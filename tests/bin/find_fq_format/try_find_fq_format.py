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

import os, tempfile, shutil, subprocess as sp

EXE = os.path.join(
  os.environ["HOME"],
  "svn/ac-dc/bl/apps/seqal/trunk/bin/find_fq_format.py"
  )

# 3rd seq is sanger, the others are undecidable
SANGER_INPUT = r"""@foo
AAAAAAAAAAAAAAA
+foo
;<=>?@ABCDEFGHI
@foo
AAAAAAAAAAAAAAAA
+foo
;<=>?@ABCDEFGHI
@foo
AAAAAAAAAAAAAAAA
+foo
:;<=>?@ABCDEFGHI
@foo
AAAAAAAAAAAAAAA
+foo
;<=>?@ABCDEFGHI
"""

# 3rd seq is illumina, the others are undecidable
ILLUMINA_INPUT = r"""@foo
AAAAAAAAAAAAAAA
+foo
;<=>?@ABCDEFGHI
@foo
AAAAAAAAAAAAAAAA
+foo
;<=>?@ABCDEFGHI
@foo
AAAAAAAAAAAAAAAA
+foo
;<=>?@ABCDEFGHIJ
@foo
AAAAAAAAAAAAAAA
+foo
;<=>?@ABCDEFGHI
"""


def make_file(tempd, basename, content):
  base_sanger_fn = basename
  sanger_fn = os.path.join(tempd, base_sanger_fn)
  f = open(sanger_fn, 'w')
  f.write(content)
  f.close()
  return sanger_fn


tempd = tempfile.mkdtemp(prefix="try_find_fq_format")

sanger_fn = make_file(tempd, "sanger.fq", SANGER_INPUT)
output = sp.Popen([EXE, sanger_fn], stdout=sp.PIPE).communicate()[0]
print "expected = sanger, result =", output
output = sp.Popen([EXE, sanger_fn, '-n 2'], stdout=sp.PIPE).communicate()[0]
print "expected = unknown, result =", output

illumina_fn = make_file(tempd, "sanger.fq", ILLUMINA_INPUT)
output = sp.Popen([EXE, illumina_fn], stdout=sp.PIPE).communicate()[0]
print "expected = illumina, result =", output
output = sp.Popen([EXE, illumina_fn, '-n 2'], stdout=sp.PIPE).communicate()[0]
print "expected = unknown, result =", output

shutil.rmtree(tempd)
