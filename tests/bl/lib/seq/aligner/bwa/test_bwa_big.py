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

"""
How to use this test:

You need to provide as command line arguments:
  1. The fasta reference sequence file name
  2, 3. The two reads and mates sequence files

The test will print an error count and generate a dump of errors.

See README.test_bwa_big for expected results on known test cases.
"""

import sys, unittest, time, logging, optparse
import testing_utilities as u

import Bio.SeqIO

import itertools as it


class bwa_big_tc(unittest.TestCase):

  PARAMS = None

  @classmethod
  def set_params(cls, args, opt):
    cls.PARAMS = (args, opt)

  def setUp(self):
    (self.refseq_fn, self.read_fn, self.mate_fn), self.opt = self.PARAMS

  def tearDown(self):
    if not self.opt.no_clean:
      for fn in [self.refseq_fn, self.read_fn, self.mate_fn]:
        u.clean_up_aux_files(fn)

  def runTest(self):
    self.__compare_step_by_step()

  def __compare_step_by_step(self):
    if not self.opt.no_index:
      u.build_index(self.refseq_fn)
    #--
    bwa_res = u.run_bwa_sampe(self.refseq_fn, self.read_fn, self.mate_fn)
    py_res =  u.run_bwa_py_sampe(self.refseq_fn, self.read_fn, self.mate_fn,
                                 log_level=self.opt.log_level,
                                 pairing_batch_size=self.opt.pairing_batch_size,
                                 seq_list_len=self.opt.seq_list_len,
                                 fastq_subfmt=self.opt.fastq_subfmt)
    f = open("errors.dump", 'w')
    error_count = 0
    try:
      self.assertEqual(len(bwa_res), len(py_res))
    except AssertionError:
      error_count += 1
      f.write("Error on result n. %d\n" % i)
      f.write("NUMBER OF RESULT FIELDS: %d, %d\n" % (
        len(bwa_res), len(py_res)))
    for i, (o, n) in enumerate(it.izip(bwa_res, py_res)):
      try:
        for k in o.keys():
          if k == 'name':
            nn = n[k].rsplit("/", 1)[0] # we are dropping the '/[12]' extension
            self.assertEqual(o[k], nn)
          elif k == 'aux':
            self.assertEqual(len(o[k]), len(n[k]))
            for x,y in it.izip(o[k], n[k]):
              self.assertEqual(x, y)
          else:
            self.assertEqual(o[k],n[k])
      except AssertionError:
        error_count += 1
        f.write("Error on result n. %d\n" % i)
        for k in o.keys():
          f.write("%r %r %r %r\n" % (k, o[k], n[k], o[k]==n[k]))
        f.write("\n")
    print "error count: %d" % error_count
    f.close()


def suite():
  suite = unittest.TestSuite()
  suite.addTest(bwa_big_tc('runTest'))
  return suite


def make_parser():
    parser = optparse.OptionParser('%prog [OPTIONS] REFSEQ_FN READ_FN MATE_FN')
    parser.add_option(
      "-f", type="str", dest="fastq_subfmt", metavar="STRING",
      default="fastq-illumina", help="fastq subformat (%default)",
      )
    parser.add_option(
      "-l", type=int, dest="seq_list_len", metavar="INT",
      default=10000, help="read this number of seqs at a time (%default)",
      )
    parser.add_option(
      "-p", type=int, dest="pairing_batch_size", metavar="INT", default=None,
      help="seq batch size for paired_sw (None, i.e., do not split in batches)",
      )
    parser.add_option(
      "--no-index", action="store_true", help="do not build build bwa index",
      )
    parser.add_option(
      "--no-clean", action="store_true", help="do not clean up aux files",
      )
    parser.add_option(
      "--log-level", type="choice", dest="log_level", metavar="STRING",
      default="INFO", help="logging level (%default)",
      choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
      )
    return parser


if __name__ == '__main__':

  parser = make_parser()
  opt, args = parser.parse_args(sys.argv)
  opt.log_level = getattr(logging, opt.log_level)
  if len(args) < 4:
    parser.print_help()
    sys.exit(2)
  bwa_big_tc.set_params(args[1:4], opt)

  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
