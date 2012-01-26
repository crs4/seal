#!/usr/bin/env python

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


import sys, os, unittest, imp

D = os.path.dirname(__file__)
BuildDir = os.path.realpath(os.path.join(D, "..", "build"))
sys.path.insert(0, BuildDir)

try:
	import bl.lib.tools.standard_monitor
except ImportError:
	print >>sys.stderr, "Can't import bl module.  Did you build Seal? (call 'make' in the Seal root directory). If you installed Seal somewhere, please export PYTHONPATH to include the installation path."
	sys.exit(1)


TEST_MODULES = [os.path.join(D, m) for m in [

  "bl/lib/seq/aligner/bwa/test_bwa_mapping.py",
  "bl/lib/seq/aligner/bwa/test_core.py",
  "bl/lib/seq/aligner/bwa/test_bwa_aligner.py",  # currently broken on some 32-bit systems, see #62
  "bl/lib/seq/aligner/io/test_protobuf_mapping.py",
  "bl/lib/seq/aligner/io/test_sam_formatter.py",
  "bl/lib/seq/aligner/test_mapping.py",
  "bl/lib/tools/test_seal_config_file.py",
  "bl/mr/lib/test_emit_sam_link.py",
  "bl/mr/lib/test_filter_link.py",
  "bl/mr/lib/test_hadoop_event_monitor.py",
  "bl/mr/lib/test_hit_processor_chain_link.py",
  "bl/mr/seq/seqal/test_mark_duplicates_emitter.py",
  "bl/mr/seq/seqal/test_reducer.py",

## add new unit test modules here.  They must provide a suite() method
## that returns a unittest.TestSuite instance.  Paths are relative to
## this module's directory

  ]]


class UnitTestRunner(object):

  def __init__(self):
    self.autotest_list = TEST_MODULES

  @staticmethod
  def __load_suite(module_path):
    module_name = os.path.splitext(os.path.basename(module_path))[0]
    ## so that test modules can import other modules in their own
    ## directories, we directly modify sys.path
    sys.path.append(os.path.dirname(module_path))
    fp, pathname, description = imp.find_module(module_name)
    try:
      module = imp.load_module(module_name, fp, pathname, description)
      del sys.path[-1]  # clean up to avoid conflicts
      return module.suite()
    finally:
      fp.close()

  def run(self):
    suites = map(UnitTestRunner.__load_suite, self.autotest_list)
    unittest.TextTestRunner(verbosity=2).run(unittest.TestSuite(tuple(suites)))


if __name__ == '__main__':
  UnitTestRunner().run()
