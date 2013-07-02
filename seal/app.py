# Copyright (C) 2011-2012 CRS4.
#
# This file is part of Seal.
#
# Seal is free software: you can redistribute it and/or modify it
# under the terms of the GNU General Public License as published by the Free
# Software Foundation, either version 3 of the License, or (at your option)
# any later version.
#
# Seal is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
# or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
# for more details.
#
# You should have received a copy of the GNU General Public License along
# with Seal.  If not, see <http://www.gnu.org/licenses/>.

"""
Seal command-line interface.
"""

import argparse
import importlib
import sys

import seal.lib.java_app_runner as runner

class JavaCall(object):
    def __init__(self, app_name, class_name):
        self.app_name = app_name
        self.class_name = class_name

    def __call__(self, args):
        print "sys.exit( runner.main(%s, %s, %s) )" % (self.class_name, self.app_name, args)
        sys.exit( runner.main(self.class_name, self.app_name, args) )

class PythonCall(object):
    def __init__(self, module_name):
        self.module_name = module_name

    def __call__(self, args):
        mod = importlib.import_module(self.module_name)
        sys.exit( mod.main(args) )


JavaApplications = {
    "demux":            JavaCall("demux", "it.crs4.seal.demux.Demux"),
    "merge_alignments": JavaCall("merge_alignments", "it.crs4.seal.read_sort.MergeAlignments"),
    "prq":              JavaCall("prq", "it.crs4.seal.prq.PairReadsQSeq"),
    "read_sort":        JavaCall("read_sort", "it.crs4.seal.read_sort.ReadSort"),
    "recab_table":      JavaCall("recab_table", "it.crs4.seal.recab.RecabTable"),
    "tsvsort":          JavaCall("tsvsort", "it.crs4.seal.tsv_sort.TsvSort"),
    "usort":            JavaCall("usort", "it.crs4.seal.usort.USort")
}

PythonApplications = {
    "bcl2qseq":          PythonCall('seal.dist_bcl2qseq'),
    "seqal":             PythonCall('seal.seqal.seqal_run'),
    "convert_bwa_index": PythonCall('seal.convert_bwa_index'),
    "distcp_files":      PythonCall('seal.distcp_files'),
    "fetch_recab_table": PythonCall('seal.fetch_recab_table'),
    "version":           PythonCall('seal.version_main')
}

def make_parser():
    parser = argparse.ArgumentParser(
        description="Seal tool suite for sequence processing on Hadoop",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
        )
    subparsers = parser.add_subparsers(help="sub-commands")
    # set add_help=False so that -h gets sent to underlying application
    for app_name in sorted(JavaApplications.keys()):
        sub = subparsers.add_parser(app_name, description=app_name, add_help=False)
        sub.set_defaults(app_name=app_name, run_func = JavaApplications[app_name])

    for app_name in sorted(PythonApplications.keys()):
        sub = subparsers.add_parser(app_name, description=app_name, add_help=False)
        sub.set_defaults(app_name=app_name, run_func = PythonApplications[app_name])
    return parser

def main(argv=None):
    p = make_parser()
    if len(argv) <= 1:
      p.error("You must specify a subcommand")
    args, left_over = p.parse_known_args(argv[1:])
    args.run_func(left_over)
    return 0

#vim: expandtab tabstop=4 shiftwidth=4 autoindent
