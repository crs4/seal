# Copyright (C) 2011-2016 CRS4.
#
# This file is part of Hadut.
#
# Hadut is free software: you can redistribute it and/or modify it
# under the terms of the GNU General Public License as published by the Free
# Software Foundation, either version 3 of the License, or (at your option)
# any later version.
#
# Hadut is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
# or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
# for more details.
#
# You should have received a copy of the GNU General Public License along
# with Hadut.  If not, see <http://www.gnu.org/licenses/>.

############################################################################
#
# I wrote this script mainly because  hadoop distcp requires that
# you identify files by a proper URI, so files have to
# be identified by like file:///home/pireddu/directory/filename.
# This precludes the use of shell expansion, which is rather
# annoying.
#
# Author:  Luca Pireddu <pireddu@crs4.it>
############################################################################


import optparse
import os
import sys

import seal.lib.tools.hadut as hadut

class HelpFormatter(optparse.IndentedHelpFormatter):
  def format_description(self, description):
    return description + "\n" if description else ""

def make_parser():
    parser = optparse.OptionParser(
        usage="%prog [OPTIONS] INPUT OUTPUT",
        formatter=HelpFormatter()
        )
    parser.add_option("-m", "--parallel", type="int", metavar="INT",
        help="Number of parallel copy operations to execute")
    parser.add_option("-b", "--block-size", type="int", metavar="INT",
        help="Block size (MB)")
    return parser

def normalize_filenames(names):
    def norm(name):
        if not os.access(name, os.R_OK):
            raise ValueError("Can't read file " + name)
        return "".join( "file://" + os.path.realpath(name) )
    return map(norm, names)

def scan_args(argv):
    parser = make_parser()
    opt, args = parser.parse_args(argv)

    if opt.parallel and opt.parallel <= 0:
        parser.error("number of parallel copy operations must by > 0")
    if opt.block_size:
        if opt.block_size <= 0:
            parser.error("block size must be > 0")
        elif opt.block_size < 64:
            print >>sys.stderr, "WARNING:  a block size of %d is pretty small" % opt.block_size

    if len(args) < 2:
        parser.error("You need to provide at least one source file and a destination")
    opt.destination = args[-1]
    try:
        opt.files_to_copy = normalize_filenames(args[0:-1])
    except ValueError as e:
        print >>sys.stderr, e
        sys.exit(1)
    return opt

def run_distcp(opt):
    properties = { 'mapred.job.name': "distcp %s" % opt.destination }
    if opt.block_size:
        properties['dfs.block.size'] = opt.block_size * 2**20 # * 2**20 to convert to MB

    args = ["-m"]
    # For -m, if the user specified a value we use it.  Otherwise use 4 per node
    if opt.parallel:
        args.append(opt.parallel)
    else:
        args.append( 4*hadut.num_nodes() )

    print >>sys.stderr, "Copying %d files to %s" % (len(opt.files_to_copy), opt.destination)
    args.extend(opt.files_to_copy)
    args.append(opt.destination)

    try:
        hadut.run_hadoop_cmd_e("distcp", properties, args)
    except Exception as e:
        print >>sys.stderr, "Error running distcp: %s" % e
        sys.exit(1)
    return 0

def main(args=None):
    opt = scan_args(args)
    return run_distcp(opt)
