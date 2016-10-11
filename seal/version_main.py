#!/usr/bin/env python

# Copyright (C) 2012-2016 CRS4.
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

import sys

def main(args=None):
    try:
        import seal.version as version
    except ImportError as e:
        print >> sys.stderr, e
        print >> sys.stderr, "Version number not set!"
        sys.exit(1)

    print version.version
    return 0
