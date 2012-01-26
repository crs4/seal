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

# These are import path manipulations used by the bin scripts.

import os
import sys

# find the absolute path to the Seal directory
SealDir = os.path.realpath( os.path.join( os.path.dirname( os.path.realpath(__file__) ), "..") )

# now we set up sys.path and PYTHONPATH to include first the build directory, if it
# exists (we could be running from within the development copy), and second the SealDir
# (for a real installation).

# prepend SealDir to sys.path and PYTHONPATH
sys.path.insert(0, SealDir)
os.environ['PYTHONPATH'] = ":".join( filter(lambda x:x, [SealDir, os.environ.get('PYTHONPATH', None)]) )
BuildDir = os.path.join(SealDir, "build")
if os.path.exists(BuildDir):
	# since it exists, prepend BuildDir to sys.path and PYTHONPATH
	sys.path.insert(0,  BuildDir) # This is the build dir.
	os.environ['PYTHONPATH'] = ":".join([BuildDir, os.environ['PYTHONPATH'] ])
