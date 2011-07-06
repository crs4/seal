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

# These are import path manipulations used by the bin scripts.

import os
import sys
SealDir = os.path.realpath( os.path.join( os.path.dirname( os.path.realpath(__file__) ), "..") )
sys.path.insert(0, SealDir)
BuildDir = os.path.join(SealDir, "build")
if os.path.exists(BuildDir):
	sys.path.insert(0,  BuildDir) # This is the build dir.
