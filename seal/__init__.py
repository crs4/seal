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

import os

logformat = '%(asctime)s\t%(levelname)s\t[ %(name)s ]\t%(message)s'

try:
  from seal.version import version as __version__
except ImportError:
  pass

def jar_path():
    return os.path.abspath( os.path.join( os.path.dirname(__file__), 'seal.jar'))
