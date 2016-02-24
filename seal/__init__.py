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

import fnmatch
from glob import glob, iglob
import logging
import os

logformat = '%(asctime)s\t%(levelname)s\t[ %(name)s ]\t%(message)s'

try:
  from seal.version import version as __version__
except ImportError:
  pass

def seal_dir():
    return os.path.dirname(os.path.abspath(__file__))

def jar_dir():
    return os.path.join(seal_dir(), 'jars')

def seal_jar_path():
    jars = glob(os.path.join(jar_dir(), 'it.crs4.seal*.jar') )
    if len(jars) == 0:
        raise RuntimeError("Couldn't find seal jar in " + jar_dir())
    elif len(jars) > 1:
        raise RuntimeError("Found more than one seal jars in %s! (%s)" % (
            jar_dir(),
            ','.join((os.path.basename(j) for j in jars))))

    return jars[0]

def dependency_jars():
    """
    Returns all jars in jar_dir() except those whose name matches seal*jar.
    """
    return [ j
        for j in iglob( os.path.join(seal_dir(), 'jars', '*.jar'))
        if not fnmatch.fnmatch(os.path.basename(j), "seal*.jar") ]

def libjars(lang='python'):
    """
    Returns a tuple with appropriate arguments to run the Seal apps in Hadoop.
    The tuple specifies the -libjars option and sets the property
    -Dmapreduce.user.classpath.first=true.

    :param: lang: 'python' or 'java' (default: 'python')
    """
    if lang not in ('python', 'java'):
        raise ValueError("Invalid lang argument.  Value must be 'python' or 'java'")
    args = ['--libjars' if lang == 'python' else '-libjars']
    args.extend( (','.join(dependency_jars()), '-Dmapreduce.user.classpath.first=true') )
    return tuple(args)

def avro_schema_dir():
    return os.path.join(seal_dir(), 'lib', 'io')

def config_logging(level='INFO', logfile=None):
    if logfile:
        logging.basicConfig(format=logformat, level=level, filename=logfile)
    else:
        logging.basicConfig(format=logformat, level=level)
