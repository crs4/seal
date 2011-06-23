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

"""Seal: Sequence Alignment on Hadoop.

Seal is a of MapReduce application for biological
sequence alignment. It runs on Hadoop (http://hadoop.apache.org) 
through Pydoop (http://pydoop.sourceforge.net), a Python MapReduce 
and HDFS API for Hadoop.
"""

import os
import sys
from distutils.core import setup
from distutils.errors import DistutilsSetupError


def get_version():
	for i in xrange(len(sys.argv)):
		arg = sys.argv[i]
		if arg.startswith("version="):
			vers = arg.replace("version=", "", 1)
			if not vers:
				raise RuntimeException("blank version specified")
			del sys.argv[i]
			return vers
	# else, if no version specified on command line
	from datetime import datetime
	# rudimentary way to detect the utc-offset.  This will fail
	# if the hour changes right between the call to now() and utcnow()
	tz = datetime.now().hour - datetime.utcnow().hour
	vers = "devel - %s %+03i00" % (datetime.now().strftime("%Y/%m/%d %H:%M:%S"), tz)
	return vers

NAME = 'seqal'
DESCRIPTION = __doc__.split("\n", 1)[0]
LONG_DESCRIPTION = __doc__
URL = "http://www.crs4.it"
# DOWNLOAD_URL = ""
LICENSE = 'GPL'
CLASSIFIERS = [
  "Programming Language :: Python",
  "License :: OSI Approved :: GNU General Public License (GPL)",
  "Operating System :: POSIX :: Linux",
  "Topic :: Scientific/Engineering :: Bio-Informatics",
  "Intended Audience :: Science/Research",
  ]
PLATFORMS = ["Linux"]
VERSION = get_version()
AUTHOR_INFO = [
  ("Luca Pireddu", "luca.pireddu@crs4.it"),
  ("Simone Leo", "simone.leo@crs4.it"),
  ("Gianluigi Zanetti", "gianluigi.zanetti@crs4.it"),
  ]
MAINTAINER_INFO = [
  ("Luca Pireddu", "luca.pireddu@crs4.it"),
  ]
AUTHOR = ", ".join(t[0] for t in AUTHOR_INFO)
AUTHOR_EMAIL = ", ".join("<%s>" % t[1] for t in AUTHOR_INFO)
MAINTAINER = ", ".join(t[0] for t in MAINTAINER_INFO)
MAINTAINER_EMAIL = ", ".join("<%s>" % t[1] for t in MAINTAINER_INFO)


#-- FIXME: handle internally ------------------------------------------------
from distutils.command.build import build

class seqal_build(build):
  def run(self):
    build.run(self)
    libbwa_dir = "bl/lib/seq/aligner/bwa"
    libbwa_src = os.path.join(libbwa_dir, "libbwa")
    libbwa_dest = os.path.abspath(os.path.join(self.build_purelib, libbwa_dir))
    ret = os.system("BWA_LIBRARY_DIR=%s make -C %s libbwa" %
                    (libbwa_dest, libbwa_src))
    if ret:
      raise DistutilsSetupError("could not make libbwa")
    # protobuf classes
    proto_src = "bl/lib/seq/aligner/io/mapping.proto"
    ret = os.system("protoc %s --python_out=%s" %
                    (proto_src, self.build_purelib))
    if ret:
      raise DistutilsSetupError("could not run protoc")
#----------------------------------------------------------------------------


#write_version()

setup(name=NAME,
      description=DESCRIPTION,
      long_description=LONG_DESCRIPTION,
      url=URL,
##      download_url=DOWNLOAD_URL,
      license=LICENSE,
      classifiers=CLASSIFIERS,
      author=AUTHOR,
      author_email=AUTHOR_EMAIL,
      maintainer=MAINTAINER,
      maintainer_email=MAINTAINER_EMAIL,
      platforms=PLATFORMS,
      version=VERSION,
      packages=['bl',
                'bl.lib',
                'bl.lib.tools',
                'bl.lib.seq',
                'bl.lib.seq.aligner',
                'bl.lib.seq.aligner.bwa',
                'bl.lib.seq.aligner.io',
                'bl.mr',
                'bl.mr.lib',
                'bl.mr.seq',
                'bl.mr.seq.seqal',
                ],
      cmdclass={"build": seqal_build},
      )
