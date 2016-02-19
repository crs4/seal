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

"""Seal: processing sequencing data on Hadoop.

Seal is a toolkit of Hadoop-based applications to process large quantities of
sequencing data.
"""

from copy import copy
import glob
import os
import re
import shutil
import subprocess
import sys

from contextlib import contextmanager

from distutils.core import setup
from distutils.errors import DistutilsSetupError
from distutils.core import Command as du_command
from distutils.command.build import build as du_build
from distutils.command.clean import clean as du_clean
from distutils.command.sdist import sdist as du_sdist
from distutils.command.bdist import bdist as du_bdist
from distutils import log as distlog

VERSION_FILENAME = 'VERSION'
TAG_VERS_SEP_STR = '--'

# used to pass a version string from the command line into the setup functions
VERSION_OVERRIDE = None

ProjectRoot = os.path.abspath(os.path.dirname(__file__))

# We need backported code if running on Python version < 2.7
if sys.version_info < (2, 7):
    # make sure the seal package is in the import path
    sys.path.append(
        os.path.join(
            os.path.abspath(os.path.dirname(__file__)),
            'seal'))
    import seal.backports

def get_arg(name):
    arg_start = "%s=" % name
    for i in xrange(len(sys.argv)):
        arg = sys.argv[i]
        if arg.startswith(arg_start):
            value = arg.replace(arg_start, "", 1)
            del sys.argv[i]
            if not value:
                raise RuntimeError("blank value specified for %s" % name)
            return value
    return None

def check_python_version():
    override = ("true" == get_arg("override_version_check"))
    if not override and sys.version_info < (2, 6):
        print >> sys.stderr, "Please use a version of Python >= 2.6 (currently using vers. %s)." % ",".join( map(str, sys.version_info))
        print >> sys.stderr, "Specify setup.py override_version_check=true to override this check."
        sys.exit(1)

@contextmanager
def chdir(new_dir):
    current_dir = os.getcwd()
    try:
        os.chdir(new_dir)
        yield
    finally:
        os.chdir(current_dir)

def is_modified():
    output = subprocess.check_output(
            "git status --porcelain 2>/dev/null", shell=True).rstrip().split('\n')
    any_modified = any(line for line in output if not line.startswith('??'))
    return any_modified

def version_from_git():
    try:
        commit_id = subprocess.check_output(
                "git rev-list --max-count=1 --abbrev-commit HEAD 2>/dev/null", shell=True).rstrip()
        if not re.match(r'[a-f0-9]{7}', commit_id):
            raise ValueError("Invalid git commit id '%s'" % commit_id)
        try:
            tag = subprocess.check_output(
                    ["git", "describe", "--tags", "--exact-match", commit_id],
                    stderr=open(os.devnull, 'w')
                  ).rstrip()
        except subprocess.CalledProcessError:
            # No tag found
            tag = ""
        if tag:
            vers = tag
        else:
            vers = "devel-" + commit_id
        if is_modified():
            vers += '-MODIFIED'
        print >> sys.stderr, "Version from git revision '%s'" % vers
        return vers
    except (StandardError, subprocess.CalledProcessError):
        print >> sys.stderr, "Unable to set version from git"
        return None

def set_version():
    """
    Build a version string and write it to the
    VERSION file and return it.

    If a git tag is present on this revision, it will be used as the version. Else
    the version string will look like devel--<commit id>.

    The return value can be overridden by giving a "version" argument to the script.
    """
    version_filename = os.path.join(os.path.dirname(__file__), VERSION_FILENAME)
    if VERSION_OVERRIDE:
        vers = VERSION_OVERRIDE
        print >> sys.stderr, "Version manually overridden and set to '%s'" % vers
    else:
        # if no version specified on command line
        # Try setting the version from git. Maybe we're in a repo
        vers = version_from_git()
        if not vers:
            # See if we have a VERSION file, maybe created by sdist, and use that
            if os.path.exists(version_filename):
                with open(version_filename) as f:
                    vers = f.read().rstrip()
                if vers:
                    return vers # return directly, without re-writing the VERSION file
            # only as a last resort, set a version string based on the build time
            from datetime import datetime
            vers = datetime.now().strftime("devel" + TAG_VERS_SEP_STR + "%Y%m%d")
            print >> sys.stderr, "Version set from build time '%s'" % vers
    # if we've generated a version string in this function we write to a VERSION file
    with open(version_filename, 'w') as f:
        print >> sys.stderr, "Writing version %s to %s" % (vers, version_filename)
        f.write(vers + '\n')
    return vers


class seal_sdist(du_sdist):
  def run(self):
      self.distribution.metadata.version = set_version()
      du_sdist.run(self)

class seal_bdist(du_bdist):
  def run(self):
      self.distribution.metadata.version = set_version()
      du_bdist.run(self)

class seal_build(du_build):

  @staticmethod
  def _get_hadoop_version():
    import pydoop
    return pydoop.hadoop_version()

  def finalize_options(self):
    du_build.finalize_options(self)
    # BUG:  get_arg("override_version_check") doesn't work here
    # since the argument has already been eaten by check_python_version(),
    # called before setup
    self.override_version_check = get_arg("override_version_check") or 'false'

  def run(self):
    self.version = set_version()
    self.distribution.metadata.version = self.version

    # Create (or overwrite) seal/version.py
    with open(os.path.join('seal', 'version.py'), 'w') as f:
      f.write('version = "%s"\n' % self.version)

    # run the usual build
    du_build.run(self)

    ## protobuf classes.  We're not using these at the moment
    #proto_src = "seal/lib/io/mapping.proto"
    #ret = os.system("protoc %s --python_out=%s" %
    #                (proto_src, self.build_purelib))
    #if ret:
    #  raise DistutilsSetupError("could not run protoc")

    # Java stuff
    sbt_cmd = "sbt -Dhadoop.version={} universal:stage".format(self._get_hadoop_version())
    distlog.info("Running sbt inside ./sbt directory")
    distlog.info(sbt_cmd)
    with chdir('sbt'):
      ret = os.system(sbt_cmd)
    if ret:
      raise DistutilsSetupError("Could not build Java components")
    # finally make the jar
    self.package()

  def package(self):
    # destination dir
    jar_dir = os.path.join(self.build_purelib, 'seal', 'jars')
    if not os.path.isdir(jar_dir):
      os.makedirs(jar_dir)
    sbt_target_dir = os.path.join(ProjectRoot, 'sbt', 'target', 'universal', 'stage', 'lib')
    for jar in glob.iglob(os.path.join(sbt_target_dir, '*.jar')):
      dest_jar_path = os.path.join(jar_dir, os.path.basename(jar))
      if os.path.exists(dest_jar_path):
        os.remove(dest_jar_path)
      distlog.info("Moving %s jar to %s", jar, dest_jar_path)
      shutil.move(jar, dest_jar_path)

# Custom clean action that removes files generated by the build process.
class seal_clean(du_clean):
  def run(self):
    du_clean.run(self)
    os.system("make -C docs clean")
    os.system("cd sbt && sbt clean")
    os.system("rm -f seal/version.py")
    os.system("rm -rf dist MANIFEST")

    os.system("find seal -name '*.pyc' -print0 | xargs -0  rm -f")
    os.system("find . -name '*~' -print0 | xargs -0  rm -f")

class seal_build_docs(du_command):
    description = "Build the docs"
    user_options = []

    def initialize_options(self):
        """Use this to set option defaults before parsing."""
        pass

    def finalize_options(self):
        """Code to validate/modify command-line/config input goes here."""
        pass

    def run(self):
        seal_source_path = os.path.dirname(__file__)
        subprocess.check_call(
            ["make", "-C",
                os.path.join(seal_source_path, "docs"),
                "html"])

def get_build_dir():
    # TODO:  Too bad I can't figure out the "correct" way to instantiate a build command
    # and read its build_purelib attribute.
    root = os.path.abspath(os.path.dirname(__file__))
    build_dir = os.path.join(root, 'build')
    lib_dir = glob.glob( os.path.join(build_dir, 'lib*'))
    if len(lib_dir) == 0:
        return None
    else:
        if len(lib_dir) > 1:
            distlog.warn("Found more than one lib* directory under %s", build_dir)
        return lib_dir[0]

def compute_test_pypath():
    pypath = os.environ.get('PYTHONPATH', '')
    bdir = get_build_dir()
    if bdir:
        pypath = os.pathsep.join((bdir, pypath))
    return pypath

class seal_run_unit_tests(du_command):
    description = "Run unit tests.  You MUST build Seal first and set the PYTHONPATH appropriately"
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        seal_source_path = os.path.abspath(os.path.dirname(__file__))
        # change into the tests directory.  DON'T change into the Seal
        # "base" directory as we may end up importing modules from the
        # source directory.
        with chdir(os.path.join(seal_source_path, 'tests')):
            new_env = copy(os.environ)
            new_env['PYTHONPATH'] = compute_test_pypath()
            cmd = ['python', 'run_py_unit_tests.py']
            subprocess.check_call(cmd, env=new_env)

        cmd = ['ant', 'run-tests']
        subprocess.check_call(cmd)

class seal_run_integration_tests(du_command):
    description = "Run integration tests.  You MUST install Seal and have your Hadoop cluster configured and running"
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        seal_source_path = os.path.abspath(os.path.dirname(__file__))
        integration_tests = os.path.join(seal_source_path, 'tests', 'integration_tests')
        with chdir(integration_tests):
            new_env = copy(os.environ)
            new_env['PYTHONPATH'] = compute_test_pypath()
            subprocess.check_call('./run_all.sh', env=new_env)


#############################################################################
# main
#############################################################################

if __name__ == '__main__':
    # chdir to Seal's root directory (where this file is located)
    os.chdir(ProjectRoot)

    check_python_version()

    # if a 'version=xxx' argument is present, we'll fish it up here and remove it
    # from the command line args
    VERSION_OVERRIDE = get_arg('version')

    NAME = 'seal'
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
          packages=['seal',
                    'seal.lib',
                    'seal.lib.aligner',
                    'seal.lib.io',
                    'seal.lib.mr',
                    'seal.seqal',
                    ],
          package_data={ 'seal.lib.io': ['Fragment.avsc', 'AlignmentRecord.avsc'] },
          cmdclass={
              "bdist": seal_bdist,
              "build": seal_build,
              "clean": seal_clean,
              "build_docs": seal_build_docs,
              "run_unit_tests": seal_run_unit_tests,
              "run_integration_tests": seal_run_integration_tests,
              "sdist": seal_sdist,
              },
          scripts=glob.glob("scripts/*"),
          )
