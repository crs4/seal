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
from distutils.core import setup
from distutils.errors import DistutilsSetupError
from distutils.core import Command as du_command
from distutils.command.build import build as du_build
from distutils.command.clean import clean as du_clean
from distutils.command.sdist import sdist as du_sdist
from distutils.command.bdist import bdist as du_bdist
from distutils import log as distlog

import xml.etree.ElementTree as ET

VERSION_FILENAME = 'VERSION'
TAG_VERS_SEP_STR = '--'

# used to pass a version string from the command line into the setup functions
VERSION_OVERRIDE = None

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
  ## Add a --hadoop-bam option to our build command.
  # The only way I've found to make this work is to
  # directly add the option definition to the user_options
  # array in du_build.
  #
  # call setup.py as
  #  python setup.py build --hadoop-bam=/my/path/to/hadoop-bam
  # to make it work.
  du_build.user_options.append( ('hadoop-bam=', None, "Path to directory containing Hadoop-BAM jars" ) )

  def initialize_options(self):
    du_build.initialize_options(self)
    self.hadoop_bam = None

  def finalize_options(self):
    du_build.finalize_options(self)
    # BUG:  get_arg("override_version_check") doesn't work here
    # since the argument has already been eaten by check_python_version(),
    # called before setup
    self.override_version_check = get_arg("override_version_check") or 'false'
    # look for HadoopBam, if it hasn't been provided as a command line argument
    if self.hadoop_bam is None:
      # check environment variable HADOOP_BAM
      self.hadoop_bam = os.environ.get("HADOOP_BAM", seal_build_hadoop_bam.hadoop_bam_autobuild_dir)
      if self.hadoop_bam is None or not os.path.exists(self.hadoop_bam):
          self.hadoop_bam = None # in case it's set but it doesn't exist
      # if self.hadoop_bam is None assume they'll be somewhere in the class path (e.g. Hadoop directory)
    if self.hadoop_bam:
      print >>sys.stderr, "Using hadoop-bam in", self.hadoop_bam
    else:
      print >>sys.stderr, "Hadoop-BAM path not specified.  Trying to build anyways."
      print >>sys.stderr, "You can specify a path with:"
      print >>sys.stderr, "python setup.py build --hadoop-bam=/my/path/to/hadoop-bam"
      print >>sys.stderr, "or by setting the HADOOP_BAM environment variable."

  def run(self):
    self.version = set_version()
    self.distribution.metadata.version = self.version
    # Create (or overwrite) seal/version.py
    with open(os.path.join('seal', 'version.py'), 'w') as f:
      f.write('version = "%s"\n' % self.version)

    # run the usual build
    du_build.run(self)

    # make bwa
    libbwa_dir = "seal/lib/aligner/bwa"
    libbwa_src = os.path.join(libbwa_dir, "libbwa")
    libbwa_dest = os.path.abspath(os.path.join(self.build_purelib, libbwa_dir))
    make_cmd = "BWA_LIBRARY_DIR=%s make -C %s libbwa" % (libbwa_dest, libbwa_src)
    ret = os.system(make_cmd)
    if ret:
      raise DistutilsSetupError("could not make libbwa")

    # protobuf classes
    proto_src = "seal/lib/io/mapping.proto"
    ret = os.system("protoc %s --python_out=%s" %
                    (proto_src, self.build_purelib))
    if ret:
      raise DistutilsSetupError("could not run protoc")

    # Java stuff
    ant_cmd = 'ant -Dversion="%s" -Doverride_version_check="%s"' % (self.version, self.override_version_check)
    if self.hadoop_bam:
      ant_cmd += ' -Dhadoop.bam="%s"' % self.hadoop_bam
    ret = os.system(ant_cmd)
    if ret:
      raise DistutilsSetupError("Could not build Java components")
    # finally make the jar
    self.package()

  def package(self):
    dest_jar_path = os.path.join(self.build_purelib,'seal', 'seal.jar')
    if os.path.exists(dest_jar_path):
      os.remove(dest_jar_path)
    shutil.move( os.path.join(self.build_base, 'seal.jar'), dest_jar_path )

# Custom clean action that removes files generated by the build process.
class seal_clean(du_clean):
  def run(self):
    du_clean.run(self)
    os.system("make -C docs clean")
    os.system("ant clean")
    os.system("rm -f seal/lib/aligner/bwa/libbwa/bwa")
    os.system("rm -f seal/version.py")
    os.system("rm -rf dist MANIFEST")

    os.system("find seal -name '*.pyc' -print0 | xargs -0  rm -f")
    os.system("find seal/lib/aligner/bwa/libbwa/ \( -name '*.ol' -o -name '*.o' -o -name '*.so' \) -print0 | xargs -0  rm -f")
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

        os.chdir(os.path.join(seal_source_path, 'tests'))
        new_env = copy(os.environ)
        if not new_env.has_key('PYTHONPATH'):
            # I think it's handy to add our build directory to the PYTHONPATH.
            # TODO:  Too bad I can't figure out the "correct" way to instantiate a build command
            # and read its build_purelib attribute.
            build_dir = os.path.join(seal_source_path, 'build')
            lib_dir = glob.glob( os.path.join(build_dir, 'lib*'))
            if len(lib_dir) == 0:
                pass # do nothing.  Maybe it's installed elsewhere
            else:
                if len(lib_dir) > 1:
                    print >> sys.stderr, "Found more than one lib* directory under build directory", build_dir
                    print >> sys.stderr, "Using the first one"
                lib_dir = lib_dir[0]
                new_env['PYTHONPATH'] = os.pathsep.join( (lib_dir, new_env.get('PYTHONPATH', '')) )
        cmd = ['python', 'run_py_unit_tests.py']
        subprocess.check_call(cmd, env=new_env)

        os.chdir(seal_source_path) # must change back so that ant can find build.xml
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
        os.chdir(seal_source_path)
        subprocess.check_call(['ant', 'run_integration_tests'])

class seal_build_hadoop_bam(du_command):
    description = """Fetch (from github) and build hadoop-bam. Requires git, hadoop, java, and mvn in the PATH"""

    # This command automates fetching and building a version of
    # Hadoop-BAM compatible with this version of Seal. It's not guaranteed to
    # work with all versions of Hadoop, but should work most of the time.

    user_options = []

    hadoop_bam_autobuild_dir = os.path.join(os.path.abspath(os.path.dirname(__file__)), "hadoop_bam_autodir")

    def initialize_options(self):
        """Use this to set option defaults before parsing."""
        self.hadoop_bam_url = "https://github.com/HadoopGenomics/Hadoop-BAM.git"
        self.hadoop_bam_version = "7.0.0"

    def finalize_options(self):
        """Code to validate/modify command-line/config input goes here."""
        pass

    @staticmethod
    def _get_hadoop_version_str():
        import pydoop
        h = pydoop.hadoop_version_info()
        return '.'.join(map(str, h.main))

    @staticmethod
    def _get_java_version():
        output = subprocess.check_output("java -version", shell=True, stderr=subprocess.STDOUT)
        m = re.search(r'^java version "(\d+\.\d+)\..*', output)
        if m:
            return m.group(1)
        else:
            raise RuntimeError("Couln't determine java version from 'java -version'. Here's its output:\n%s" % output)

    @staticmethod
    def _edit_pom(pom_path, java_version, hadoop_version):
        """
        Edits the pom file in place, inserting the Java and Hadoop versions
        that we're using to build our software.
        """
        # Handling the XML namespace gave me some grief. If you simply load the
        # XML, edit and then write ElementTree inserts a new namespace alias in the
        # pom.xml file (e.g., ns0:project). Although formally correct, mvn barfs; I get
        # the feeling that it doesn't understand namespaces.  The way I found that no
        # namespace alias is inserted by ElementTree when writing the XML is to use an
        # empty prefix (see 'ns = {...}' below). However, this causes some grief when
        # writing the element queries (root.find etc.) since, not having an alias, ElementTree
        # expects the tag names to be prefixed with the entire namespace name in Clark's
        # notation -- hence the use tag_prefix + tag name when forming the queries.
        ns = { '': 'http://maven.apache.org/POM/4.0.0' }
        tag_prefix = '{%s}' % ns['']
        ET.register_namespace('', ns[''])
        pom_tree = ET.parse(pom_path)
        root = pom_tree.getroot()
        props = root.find(tag_prefix + 'properties', namespaces=ns)

        java_version_elem = props.find(tag_prefix + 'java.version', ns)
        if java_version_elem is None:
            raise RuntimeError("%s is missing <java.version> property" % pom_path)
        java_version_elem.text = java_version

        hadoop_version_elem = props.find(tag_prefix + 'hadoop.version', ns)
        if hadoop_version_elem is None:
            raise RuntimeError("%s is missing <hadoop.version> property" % pom_path)
        hadoop_version_elem.text = hadoop_version
        pom_tree.write(pom_path)


    def run(self):
        hadoop_version = self._get_hadoop_version_str()
        java_version = self._get_java_version()

        shutil.rmtree(self.hadoop_bam_autobuild_dir, ignore_errors=True)
        distlog.info("cloning hadoop-bam from %s", self.hadoop_bam_url)
        subprocess.check_call("git clone %s %s" % (self.hadoop_bam_url, self.hadoop_bam_autobuild_dir), shell=True)
        os.chdir(self.hadoop_bam_autobuild_dir)
        try:
            distlog.info("Checking out version %s", self.hadoop_bam_version)
            subprocess.check_call("git checkout %s" % self.hadoop_bam_version, shell=True)
            distlog.info("Editing pom.xml. Setting java version to %s and hadoop version to %s", java_version, hadoop_version)
            self._edit_pom('pom.xml', java_version, hadoop_version)
            distlog.info("Building...")
            subprocess.check_call("mvn clean package -DskipTests", shell=True)
        finally:
            os.chdir('..')


#############################################################################
# main
#############################################################################

if __name__ == '__main__':
    # chdir to Seal's root directory (where this file is located)
    os.chdir(os.path.abspath(os.path.dirname(__file__)))

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
                    'seal.lib.aligner.bwa',
                    'seal.lib.io',
                    'seal.lib.mr',
                    'seal.seqal',
                    ],
          cmdclass={
              "bdist": seal_bdist,
              "build": seal_build,
              "clean": seal_clean,
              "build_docs": seal_build_docs,
              "build_hadoop_bam": seal_build_hadoop_bam,
              "run_unit_tests": seal_run_unit_tests,
              "run_integration_tests": seal_run_integration_tests,
              "sdist": seal_sdist,
              },
          scripts=glob.glob("scripts/*"),
          )
