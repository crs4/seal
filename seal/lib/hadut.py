
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

import pydoop

import copy
import os
import sys
import subprocess

#def run_hadoop_cmd_e(cmd, properties=None, args_list=[]):
#	retcode = run_hadoop_cmd(cmd, properties, args_list)
#	if retcode != 0:
#		raise RuntimeError("Error running Hadoop command")

#def run_hadoop_cmd(cmd, properties=None, args_list=[]):
#	args = [pydoop.hadoop_exec(), cmd]
#	if properties:
#		args += __construct_property_args(properties)
#	args += map(str, args_list) # only string arguments are allowed
#	return subprocess.call(args)

def __construct_property_args(prop_dict):
	return sum(map(lambda pair: ["-D", "%s=%s" % pair], prop_dict.iteritems()), []) # sum flattens the list

#def run_class_e(class_name, additional_cp=None, properties=None, args_list=[]):
#	retcode = run_class(class_name, additional_cp, properties, args_list)
#	if retcode != 0:
#		raise RuntimeError("Error running Hadoop class")

def run_pipes(executable, input_path, output_path, properties=None, args_list=[]):
	args = [pydoop.hadoop_exec(), "pipes"]
	properties = properties.copy() if properties else {}
	properties['hadoop.pipes.executable'] = executable

	args.extend( __construct_property_args(properties) )
	args.extend(["-input", input_path, "-output", output_path])
	args.extend(args_list)
	return subprocess.call(args)


#def run_class(class_name, additional_cp=None, properties=None, args_list=[]):
#	"""
#	Run a class that needs the Hadoop jars in its class path
#	"""
#	args = [pydoop.hadoop_exec(), class_name]
#	if additional_cp:
#		env = copy.copy(os.environ)
#		if type(additional_cp) == str: # wrap a single class path in a list
#			additional_cp = [additional_cp]
#		# Pass this classpath string to hadoop through the HADOOP_CLASSPATH
#		# environment variable.  If HADOOP_CLASSPATH is already defined, we'll
#		# append our values to it.
#		if env.has_key('HADOOP_CLASSPATH'):
#			additional_cp.insert(0, env['HADOOP_CLASSPATH'])
#		env['HADOOP_CLASSPATH'] = ":".join(additional_cp)
#	else:
#		env = os.environ
#	if properties:
#		args.extend( __construct_property_args(properties) )
#	args.extend(args_list)
#	return subprocess.call(args, env=env)
#
#def find_jar(jar_name, root_path=None):
#	root = root_path or os.getcwd()
#	paths = (root, os.path.join(root, "build"), "/usr/share/java")
#	for path in [ os.path.join(path, jar_name) for path in paths ]:
#		if os.path.exists(path):
#			return path
#	return None

def run_hadoop_jar(jar, class_name=None, additional_cp=None, properties=None, args_list=[]):
	"""
	Run a jar with "hadoop jar", optionally specifying the main class.
	"""
	if not os.path.exists(jar) or not os.access(jar, os.R_OK):
		raise ValueError("Can't read jar file %s" % jar)
	args = [pydoop.hadoop_exec(), 'jar', jar]
	if class_name:
		args.append(class_name)
	if additional_cp:
		env = copy.copy(os.environ)
		if type(additional_cp) == str: # wrap a single class path in a list
			additional_cp = [additional_cp]
		# Pass this classpath string to hadoop through the HADOOP_CLASSPATH
		# environment variable.  If HADOOP_CLASSPATH is already defined, we'll
		# append our values to it.
		if env.has_key('HADOOP_CLASSPATH'):
			additional_cp.insert(0, env['HADOOP_CLASSPATH'])
		env['HADOOP_CLASSPATH'] = ":".join(additional_cp)
	else:
		env = os.environ
	if properties:
		args.extend( __construct_property_args(properties) )
	args.extend(args_list)
	return subprocess.call(args, env=env)
