# Copyright (C) 2011 CRS4.
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


import argparse
import os
import sys

from bl.lib.tools.seal_config_file import SealConfigFile

class SeqalConfig(object):
	"""
	Reads the command line and the Seal config file.
	Merges options from both sources, giving priority to the command line.
	"""

	ConfigSection = "Seqal"

	class Args(argparse.Namespace):
		def __init__(self):
			self.properties = {}

	class SetProperty(argparse.Action):
		"""
		Used with argparse to parse arguments setting property values.
		Creates an attribute 'property' in the results namespace containing
		all the property-value pairs read from the command line.
		"""
		def __call__(self, parser, namespace, value, option_string=None):
			name, v = value.split('=', 1)
			namespace.properties[name] = v

	def __init__(self):
		self.cmd_parser = argparse.ArgumentParser(description='Distributed BWA read alignment and duplicates removal.')

		self.cmd_parser.add_argument('input', metavar='INPUT', help='input path')
		self.cmd_parser.add_argument('output', metavar='OUTPUT', help='output path')
		self.cmd_parser.add_argument('reference', metavar='REF.tar', help='reference archive (tar or tar.gz)')
		self.cmd_parser.add_argument('-q', '--trimq', metavar='Q', type=int, dest="trimq", 
				help="trim quality, like BWA's -q argument (default: 0).", 
				default=0)
		self.cmd_parser.add_argument('-r', '--num-reducers', metavar='INT', type=int, dest="num_reducers", 
				help="Number of reduce tasks. Specify 0 to perform alignment without duplicates removal (default: 3 * num task trackers).")
		self.cmd_parser.add_argument('-sc', '--seal-config', metavar='FILE', dest="seal_config", default=os.path.join(os.path.expanduser('~'), '.sealrc'),
				help='Override the default Seal config file')
		self.cmd_parser.add_argument('-D', metavar="PROP=VALUE", action=type(self).SetProperty, 
				help='Set a property value, such as -D mapred.compress.map.output=true')

	def load_config_and_cmd_line(self, argv=sys.argv[1:]):
		# we scan the command line first in case the user wants to 
		# override the default config file location
		args, left_over = self.cmd_parser.parse_known_args(args=argv, namespace=SeqalConfig.Args())

		# load the config for this program, if the file exists
		config = SealConfigFile()

		# was a config file different from the default specified on the command line?
		if args.seal_config != self.cmd_parser.get_default("seal_config"):
			# in this case, make sure it exists and is readable
			if not os.path.exists(args.seal_config):
				raise StandardError("The specified Seal config file %s doens't exist" % args.seal_config)
			if not os.access(args.seal_config, os.R_OK):
				raise StandardError("The specified Seal config file %s isn't readable" % args.seal_config)
			config.read(args.seal_config) # no problems.  Load the file.
		else:
			# check the default path.  If the file exists and is readable we'll load it
			if os.path.exists(args.seal_config):
				if os.access(args.seal_config, os.R_OK):
					config.read(args.seal_config)
				else:
					print >>sys.stderr, "WARNING:  Seal config file %s exists but isn't readable" % args.seal_config

		# override configuration properties from file with the ones
		# provided on the command line.
		for name, value in config.items(SeqalConfig.ConfigSection):
			if not args.properties.has_key(name):
				args.properties[name] = value

		return args, left_over
