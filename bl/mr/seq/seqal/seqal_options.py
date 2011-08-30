
import argparse

class SeqalOptions(object):
	ConfigSection = "Seqal"


	class SetProperty(argparse.Action):
		"""
		Used with argparse to parse arguments setting property values.
		Creates an attribute 'property' in the results namespace containing
		all the property-value pairs read from the command line.
		"""
		def __call__(self, parser, namespace, value, option_string=None):
			if not hasattr(namespace, 'properties'):
				setattr(namespace, 'properties', {})
			name, v = value.split('=', 1)
			namespace.properties[name] = v

	def __init__(self):
		self.cmd_parser = argparse.ArgumentParser(description='Distributed BWA read alignment and duplicates removal.')

		self.cmd_parser.add_argument('input', metavar='INPUT', nargs='+', help='input path(s)')
		self.cmd_parser.add_argument('output', metavar='OUTPUT', help='output path')
		self.cmd_parser.add_argument('reference', metavar='REF.tar', help='reference archive (tar or tar.gz)')
		self.cmd_parser.add_argument('-q', '--trimq', metavar='Q', type=int, dest="trimq", 
				help="trim quality, like BWA's -q argument (default: 0).", 
				default=0)
		self.cmd_parser.add_argument('-r', '--num-reducers', metavar='INT', type=int, dest="num_reducers", 
				help="Number of reduce tasks. Specify 0 to perform alignment without duplicates removal (default: 3 * num task trackers).")
		self.cmd_parser.add_argument('-sc', '--seal-config', metavar='FILE', dest="seal_config", default=os.path.join(os.path.expanduser('~'), '.sealrc'),
				help='Override the default Seal config file')
		self.cmd_parser.add_argument('-D', metavar="PROP=VALUE", action=SetProperty, 
				help='Set a property value, such as -D mapred.compress.map.output=true')

	def parse(self):
		# we scan the command line first in case the user wants to 
		# override the default config file location
		args, left_over = self.cmd_parser.parse_known_args(namespace=SeqalOptions.Args())

		# load the config for this program
		config = ConfigParser.SafeConfigParser()
		config.load(args.seal_config)

		# override configuration properties from file with the ones
		# provided on the command line.
		for name, value in config.items(SeqalOptions.ConfigSection):
			if not args.properties.has_key(name):
				args.properties[name] = value

		return args, left_over
