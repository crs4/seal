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


import io
import unittest

from bl.lib.tools.seal_config_file import SealConfigFile, FormatError

class TestSealConfigFile(unittest.TestCase):

	CfgDefaultWithTitle = \
"""[DEFAULT]
key1:value1
key2:value2
"""

	CfgSection1 = \
"""[DEFAULT]
key1:value1
key2:value2
[ Section1 ]
keyS1: valueS1
"""

	CfgSection2 = \
"""[DEFAULT]
key1:value1
key2:value2
[ Section1 ]
keyS1: valueS1
[ Section2 ]
keyS2: valueS2
"""

	def setUp(self):
		self.emptyConfig = SealConfigFile()
		self.config = SealConfigFile()

	def test_empty(self):
		self.assertFalse( self.emptyConfig.has_section("bla") )
		self.assertTrue( self.emptyConfig.has_section("DEFAULT") )
		self.assertEqual(0, len(self.emptyConfig.sections()) )
		self.assertEqual(0, len(self.emptyConfig.items("non existent")))

	def test_default(self):
		self.config.readfp(io.BytesIO(self.__class__.CfgDefaultWithTitle))
		self.assertTrue( self.config.has_section("DEFAULT") )
		self.assertTrue( self.config.has_section("default") )
		self.assertTrue( self.config.has_section("deFAult") )

		items = self.config.items("anything")
		self.assertEqual(2, len(items))
		kv = dict(items)
		self.assertEqual(2, len(kv))
		self.assertEqual("value1", kv.get("key1"))
		self.assertEqual("value2", kv.get("key2"))

	def test_equals(self):
		self.config.readfp( io.BytesIO("key1=value1\n") )
		self.assertEqual("value1", self.config.get("default", "key1"))

	def test_colon(self):
		self.config.readfp( io.BytesIO("key1:value1\n") )
		self.assertEqual("value1", self.config.get("default", "key1"))

	def test_trim_key(self):
		self.config.readfp( io.BytesIO("    key1          :value1\n") )
		self.assertEqual("value1", self.config.get("default", "key1"))

	def test_trim_value(self):
		self.config.readfp( io.BytesIO("key1:     value1       \n") )
		self.assertEqual("value1", self.config.get("default", "key1"))

	def test_section1(self):
		self.config.readfp( io.BytesIO(self.__class__.CfgSection1) )
		self.assertEqual("value1", self.config.get("Section1", "key1"))
		self.assertEqual("valueS1", self.config.get("Section1", "keyS1"))

		kv = dict(self.config.items("Section1"))
		# make sure the iterator goes through all k-v pairs, even the ones inherited from DEFAULT
		self.assertEqual(3, len(kv))
		self.assertEqual("value1", kv.get("key1"))
		self.assertEqual("value2", kv.get("key2"))
		self.assertEqual("valueS1", kv.get("keyS1"))

	def test_section2(self):
		self.config.readfp( io.BytesIO(self.__class__.CfgSection2) )
		self.assertEqual("value1", self.config.get("Section1", "key1"))
		self.assertEqual("valueS1", self.config.get("Section1", "keyS1"))
		self.assertTrue( self.config.get("Section2", "keyS1") is None)
		self.assertEqual("valueS2", self.config.get("Section2", "keyS2"))

	def test_section_that_doesnt_exist(self):
		self.config.readfp( io.BytesIO(self.__class__.CfgSection1) )
		self.assertFalse( self.config.has_section("MySection") )
		self.assertTrue( self.config.get("MySection", "option") is None )

	def test_space_in_section_name(self):
		self.assertRaises(FormatError, self.config.readfp, io.BytesIO("[ Section 1 ]") )

	def test_hash_comment(self):
		self.config.readfp( io.BytesIO(" #key1=value1\nkey2=value2;\n") )
		self.assertTrue( self.config.get("default", "key1") is None)
		self.assertEqual("value2;", self.config.get("default", "key2"))

	def test_semi_colon_comment(self):
		self.config.readfp( io.BytesIO(" ;key1=value1\nkey2=value2;\n") )
		self.assertTrue( self.config.get("default", "key1") is None)
		self.assertEqual("value2;", self.config.get("default", "key2"))

def suite():
	"""Get a suite with all the tests from this module"""
	return unittest.TestLoader().loadTestsFromTestCase(TestSealConfigFile)

if __name__ == '__main__':
	unittest.TextTestRunner(verbosity=2).run(suite())
