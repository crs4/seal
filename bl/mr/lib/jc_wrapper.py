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

class jc_wrapper(object):
	"""
	Simple wrapper to provide a dict-list __setitem__ method to JobConf
	"""
	def __init__(self, jc):
		self.jc = jc
		self.cache = {}

	def __getitem__(self, k):
		if self.cache.has_key(k):
			return self.cache[k]
		else:
			return self.jc.get(k)

	def get(self, k):
		return self[k]

	def getInt(self, k):
		return int(self[k])

	def getFloat(self, k):
		return float(self[k])

	def getBoolean(self, k):
		return bool(self[k])

	def __setitem__(self, k, v):
		self.cache[k] = v

	def hasKey(self, k):
		return self.cache.has_key(k) or self.jc.hasKey(k)
