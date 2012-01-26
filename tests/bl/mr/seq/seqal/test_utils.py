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

import random
import re

from bl.lib.seq.aligner.mapping import SimpleMapping

def erase_read1(pair):
	pair[0] = None
	p1 = pair[1]
	p1.set_mate_mapped(False)
	p1.mtid = None
	p1.m_ref_id = None
	p1.mpos = 0
	return pair

def erase_read2(pair):
	pair[1] = None
	p0 = pair[0]
	p0.set_mate_mapped(False)
	p0.mtid = None
	p0.m_ref_id = None
	p0.mpos = 0
	return pair

def pair1():
	p = (SimpleMapping(), SimpleMapping())
	p[0].set_name("p1:read/1") ; p[1].set_name("p1:read/2")
	p[0].tid = "chr1" ; p[1].tid = "chr1"
	p[0].ref_id = 0 ; p[1].ref_id = 0
	p[0].pos = 12345  ; p[1].pos = p[0].pos + 150
	p[0].set_read1(True) ; p[1].set_read2(True)
	p[0].qual = 50 ; p[1].qual = 30
	return p

def pair2():
	# reversed reads such that r2 is at a lower position than r1
	p = (SimpleMapping(), SimpleMapping())
	p[0].set_name("p1:read/1") ; p[1].set_name("p1:read/2")
	p[0].tid = "chr1" ; p[1].tid = "chr1"
	p[0].ref_id = 0 ; p[1].ref_id = 0
	p[0].pos = 12345 + 150 ; p[1].pos = p[0].pos - 150
	p[0].set_read1(True) ; p[1].set_read2(True)
	p[0].qual = 50 ; p[1].qual = 30
	return p


def make_key(mapping):
	# This function must be the same as MarkDuplicatesEmitter::get_hit_key
	if mapping.is_mapped():
		values = ("%04d" % mapping.ref_id, "%012d" % mapping.get_untrimmed_pos(), 'R' if mapping.is_on_reverse() else 'F')
	else:
		# 
		values = ("unmapped", "%010d" % random.randint(0, 9999999999))
	return ':'.join( values )
