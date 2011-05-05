#!/usr/bin/env ruby

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


# Convert a PRQ file (each line has five tab-separated fields:  id, read1, qual1, read2, qual2) 
# to two fastq files (one for each read).

raise(ArgumentError, "usage: #{$0} <prq> <fastq1> <fastq2>") unless ARGV.size == 3

input, output1, output2 = ARGV

[ output1, output2 ].each { |fname|
	if File.exist?(fname) then
		raise(RuntimeError, "File #{output1} already exists")
	end
}

File.open(output1, 'w') { |f1|
	File.open(output2, 'w') { |f2|
		File.open(input) { |prq|
			prq.each_line { |prq_line|
				record = prq_line.split("\t")
				raise(RuntimeError, "Error.  Line #{prq.lineno} has #{record.size} fields instead of 5") unless record.size == 5
				f1.print(record[0], "/1\n", record[1], "\n+\n", record[2], "\n")
				f2.print(record[0], "/2\n", record[3], "\n+\n", record[4])
			}
		}
	}
}
