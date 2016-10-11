# Copyright (C) 2011-2016 CRS4.
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

SAM_FPD = 1     # paired
SAM_FPP = 2     # properly paired
SAM_FSU = 4     # self-unmapped
SAM_FMU = 8     # mate-unmapped
SAM_FSR = 16    # self on the reverse strand
SAM_FMR = 32    # mate on the reverse strand
SAM_FR1 = 64    # this is read one
SAM_FR2 = 128   # this is read two
SAM_FSC = 256   # secondary alignment
SAM_FQC = 0x200   # failed quality checks
SAM_FDP = 0x400   # PCR or optical duplicate
