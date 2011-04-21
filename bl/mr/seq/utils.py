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

import os

def get_ref_archive(jobconf):
  cwd = os.getcwd()
  archive = jobconf.get("mapred.cache.archives")
  if len(archive.split(",")) > 1:
    raise ValueError("mapred.cache.archives can now only reference one index archive")

  try:
    symlink = archive.split("#")[1].strip()
  except IndexError:
    raise ValueError("Cache entry in mapred.cache.archives MUST be <hdfs_path>#<symlink>")

  return os.path.join(cwd, symlink)
