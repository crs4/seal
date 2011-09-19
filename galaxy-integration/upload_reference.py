#!/usr/bin/env python

################################################################################
# This script copies the specified reference archive to HDFS, if it doesn't
# find the file there.  By default, it will try to copy the archive to HDFS 
# at the same path as the source. E.g.,
# source:  /mnt/storage/reference.tar
# dest:  hdfs://host:port/mnt/storage/reference.tar
# To change this behavious specify a destination path.
#
# In all cases, if it finds a file with the same name and size at the destination
# path it will assume that it is the same file and skip copying.
################################################################################

import os
import sys
import pydoop.hdfs

reference, galaxy_output = sys.argv[1:3]

hdfs = pydoop.hdfs.hdfs("default", 0)
local = pydoop.hdfs.hdfs("", 0)

# look for the archive.  It should follow the naming convention 
# reference_root.tar
reference_archive = None
for ext in ('.tar', '.tar.gz', '.tar.bz2'):
	possible_name = reference + ext
	if os.path.exists(possible_name):
		reference_archive = possible_name
		break

if reference_archive is None:
	raise RuntimeError("Specified reference archive %s does not exist" % reference)

if len(sys.argv) > 3:
	destination = sys.argv[3]
else:
	destination = os.path.abspath(reference_archive)


# now check the destination
if hdfs.exists(destination):
	hdfs_stat = hdfs.get_path_info(destination)
	if os.path.getsize(reference_archive) != hdfs_stat['size']:
		raise RuntimeError("Destination path %s is occupied by a file of a size different from %s" % (destination, reference_archive))
	# else we assume it's the same file
else:
	directory, filename = os.path.split(destination)
	hdfs.create_directory(directory)
	local.copy(reference_archive, hdfs, destination)

# write the HDFS path of the archive to the output file
with open(galaxy_output, 'w') as f:
	f.write(destination)

