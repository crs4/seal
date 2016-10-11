#!/usr/bin/env python

# Copyright (C) 2011-2016 CRS4.
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

import logging
import pydoop

import seal
import seal.lib.hadut as seal_utilities

def main(class_name, app_name, args):
    log = logging.getLogger('seal')

    log.info("Using hadoop executable: %s", pydoop.hadoop_exec())
    log.info("Using seal jar: %s", seal.seal_jar_path())

    cp = ':'.join(seal.dependency_jars())
    args_with_jars = seal.libjars('java') + tuple(args or [])
    log.debug("arguments: %s", args_with_jars)

    retcode = seal_utilities.run_hadoop_jar(seal.seal_jar_path(), class_name, additional_cp=cp, args_list=args_with_jars)
    if retcode != 0 and retcode != 3: # 3 for usage error
        log.fatal("Error running %s", app_name)
    return retcode
