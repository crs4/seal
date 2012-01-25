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

# a couple of utility functions to deal with deprecated properties

from bl.mr.lib.jc_wrapper import jc_wrapper

# Warn the user that he's using a deprecated property.
# If new_property is provided, the message will suggest to the user to substitute uses
# of the deprecatedProperty with the new one.
def deprecation_warning(log, deprecated_property, new_property):
	log.warning("Your configuration is using the deprecated property %s", deprecated_property)
	if new_property is None:
		log.warning("You should update your configuration to avoid using it.  See the documentation for details.")
	else:
		log.warning("You should update your configuration to replace it with %s. See the documentation for details.", new_property)


# Check whether a deprecated property is used, and if so emit a warning.
# @param job_conf A Pydoop JobConf object.
# @param deprecatedProperty The name of the deprecated property.
# @param new_property The name of the property that replaces the deprecated one, if any,
#        to write a suggestion to the user.
def check_deprecated_prop(job_conf, log, deprecated_property, new_property):
	if job_conf.hasKey(deprecated_property):
		deprecation_warning(log, deprecated_property, new_property)


def convert_job_conf(jobconf, deprecation_map, logger):
	wrapper = jc_wrapper(jobconf)

	for new, old in deprecation_map.iteritems():
		check_deprecated_prop(wrapper, logger, old, new)
		if wrapper.hasKey(old):
			wrapper[new] = wrapper[old]
			logger.warning("Using value %s for property %s (value taken from its deprecated equivalent property %s).", wrapper[new], new, old)
	return wrapper
