# Copyright (C) 2011-2012 CRS4.
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


import ConfigParser
import re

# This class provides functionality very similar to ConfigParser.
# There are a few differences however:
# * when a requested section doesn't exist ConfigParser raises an error
#   (NoSectionError) while SealConfigFile shows the default values.
# * case sensitive section and key names (except DEFAULT)
# * ignore blanks at the start of a line
# * when getting an option that doesn't exist SealConfigFile returns None (ConfigParser raises NoOptionError)

class FormatError(RuntimeError):
    pass

class SealConfigFile(object):

    DefaultSectionName = "DEFAULT"

    def __init__(self):
        self.__config = self.__make_config()

    def __make_config(self):
        # make a config object, but overwrite is key formatting function to
        # make it case sensitive and trim the spaces around the key.
        cfg = ConfigParser.SafeConfigParser()
        cfg.optionxform = lambda option: option.strip()
        return cfg

    def read(self, filename):
        with open(filename) as f:
            self.readfp(f)

    def readfp(self, fp):
        # A custom reader for the config file since we slightly modified the
        # Python ConfigFile format:
        # * case sensitive section and key names (except DEFAULT)
        # * ignore blanks at the start of a line
        self.__config = self.__make_config()
        re_section = re.compile('^\[ *(\w+) *\] *$')
        re_kv_line = re.compile('^ *([^:=]+)[:=] *(.*) *$')
        current_section = None
        lineno = 0

        for line in fp:
            lineno += 1
            line = line.strip()
            if line == "" or line[0] == '#' or line[0] == ';':
                # comment or blank line
                continue
            else:
                m_section = re_section.match(line)
                m_kv = re_kv_line.match(line)
                if m_section:
                    current_section = m_section.group(1)
                    if current_section != self.__class__.DefaultSectionName:
                        self.__config.add_section(current_section)
                elif m_kv:
                    k,v = m_kv.group(1,2)
                    if current_section is None or current_section.upper() == self.__class__.DefaultSectionName:
                        self.__config.set("DEFAULT", k, v)
                    else:
                        self.__config.set(current_section, k, v)
                else:
                    raise FormatError("Invalid config file line %d: %s" % (lineno, line))


    def has_section(self, name):
        """Whether the config has the named section.  Unlike ConfigParser, returns true for section "default"."""
        if name is not None and name.upper() == "DEFAULT":
            return True
        else:
            return self.__config.has_section(name)

    def sections(self):
        return self.__config.sections()

    def get(self, section, key):
        """Unlike ConfigParser, if the section doesn't exist is looks
        for the key among the defaults (ConfigParser raises NoSectionError).
        Returns None if the section-key doesn't exist."""
        try:
            if self.__config.has_section(section):
                return self.__config.get(section, key)
            else:
                return self.__config.get("DEFAULT", key)
        except ConfigParser.NoOptionError:
            return None

    def items(self, section):
        if self.__config.has_section(section):
            return self.__config.items(section)
        else:
            return self.__config.items("DEFAULT")
