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

# FIXME: this does not belong here. We should move it to pydoop or,
# even better, add a testing utilities framework to pydoop (see ticket
# #277) and use that.

#from pydoop._pipes import MapContext, ReduceContext
import StringIO

class SavingLogger(object):
    DEBUG = 50
    INFO = 40
    WARNING = 30
    ERROR = 20
    CRITICAL = 10

    def __init__(self):
        self.output = StringIO.StringIO()
        self.log_level = type(self).DEBUG

    def contents(self):
        return self.output.getvalue()

    def debug(self, *args):
        if self.log_level >= type(self).DEBUG:
            self.output.write(' '.join(map(str, args)))
            self.output.write("\n")

    def info(self, *args):
        if self.log_level >= type(self).INFO:
            self.output.write(' '.join(map(str, args)))
            self.output.write("\n")

    def warning(self, *args):
        if self.log_level >= type(self).WARNING:
            self.output.write(' '.join(map(str, args)))
            self.output.write("\n")

    def error(self, *args):
        if self.log_level >= type(self).ERROR:
            self.output.write(' '.join(map(str, args)))
            self.output.write("\n")

    def critical(self, *args):
        if self.log_level >= type(self).CRITICAL:
            self.output.write(' '.join(map(str, args)))
            self.output.write("\n")

class map_context(object):

  def __init__(self, jc, input_split):
    self.job_conf = jc
    self.input_split = input_split
    self.counters = {}
    self.emitted = {}
    self.status_messages = []
    self._input_key = None
    self._input_value = None

  def getJobConf(self):
    return self.job_conf

  def getInputSplit(self):
    return self.input_split

  def getInputKey(self):
    return self._input_key

  def getInputValue(self):
    return self._input_value

  def getCounter(self, group, name):
    k = '%s:%s' % (group, name)
    self.counters[k] = 0
    return k

  def incrementCounter(self, counter, amount):
    self.counters[counter] += amount

  def emit(self, k, v):
    if not isinstance(k, str):
      raise TypeError("key must be a string (it's a %s)" % type(k))
    if not isinstance(v, str):
      raise TypeError("value must be a string (it's a %s)" % type(v))

    if not self.emitted.has_key(k):
      self.emitted[k] = []
    self.emitted[k].append(v)

  def progress(self):
    pass

  def setStatus(self, status):
    self.status_messages.append(status)

  # for compatibility with pydoop 1
  @property
  def key(self):
    return self._input_key

  @property
  def value(self):
    return self._input_value

  def set_input_key(self, k):
    self._input_key = k

  def set_input_value(self, v):
    self._input_value = v

class reduce_context(object):
  # @param jc:  JobConf object
  # @param values:  a list of dictionaries. The first dictionary
  #          needs to have a 'key' and a 'value'.  Each subsequent
  #          only needs a 'value'.
  def __init__(self, jc, values):
    self.job_conf = jc
    self.counters = {}
    self.emitted = {}
    self.counter = -1
    self._values = values

  def nextValue(self):
    self.counter += 1
    return self.counter < len(self._values)

  def getJobConf(self):
    return self.job_conf

  def getInputKey(self):
    return self._values[0]['key']

  def getInputValue(self):
    return self._values[self.counter]['value']

  def emit(self, k, v):
    if not isinstance(k, str):
      raise TypeError("key must be a string (it's a %s)" % type(k))
    if not isinstance(v, str):
      raise TypeError("value must be a string (it's a %s)" % type(v))

    if not self.emitted.has_key(k):
      self.emitted[k] = []
    self.emitted[k].append(v)

  def progress(self):
    pass

  def setStatus(self, status):
    pass

  def getCounter(self, group, name):
    k = '%s:%s' % (group, name)
    self.counters[k] = 0
    return k

  def incrementCounter(self, counter, amount):
    self.counters[counter] += amount

  def add_value(self, key, value):
    """
    Convenience method to insert a (key, value) pair to the data that
    will be returned by this reduce_context.
    It is recommended that this method be used in place of directly
    modifying the object's 'value' attribute so that in the future
    we may easily change the internals.
    """
    if self._values:
      if self._values[0]['key'] == key:
        self._values.append({'value': value })
      else:
        raise ValueError(("key %s doesn't match the key that's already been inserted (%s).  "
                         "Sorry, but for now we only support a single key value") % (key, self._values[0]['key']))
    else: # empty _values
      self._values.append({'key':key, 'value':value})

  # for compatibility with pydoop 1
  @property
  def key(self):
    return self.getInputKey()

  @property
  def values(self):
    return self.getInputValue()

  def set_values(self, _dict):
    self._values = _dict
