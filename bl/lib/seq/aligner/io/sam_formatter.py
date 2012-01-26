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

class SamFormatter(object):
  """
  Creates SAM records from BwaAlignment objects.
  """

  SAM_FIELDS = [
    'qname',
    'flag',
    'rname',
    'pos',
    'mapq',
    'cigar',
    'mrnm',
    'mpos',
    'isize',
    'seq',
    'qual'
    ]

  def __init__(self, strip_pe_tag=True):
    self.strip_pe_tag = strip_pe_tag

  def format(self, mapping):
    sam_record = {}
    sam_record['qname'] = mapping.get_name()
    if self.strip_pe_tag:
      sam_record['qname'] = sam_record['qname'].rsplit("/",1)[0]
    sam_record['flag'] = str(mapping.flag)
    sam_record['rname'] = mapping.tid or '*'
    sam_record['pos'] = str(mapping.pos)
    sam_record['mapq'] = str(mapping.qual)
    sam_record['cigar'] = mapping.get_cigar_str()
    sam_record['mrnm'] = mapping.mtid or '*'
    sam_record['mpos'] = str(mapping.mpos)
    sam_record['isize'] = str(mapping.isize)
    sam_record['seq'] = mapping.get_seq_5()
    sam_record['qual'] = mapping.get_ascii_base_qual()
    sam_aux = []
    for tag in mapping.each_tag():
      sam_aux.append('%s:%s:%s' % (tag[0], tag[1], tag[2]))
    sam_record['aux'] = sam_aux
    sam_record_str = '\t'.join([sam_record[k] for k in self.SAM_FIELDS])
    if sam_record['aux']:
      sam_record_str += '\t' + '\t'.join(sam_record['aux'])
    return sam_record_str

  def parse(self, sam_record_str):
    r = sam_record_str.split('\t')
    return dict(name=r[0],
                flag=int(r[1]),
                tid=r[2],
                pos=int(r[3]),
                qual=int(r[4]),
                cigar=self.__convert_cigar(r[5]),
                mtid=r[6],
                mpos=int(r[7]),
                isize=int(r[8]),
                seq=r[9],
                qualseq=r[10],
                aux=self.__convert_aux(r[11:]))

  def __convert_cigar(self, cig):
    res, d = [], ''
    for c in cig:
      if c.isalpha():
        res.append((int(d), c))
        d = ''
      else:
        d += c
    return res

  def __convert_aux(self, aux):
    def convert(x):
      return (x[0], x[1],
              int(x[2]) if x[2].isdigit() else x[2])
    return [convert(x.split(':')) for x in aux]
