# BEGIN_COPYRIGHT
#
# Copyright 2009-2015 CRS4.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy
# of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#
# END_COPYRIGHT

import re
import sys

import pydoop.mapreduce.api as api
import pydoop.mapreduce.pipes as pp
from pydoop.avrolib import AvroContext

QENC_ILLUMINA = 64
QENC_SANGER = 33

_BarcodeRE = re.compile(r'#([AGCTN]+)')
_ReadNumRE = re.compile(r'/([123])')

class IlluminaNameScanner(object):
    def scan(self, name):
        fields = name.split('\t')
        if len(fields) == 2:
            first_part = fields[0].split(':')
            second_part = fields[1].split(':')
            if len(first_part) == 7 and len(second_part) == 3:
                outval = dict()
                outval['instrument'] = first_part[0]
                outval['run_num']    = first_part[1]
                outval['lane']       = first_part[3]
                outval['tile']       = first_part[4]
                outval['xpos']       = first_part[5]
                outval['ypos']       = first_part[6]
                outval['index']      = second_part[3]
                outval['read_num']   = second_part[0]
                outval['filter']     = second_part[1]
                return outval
        return None

class FastqNameScanner(object):
    def scan(self, name):
        parts = name.split(':')
        if len(parts) == 6:
            # process the last bit, which may contain the barcode and the read number
            last_part = parts[-1]
            index = None
            read_num = None
            cut_point = len(last_part)
            m = _BarcodeRE.search(last_part)
            if m:
                index = m.group(1)
                cut_point = m.start()
            m = _ReadNumRE.search(last_part)
            if m:
                read_num = m.group(1)
                if m.start() < cut_point:
                    cut_point = m.start()
            last_part = last_part[0:cut_point]

            outval = dict()
            outval['instrument'] = parts[0] # instrument
            outval['run_num']    = parts[1] # run id..this is actually a run number though
            outval['lane']       = parts[2] # lane
            outval['tile']       = parts[3] # tile
            outval['xpos']       = parts[4] # x pos
            outval['ypos']       = last_part # y pos
            outval['index']      = index or '' # index
            outval['read_num']   = read_num or '' # read num
            return outval
        # else
        return None

class OnlyReadAndIndexScanner(object):
    def scan(self, name):
        index = ''
        read_num = ''
        m = _BarcodeRE.search(name)
        if m:
            index = m.group(1)
        m = _ReadNumRE.search(name)
        if m:
            read_num = m.group(1)
        outval = {
            'index': index,
            'read_num': read_num
        }
        if index or read_num:
            return outval
        else:
            return None

class Mapper(api.Mapper):
    def __init__(self, ctx):
        super(Mapper, self).__init__(ctx)
        self._read_name_scanner = None
        # In rich metadata mode we look for metadata fields in the schema such
        # as 'lane', 'tile', etc.  These were implemented by our group in some tests
        # but were never integrated into the main bdgenomics schema
        self._rich_metadata_mode = None

    @staticmethod
    def _check_rich_metadata(record):
        # 'lane' is only in the rich schema
        if record.get('lane') is not None:
            return True
        return False

    def _init_mapper(self, value):
        """
        Check the value we received and try to decifer the schema type and id format.
        Then we set the mapper's attributes appropriately.
        """
        self._rich_metadata_mode = self._check_rich_metadata(value)
        if self._rich_metadata_mode:
            self._read_name_scanner = OnlyReadAndIndexScanner()
        else:
            read_name = value.get('readName')
            scanner = IlluminaNameScanner()
            result = scanner.scan(read_name)
            if result:
                self._read_name_scanner = scanner
                return

            scanner = FastqNameScanner()
            result = scanner.scan(read_name)
            if result:
                self._read_name_scanner = scanner
                return

            # last resort
            self._read_name_scanner = OnlyReadAndIndexScanner()

    def map(self, ctx):
        payload = ctx.value
        if self._rich_metadata_mode is None:
            self._init_mapper(payload)
            print >> sys.stderr, "Using name scanner of type", self._read_name_scanner.__class__

        name_metadata = self._read_name_scanner.scan(payload.get('readName'))
        if name_metadata is None:
            name_metadata = {}

        if self._rich_metadata_mode:
            tpl = [
                payload.get('instrument') or '',
                name_metadata.get('run_num') or payload.get('runId') or '',
                str(payload.get('lane') or ''),
                str(payload.get('tile') or ''),
                str(payload.get('xpos') or ''),
                str(payload.get('ypos') or ''),
                name_metadata.get('index') or '0',
                '', # idx 7
                '', # idx 8
                '', # idx 9
                '', # idx 10
            ]
        else:
            # get everything from the read name
            tpl = [
                payload.get('instrument') or '',
                name_metadata.get('run_num') or payload.get('runId') or '',
                name_metadata.get('lane') or '',
                name_metadata.get('tile') or '',
                name_metadata.get('xpos') or '',
                name_metadata.get('ypos') or '',
                name_metadata.get('index') or '0',
                '', # idx 7
                '', # idx 8
                '', # idx 9
                '', # idx 10
            ]
        # now fill in the sequence and quality strings
        for idx, seq in enumerate(payload['sequences']):
            tpl[7] = str(idx + 1)
            tpl[8] = seq.get('bases') or ''
            q = seq.get('qualities') or ''
            # recode the quality from sanger offset to illumina
            tpl[9] = ''.join( [ chr(ord(c) - QENC_SANGER + QENC_ILLUMINA) for c in q ] )
            tpl[10] = '0' if seq.get('failedVendorQualityChecks') else '1'
            ctx.emit('', '\t'.join(tpl))

def __main__():
    factory = pp.Factory(Mapper)
    pp.run_task(factory, context_class=AvroContext)
