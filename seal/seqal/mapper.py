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

import itertools as it
import json
import logging
import os

from pydoop.pipes import Mapper

import seal.seqal.properties as props
from seal.lib.aligner.hirapi import HiRapiAligner
import seal.lib.mr.utils as utils
from seal.lib.mr.hit_processor_chain_link import HitProcessorChainLink
from seal.lib.mr.emit_sam_link import RapiEmitSamLink
from seal.lib.mr.filter_link import RapiFilterLink
from seal.lib.mr.hadoop_event_monitor import HadoopEventMonitor
import seal.lib.deprecation_utils as deprecation_utils

_BWA_INDEX_MANDATORY_EXT = set(["amb", "ann", "bwt", "pac", "rbwt", "rpac"])
_BWA_INDEX_MMAP_EXT = set(["rsax", "sax"])
_BWA_INDEX_NORM_EXT = set(["rsa", "sa"])
_BWA_INDEX_EXT = _BWA_INDEX_MANDATORY_EXT | _BWA_INDEX_MMAP_EXT | _BWA_INDEX_NORM_EXT


class EmitBdg(HitProcessorChainLink):
    def __init__(self, context, event_monitor, hi_rapi_instance, next_link = None):
        super(EmitBdg, self).__init__(next_link)
        self.ctx = context
        self.event_monitor = event_monitor
        self.hi_rapi = hi_rapi_instance
        self._nprocessed = 0

    def process(self, frag, rapi_frag):
        self._nprocessed += 1
        frag['fragmentSize'] = self.hi_rapi.get_insert_size(rapi_frag)
        paired = len(rapi_frag) == 2

        def rapi_to_avro(read_num, aligned):
            avro_aln = dict()
            avro_aln['readNum'] = read_num
            avro_aln['readName'] = aligned.id
            avro_aln['readPaired'] = paired # from outer scope
            avro_aln['sequence'] = aligned.seq
            avro_aln['qual'] = aligned.qual
            avro_aln['basesTrimmedFromStart'] = 0
            avro_aln['basesTrimmedFromEnd'] = 0
            if paired:
                avro_aln['firstOfPair'] = read_num == 1
                avro_aln['secondOfPair'] = read_num == 2
            if aligned.n_alignments > 0:
                aln = aligned.get_aln(0)
                tags = aln.get_tags()
                avro_aln['primaryAlignment'] = True
                avro_aln['readMapped'] = aln.mapped
                if aln.mapped:
                    avro_aln['contig'] = {
                            'contigName': aln.contig.name,
                            'contigLength': aln.contig.len,
                            'contigMD5': aln.contig.md5,
                            'referenceURL': aln.contig.uri,
                            'assembly': aln.contig.assembly_identifier,
                            'species': aln.contig.species
                            }
                    avro_aln['start'] = aln.pos - 1
                    avro_aln['end'] = aln.pos - 1 + aln.get_rlen() - 1
                    avro_aln['mapq'] = aln.mapq
                    avro_aln['cigar'] = aln.get_cigar_string()
                    avro_aln['properPair'] = aln.prop_paired
                    avro_aln['readNegativeStrand'] = aln.reverse_strand
                    if tags.has_key('MD'):
                        avro_aln['mismatchingPositions'] = tags.pop('MD')
                avro_aln['attributes'] = json.dumps(tags)
            #avro_aln['secondaryAlignment']
            #avro_aln['supplementaryAlignment']
            # LP: we should consider removing these fields from the schema, since it otherwise does not
            # assume reads are paired
            # avro_aln['mateMapped']
            # avro_aln['mateNegativeStrand']
            # avro_aln['mateAlignmentStart']
            # avro_aln['mateAlignmentEnd']
            # avro_aln['mateContig']
            return avro_aln

        frag['alignments'] = [ rapi_to_avro(idx + 1, aln) for idx, aln in enumerate(rapi_frag) ]
        self.ctx.emit('', frag)

        if self._nprocessed == 1:
            self.event_monitor.log_debug("Wrote alignments to frag")
            self.event_monitor.log_debug(frag)
        elif self._nprocessed % 100 == 0:
            self.event_monitor.log_debug("bdg processed %s", self._nprocessed)
        super(EmitBdg, self).process(frag, rapi_frag) # forward pair to next element in chain



class mapper(Mapper):
    """
    Aligns sequences to a reference genome.

    @input-record: C{key} does not matter (standard LineRecordReader);
    C{value} is a tab-separated text line with 5 fields: ID, read_seq,
    read_qual, mate_seq, mate_qual.

    @output-record: protobuf-serialized mapped pairs (map-reduce job) or alignment
    records in SAM format (map-only job).

    @jobconf-param: C{mapred.reduce.tasks} number of Hadoop reduce tasks to launch.
    If the value of this property is set to 0, then the mapper will directly output
    the mappings in SAM format, like BWA.  If set to a value > 0 the mapper will output
    mappings in the protobuf serialized format for the rmdup reducer.

    @jobconf-param: C{seal.seqal.log.level} logging level,
    specified as a logging module literal.

    @jobconf-param: C{mapred.cache.archives} distributed
    cache entry for the bwa index archive. The entry
    is of the form HDFS_PATH#LINK_NAME. The archive for a given
    chromosome must contain (at the top level, i.e., no directories) all
    files generated by 'bwa index' for that chromosome.

    @jobconf-param: C{seal.seqal.alignment.max.isize}: if the
    inferred isize is greater than this value, Smith-Waterman alignment
    for unmapped reads will be skipped.

    @jobconf-param: C{seal.seqal.pairing.batch.size}: how many
    sequences should be processed at a time by the pairing
    function. Status will be updated at each new batch: therefore,
    lowering this value can help avoid timeouts.

    @jobconf-param: C{seal.seqal.fastq-subformat} Specifies base quality
    score encoding.  Supported types are: 'fastq-sanger' and 'fastq-illumina'.

    @jobconf-param: C{mapred.create.symlink} must be set to 'yes'.

    @jobconf-param: C{seal.seqal.min_hit_quality} mapping quality
    threshold below which the mapping will be discarded.
    """
    SUPPORTED_FORMATS = "fastq-illumina", "fastq-sanger"
    DEFAULT_FORMAT = "fastq-sanger"
    COUNTER_CLASS = "SEQAL"
    DeprecationMap = {
      "seal.seqal.log.level":           "bl.seqal.log.level",
      "seal.seqal.alignment.max.isize": "bl.seqal.alignment.max.isize",
      "seal.seqal.pairing.batch.size":  "bl.seqal.pairing.batch.size",
      "seal.seqal.fastq-subformat":     "bl.seqal.fastq-subformat",
      "seal.seqal.min_hit_quality":     "bl.seqal.min_hit_quality",
      "seal.seqal.remove_unmapped":     "bl.seqal.remove_unmapped",
      "seal.seqal.discard_duplicates":  "bl.seqal.discard_duplicates",
      "seal.seqal.nthreads":            "bl.seqal.nthreads",
      "seal.seqal.trim.qual":           "bl.seqal.trim.qual",
    }

    def __get_configuration(self, ctx):
        # TODO:  refactor settings common to mapper and reducer
        jc = ctx.getJobConf()

        jobconf = deprecation_utils.convert_job_conf(jc, self.DeprecationMap, self.logger)

        if jobconf.hasKey('seal.seqal.log.level'):
            level = jobconf.get('seal.seqal.log.level')
            if hasattr(logging, level):
                self.log_level = getattr(logging, level)
            else:
                self.logger.warn("Invalid log level value %s.  Leaving log level at default %s value",
                        level, logging.getLevelName(self.log_level))

        if jobconf.hasKey('seal.seqal.fastq-subformat'):
            self.format = jobconf.get('seal.seqal.fastq-subformat')

        if jobconf.hasKey('seal.seqal.alignment.max.isize'):
            self.max_isize = jobconf.getInt('seal.seqal.alignment.max.isize')

        if jobconf.hasKey('seal.seqal.alignment.min.isize'):
            self.min_isize = jobconf.getInt('seal.seqal.alignment.min.isize')

        if jobconf.hasKey('seal.seqal.pairing.batch.size'):
            self.batch_size = jobconf.getInt('seal.seqal.pairing.batch.size')

        if jobconf.hasKey('seal.seqal.min_hit_quality'):
            self.min_hit_quality = jobconf.getInt('seal.seqal.min_hit_quality')

        if jobconf.hasKey('seal.seqal.remove_unmapped'):
            self.remove_unmapped = jobconf.getBoolean('seal.seqal.remove_unmapped')

        if jobconf.hasKey('seal.seqal.nthreads'):
            self.nthreads = jobconf.getInt('seal.seqal.nthreads')

        if jobconf.hasKey('seal.seqal.trim.qual'):
            self.trim_qual = jobconf.getInt('seal.seqal.trim.qual')

        self.input_format = jc.get(props.InputFormat)

        self.output_format = jc.get(props.OutputFormat)

        if jc.hasKey('mapred.reduce.tasks') and jc.getInt('mapred.reduce.tasks') > 0:
            self.__map_only = False
        else:
            self.__map_only = True

        # validate
        if self.format not in self.SUPPORTED_FORMATS:
            raise ValueError(
              "seal.seqal.fastq-subformat must be one of %r" %
              (self.SUPPORTED_FORMATS,)
              )

        if self.remove_unmapped:
            raise NotImplementedError("seal.seqal.remove_unmapped is currently unsupported")
        if self.min_hit_quality > 0:
            raise NotImplementedError("seal.seqal.min_hit_quality is currently unsupported")
        if self.trim_qual > 0:
            raise NotImplementedError("seal.seqal.trim_qual is currently unsupported")

        if self.max_isize <= 0:
            raise ValueError("'seal.seqal.alignment.max.isize' must be > 0, if specified [1000]")

        if self.batch_size <= 0:
            raise ValueError("'seal.seqal.pairing.batch.size' must be > 0, if specified [10000]")

        # minimum qual value required for a hit to be kept.  By default outputs all the
        # hits BWA returns.
        if self.min_hit_quality < 0:
            raise ValueError("'seal.seqal.min_hit_quality' must be >= 0, if specified [0]")

        # number of concurrent threads for main alignment operation
        if self.nthreads <= 0:
            raise ValueError("'seal.seqal.nthreads' must be > 0, if specified [1]")

        # trim quality parameter used by BWA from read trimming.  Equivalent to
        # the -q parameter for bwa align
        if self.trim_qual < 0:
            raise ValueError("'seal.seqal.trim.qual' must be >= 0, if specified [0]")

        if self.input_format not in (props.Bdg, props.Prq):
            raise ValueError("Unrecognized input format '%s'" % self.input_format)

        if self.output_format not in (props.Bdg, props.Sam):
           raise ValueError("Unrecognized output format '%s'" % self.output_format)

        if not self.__map_only:
            raise NotImplementedError("Only mapping mode is supported at the moment")

    def get_reference_prefix(self, ref_dir):
        """
        Given a directory containing an indexed reference,
        such that all its files have a common name (except the extension),
        this method find the path to the reference including the common name.
         e.g. my_reference/hg_18.bwt
              my_reference/hg_18.rsax
              my_reference/hg_18.sax   => "my_references/hg_18"
              my_reference/hg_18.pac
              my_reference/.irrelevant_file
        """
        file_list = [ p for p in os.listdir(ref_dir) ]

        self.logger.debug("file_list extracted from reference archive: %s", file_list)

        filtered_file_list = [ p for p in file_list if not p.startswith('.') and os.path.splitext(p)[1].lstrip('.') in _BWA_INDEX_EXT ]
        prefix = os.path.commonprefix(filtered_file_list).rstrip('.')
        if not prefix:
            raise RuntimeError("Could not determine common prefix from list of files (%s)" %\
                    filtered_file_list if len(filtered_file_list) < 15 else "{}, ...".format(', '.join(filtered_file_list[0:15])))
        full_prefix = os.path.join(ref_dir, prefix)
        self.logger.debug("full reference prefix: %s", full_prefix)
        return full_prefix

    def _load_reference(self, ctx):
        jc = ctx.getJobConf()
        if jc.has_key(props.LocalReferencePrefix):
            ref_prefix = jc[props.LocalReferencePrefix]
            self.logger.info("Loading reference with prefix %s", ref_prefix)
        elif jc.has_key(props.LocalReferenceDir):
            # reference is specified as a directory.  We need to figure out the prefix
            reference_dir = os.path.abspath(jc[props.LocalReferenceDir])
            self.logger.info("Loading reference from directory %s", reference_dir)
            if not os.path.exists(reference_dir):
                raise RuntimeError("The local reference directory %s doesn't exist" % reference_dir)
            ref_prefix = self.get_reference_prefix(reference_dir)
        else:
            raise RuntimeError("Both configuration properties %s and %s are missing!" % (props.LocalReferenceDir, props.LocalReferencePrefix))

        self.logger.info("Full reference path (prefix): %s", ref_prefix)
        with self.event_monitor.time_block("Loading reference %s" % ref_prefix):
            self.hi_rapi.load_ref(ref_prefix)

    def __init__(self, ctx):
        super(mapper, self).__init__(ctx)

        # define attributes
        self.max_isize = 1000
        self.min_isize = None
        self.batch_size = 10000
        self.min_hit_quality = 0
        self.remove_unmapped = False
        self.nthreads = 1
        self.trim_qual = 0

        self._batch = []
        self.hi_rapi = None
        self.__map_only = None

        self.format = self.DEFAULT_FORMAT
        self.input_format = None
        self.output_format = None

        self.log_level = logging.DEBUG
        self.logger = logging.getLogger("seqal")
        logging.basicConfig(level=self.log_level)

        self.__get_configuration(ctx)
        logging.root.setLevel(self.log_level)

        self.event_monitor = HadoopEventMonitor(self.COUNTER_CLASS, logging.getLogger("mapper"), ctx)

        pe = True # single-end sequencen alignment not yet supported by Seqal
        self.hi_rapi = HiRapiAligner('rapi_bwa', paired=pe)

        # opts
        self.hi_rapi.opts.n_threads = self.nthreads
        self.hi_rapi.opts.isize_max = self.max_isize
        if self.min_isize is not None:
            self.hi_rapi.opts.isize_min = self.min_isize
        self.hi_rapi.qoffset = self.hi_rapi.Qenc_Illumina if self.format == "fastq-illumina" else self.hi_rapi.Qenc_Sanger
        # end opts

        self.logger.info("Using the %s aligner plugin, aligner version %s, plugin version %s",
                self.hi_rapi.aligner_name, self.hi_rapi.aligner_version, self.hi_rapi.plugin_version)
        self.logger.info("Working in %s mode", 'paired-end' if pe else 'single-end')

        # allocate space for reads
        self.logger.debug("Reserving batch space for %s reads", self.batch_size)
        self.hi_rapi.reserve_space(self.batch_size)

        self._load_reference(ctx)

        ######## assemble hit processor chain

        if self.output_format == props.Sam:
            chain = RapiEmitSamLink(ctx, self.event_monitor, self.hi_rapi)
        elif self.output_format== props.Bdg:
            chain = EmitBdg(ctx, self.event_monitor, self.hi_rapi)
        else:
            raise RuntimeError("BUG:  unsupported output format %s" % self.output_format)

        self.hit_visitor_chain = chain

        self._decode_input_fn = self.decode_bdg_input if self.input_format == props.Bdg else self.decode_prq_input


    def decode_prq_input(self, line):
        f_id, r1, q1, r2, q2 = line.split("\t")
        retval = dict()
        retval['readName'] = f_id
        retval['sequences'] = [
                {'bases': r1, 'qualities': q1 },
                {'bases': r2, 'qualities': q2 }
            ]
        retval['alignments'] = []
        return retval

    def make_read_id(self, record):
        read_name = ':'.join( [
            record[k] for k in (
                    'instrument',
                    # need run number
                    'flowcellId',
                    'lane',
                    'tile',
                    'xPosition',
                    'yPosition')
             if record[k] # not empty or None
            ])
        return read_name

    def decode_bdg_input(self, record):
        # convert the unicode strings for the aligner to normal
        # ASCII strings.  RAPI doesn't like unicode.
        if record['readName'] is not None:
            record['readName'] = str(record['readName'])

        for i in xrange(len(record['sequences'])):
            s = record['sequences'][i]
            s['bases'] = str(s['bases'])
            s['qualities'] = str(s['qualities'])
        return record

    def _visit_hits(self):
        for idx, z in enumerate(it.izip(self._batch, self.hi_rapi.ifragments())):
            if idx % 2000 == 0:
                self.logger.debug("\tVisited %s fragments...", idx)
            self.hit_visitor_chain.process(z[0], z[1])

    def _process_batch(self):
        self.logger.info("===== processing batch of %s fragments =====", len(self._batch))
        self.logger.debug("hirapi.batch_size: %s", self.hi_rapi.batch_size)

        with self.event_monitor.time_block("aligning"):
            self.hi_rapi.align_batch()
        with self.event_monitor.time_block("processing alignments"):
            self._visit_hits()
        self.hi_rapi.clear_batch()
        del self._batch[:]

    def map(self, ctx):
        # Accumulates reads in self.pairs, until batch size is reached.
        # At that point it calls run_alignment and emits the output.
        record = self._decode_input_fn(ctx.value)
        self._batch.append(record)
        self.hi_rapi.load_pair(
                record['readName'],
                record['sequences'][0]['bases'], record['sequences'][0]['qualities'],
                record['sequences'][1]['bases'], record['sequences'][1]['qualities'])
        if self.hi_rapi.batch_size >= self.batch_size:
            self._process_batch()

    def close(self):
        # If there are any reads left in the aligner batch,
        # align them too
        if self.hi_rapi.batch_size > 0:
           self._process_batch()
        self.logger.info("Releasing resources")
        self.hi_rapi.release_resources()
        self.logger.info("Closing mapper")
