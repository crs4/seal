

import pyrapi
from seal.lib.event_monitor import QuietMonitor

import itertools as it


class HiRapiOpts(object):
    def __init__(self, plugin):
        self._rapi_opts = plugin.rapi_opts()

    @property
    def mapq_min(self):
        return self._rapi_opts.mapq_min

    @mapq_min.setter
    def set_mapq_min(self, v):
        self._rapi_opts.mapq_min = v

    @property
    def isize_min(self):
        return self._rapi_opts.isize_min

    @isize_min.setter
    def set_isize_min(self, v):
        self._rapi_opts.isize_min = v

    @property
    def isize_max(self):
        return self._rapi_opts.isize_max

    @isize_max.setter
    def set_isize_max(self, v):
        self._rapi_opts.isize_max = v

    @property
    def n_threads(self):
        return self._rapi_opts.n_threads

    @n_threads.setter
    def set_n_threads(self, v):
        self._rapi_opts.n_threads = v


class HiRapiAligner(object):
    """
    Facade over the various RAPI components.
    """

    AlignerPluginId = 'rapi_bwa'

    def __init__(self, rapi_plugin_id, paired=True):
        self._plugin = pyrapi.load_aligner(rapi_plugin_id)
        self._opts = HiRapiOpts(self._plugin)
        self._plugin.init(self._opts._rapi_opts)
        self._batch = self._plugin.read_batch(2 if paired else 1)

        self._aligner = None
        self._ref = None
        self._qoffset = pyrapi.QENC_SANGER

    @property
    def opts(self):
        return self._opts

    @property
    def aligner_name(self):
        return self._plugin.aligner_name()

    @property
    def aligner_version(self):
        return self._plugin.aligner_version()

    @property
    def plugin_version(self):
        return self._plugin.plugin_version()

    @property
    def paired(self):
        return self._batch.n_reads_per_frag == 2

    @property
    def q_offset(self):
        return self._qoffset

    @q_offset.setter
    def set_qoffset(self, v):
        self._qoffset = v

    @property
    def batch_size(self):
        return self._batch.len / self._batch.n_reads_per_frag

    def reserve_space(self, n_reads):
        self._batch.reserve(n_reads / self._batch.n_reads_per_frag)

    def load_ref(self, path):
        if self._ref is not None:
            self._ref.unload()
        self._ref = self._plugin.ref(path)

    def load_single_end(self, f_id, r1, q1):
        if self._batch.nreads_per_frag != 1:
            raise RuntimeError("Trying to load a pair but aligner is configured for %s reads per fragment" %
                    self._batch.nreads_per_frag)
        self._batch.append(f_id, r1, q1, self._qoffset)

    def load_pair(self, f_id, r1, q1, r2, q2):
        if self._batch.nreads_per_frag != 2:
            raise RuntimeError("Trying to load a pair but aligner is configured for %s reads per fragment" %
                    self._batch.nreads_per_frag)
        self._batch.append(f_id, r1, q1, self._qoffset)
        self._batch.append(f_id, r2, q2, self._qoffset)

    def load_fragment(self, f_id, reads, quals):
        if len(reads) != self._batch.nreads_per_frag:
            raise ValueError("Expected fragment with %s reads but got %s" %\
                    (self._batch.nreads_per_frag, len(reads)))
        if quals and len(reads) != len(quals):
            raise ValueError("Incompatible number of reads and quality strings (%s, %s)" % (len(reads), len(quals)))
        for r, q in it.izip_longest(reads, quals or []):
            self._batch.append(f_id, r, q, self._qoffset)

    def clear_batch(self):
        self._batch.clear()

    def align_batch(self):
        self._aligner.align_reads(self._ref, self._batch)

    def release_resources(self):
        self._ref.unload()

    def ifragments(self):
        for f in self._batch:
            yield f

    def write_sam(self, dest_io, include_header=True):
        if include_header:
            dest_io.write(self._plugin.format_sam_hdr(self._ref))
        for idx in xrange(self._batch.n_fragments):
            dest_io.write(self._plugin.format_sam(self._batch, idx))
