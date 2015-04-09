

import pyrapi

import itertools as it

class HiRapiOpts(object):
    def __init__(self, plugin):
        self._rapi_opts = plugin.opts()

    @property
    def mapq_min(self):
        return self._rapi_opts.mapq_min

    @mapq_min.setter
    def mapq_min(self, v):
        self._rapi_opts.mapq_min = v

    @property
    def isize_min(self):
        return self._rapi_opts.isize_min

    @isize_min.setter
    def isize_min(self, v):
        self._rapi_opts.isize_min = v

    @property
    def isize_max(self):
        return self._rapi_opts.isize_max

    @isize_max.setter
    def isize_max(self, v):
        self._rapi_opts.isize_max = v

    @property
    def n_threads(self):
        return self._rapi_opts.n_threads

    @n_threads.setter
    def n_threads(self, v):
        self._rapi_opts.n_threads = v


class HiRapiAligner(object):
    """
    Facade over the various RAPI components.
    """

    AlignerPluginId = 'rapi_bwa'
    Qenc_Sanger = pyrapi.rapi.QENC_SANGER
    Qenc_Illumina = pyrapi.rapi.QENC_ILLUMINA

    def __init__(self, rapi_plugin_id, paired=True):
        self._plugin = pyrapi.load_aligner(rapi_plugin_id)
        self._opts = HiRapiOpts(self._plugin)
        self._plugin.init(self._opts._rapi_opts)
        self._batch = self._plugin.read_batch(2 if paired else 1)

        self._aligner = None
        self._ref = None
        self._qoffset = self.Qenc_Sanger

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
    def q_offset(self, v):
        if v not in (self.Qenc_Sanger, self.Qenc_Illumina):
            raise ValueError("Invalid base quality offset %s" % v)
        self._qoffset = v

    @property
    def batch_size(self):
        """
        Number of reads in batch
        """
        return len(self._batch)

    def reserve_space(self, n_reads):
        """
        Reserve space in memory, so that `append` won't have to reallocate.
        """
        self._batch.reserve(n_reads / self._batch.n_reads_per_frag)

    def load_ref(self, path):
        if self._ref is not None:
            self._ref.unload()
        try:
            self._ref = self._plugin.ref(path)
        except RuntimeError as e:
            raise RuntimeError("%s Reference path: %s" % (e.message, path))


    def load_read(self, f_id, r1, q1):
        if self._batch.n_reads_per_frag != 1:
            raise RuntimeError("Trying to load a single read but aligner is configured for %s reads per fragment" %
                    self._batch.n_reads_per_frag)
        self._batch.append(f_id, r1, q1, self._qoffset)

    def load_pair(self, f_id, r1, q1, r2, q2):
        if self._batch.n_reads_per_frag != 2:
            raise RuntimeError("Trying to load a pair but aligner is configured for %s reads per fragment" %
                    self._batch.n_reads_per_frag)
        self._batch.append(f_id, r1, q1, self._qoffset)
        self._batch.append(f_id, r2, q2, self._qoffset)

    def load_fragment(self, f_id, reads, quals):
        if len(reads) != self._batch.n_reads_per_frag:
            raise ValueError("Expected fragment with %s reads but got %s" %\
                    (self._batch.n_reads_per_frag, len(reads)))
        if quals and len(reads) != len(quals):
            raise ValueError("Incompatible number of reads and quality strings (%s, %s)" % (len(reads), len(quals)))
        for r, q in it.izip_longest(reads, quals or []):
            self._batch.append(f_id, r, q, self._qoffset)

    def clear_batch(self):
        self._batch.clear()

    def align_batch(self):
        if self._ref is None:
            raise RuntimeError("Reference not loaded. You must load a reference before aligning")
        if self._aligner is None:
            self._aligner = self._plugin.aligner(self._opts._rapi_opts)
        self._aligner.align_reads(self._ref, self._batch)

    def release_resources(self):
        if self._ref is not None:
            self._ref.unload()

    def ifragments(self):
        for f in self._batch:
            yield f

    def write_sam(self, dest_io, include_header=True):
        if include_header:
            dest_io.write(self._plugin.format_sam_hdr(self._ref))
        if self._batch.n_fragments > 0:
            dest_io.write(self._plugin.format_sam_by_batch(self._batch, 0))
        for idx in xrange(1, self._batch.n_fragments):
            dest_io.write('\n')
            dest_io.write(self._plugin.format_sam_by_batch(self._batch, idx))

    def format_sam_for_fragment(self, fragment):
        return self._plugin.format_sam(fragment)

    def get_insert_size(self, fragment):
        if len(fragment) == 2:
            if fragment[0].mapped and fragment[1].mapped:
                return self._plugin.get_insert_size(fragment[0].get_aln(0), fragment[1].get_aln(0))
            else:
                return None
        else:
            raise ValueError("To calculate the insert size the reads must be paired (got %s reads)" % len(fragment))
