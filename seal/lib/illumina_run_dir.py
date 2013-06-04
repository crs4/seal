
"""
Get info from Illumina run directories
"""

import logging
from datetime import datetime
import os
import operator
from xml.etree.ElementTree import ElementTree

class RunDir(object):
    def __init__(self, directory):
        if not os.path.exists(directory):
            raise RuntimeError("Illumina run directory %s doesn't exist" % directory)
        if not os.access(directory, os.R_OK | os.X_OK):
            raise RuntimeError("Can't access Illumina run directory %s" % directory)
        self.path = os.path.abspath(directory)

    def get_path(self):
        return self.path

    def is_finished(self):
        """
        Determine if the sequencing directory has all files.

        The final checkpoint file will differ depending if we are a
        single or paired end run.
        """
        # Check final output files; handles both HiSeq and GAII
        run_info = self.get_run_info()
        hi_seq_checkpoint = "Basecalling_Netcopy_complete_Read%s.txt" % run_info.get_num_reads()
        to_check = ["Basecalling_Netcopy_complete_SINGLEREAD.txt",
                    "Basecalling_Netcopy_complete_READ2.txt",
                    hi_seq_checkpoint]
        return reduce(operator.or_,
                [os.path.exists(os.path.join(self.path, f)) for f in to_check])

    def get_run_info(self):
        if hasattr(self, 'run_info'):
            return self.run_info

        run_info_path = os.path.join(self.path, 'RunInfo.xml')
        if not os.access(run_info_path, os.R_OK):
            raise RuntimeError("Can't read run info from %s" % run_info_path)
        self.run_info = RunInfo(run_info_path)
        return self.run_info

    def get_run_parameters(self):
        if hasattr(self, 'run_param'):
            return self.run_param

        file_path = os.path.join(self.path, 'runParameters.xml')
        if not os.access(file_path, os.R_OK):
            raise RuntimeError("Can't read run parameters from %s" % file_path)
        self.run_param = RunParameters(file_path)
        return self.run_param

    def get_base_calls_dir(self):
        return os.path.join(self.path, 'Data/Intensities/BaseCalls')

    @staticmethod
    def __make_lane_tile_fname(lane, tile, ext="", prefix="s"):
        return "%s_%s_%s%s" % (prefix, lane, tile, "." + ext)

    def make_filter_path(self, lane, tile):
        return os.path.join( self.get_base_calls_dir(), "L%03d" % int(lane), self.__make_lane_tile_fname(lane, tile, "filter", "s") )

    def make_control_path(self, lane, tile):
        return os.path.join( self.get_base_calls_dir(), "L%03d" % int(lane), self.__make_lane_tile_fname(lane, tile, "control", "s") )

    def make_clocs_path(self, lane, tile):
        return os.path.join( self.path, 'Data/Intensities', "L%03d" % int(lane), self.__make_lane_tile_fname(lane, tile, "clocs", "s") )

    def make_qseq_name(self, lane, tile, read_num):
        """
        Create a file name corresponding to the default qseq names generated
        by Illumina's software.
        """
        return "s_%s_%s_%s_qseq.txt" % (lane, read_num, tile)

class RunParameters(object):
    """
    Simple class to load run parameters xml into a dictionary, using the
    same structure and tag names as the xml itself.
    """

    class Read(object):
        def __init__(self, num, cycle_first, cycle_last, indexed=False):
            self.num = num
            self.first_cycle = cycle_first
            self.last_cycle = cycle_last
            self.indexed = indexed

        def icycles(self):
            return xrange(self.first_cycle, self.last_cycle+1)

        def __str__(self):
            return "Read { 'Num': %s, 'FirstCycle': %s, 'LastCycle': %s, 'IsIndexed': %s }" % \
               (self.num, self.first_cycle, self.last_cycle, self.indexed)

    def __init__(self, xml=None):
        if xml:
            self.load(xml)

    def __node_children_to_dict(self, node):
        children = node.getchildren()
        if not children:
            return None
        retval = dict()
        for child in children:
            # We handle three tags specially since they have many children with
            # the same name, and that wouldn't play well with our dict hierarchy.
            if child.tag == 'AlignToPhiX':
                value = [ c.text for c in child.getchildren() ]
            elif child.tag == 'SelectedSections':
                value = [ c.attrib['Name'] for c in child.getchildren() ]
            elif child.tag == 'Reads':
                value = [ c.attrib for c in child.getchildren() ]
            else:
                # all the rest are simply converted recursively
                value = self.__node_children_to_dict(child) or child.text
            retval[child.tag] = value
        return retval

    def load(self, xml):
        if hasattr(self, 'reads'):
            # invalidate cached reads list
            delattr(self, 'reads')
        tree = ElementTree()
        tree.parse(xml)
        root = tree.getroot()
        self.version = root.find('Version').text
        if self.version != '1':
            logging.warn("untested run_parameters.xml version %s", self.version)
        # load the raw tree
        self.setup = self.__node_children_to_dict(root.find('Setup'))

    def get_reads(self):
        """
        Get defined read objects.  Caches result.
        """
        if hasattr(self, 'reads'):
            return self.reads

        read_elements = self.setup['Reads']
        start = 1
        reads = list()
        for i in xrange(len(read_elements)):
            elem = read_elements[i]
            end = start + int(elem['NumCycles']) - 1 # start + num cycles - 1
            reads.append(self.Read(int(elem['Number']), start, end, elem['IsIndexedRead'] == 'Y'))
            start += int(elem['NumCycles'])
        self.reads = reads
        return self.reads

    def get_lanes(self):
        return self.setup['AlignToPhiX']

    def iget_simple_tile_codes(self):
        if self.setup['SelectedSurface'] == 'BothLaneSurfaces':
            surfaces = (1, 2)
        elif self.setup['SelectedSurface'] == 'FirstLaneSurface':
            # LP: for my testing purposes, I've defined 'FirstLaneSurface' as
            # meaning "surface 1 only".  This lets me use a small one-surface
            # data set for my tests.
            surfaces = (1,)
        else:
            # LP: I don't know what the other legal values for this element are
            raise ValueError("Unrecognized value for element 'SelectedSurface': %s.  Try 'BothLaneSurfaces'" % self.setup['SelectedSurface'])
        num_swaths = int(self.setup['NumSwaths'])
        tiles_per_swath = int(self.setup['NumTilesPerSwath'])
        for surface in surfaces:
            for swath in xrange(1, num_swaths+1):
                for tile in xrange(1, tiles_per_swath+1):
                    yield("%d%d%02d" % (surface, swath, tile))


class RunInfo(object):
    """
    Class to parse the RunInfo.xml file.
    """

    def __init__(self, xml=None):
        if xml:
            self.load(xml)

    def load(self, xml):
        tree = ElementTree()
        tree.parse(xml)
        r = tree.find('Run')
        self.run_id = r.attrib['Id']
        self.run_number = r.attrib['Number']
        self.flowcell_id = r.find('Flowcell').text
        self.instrument = r.find('Instrument').text
        self.date = datetime.strptime(r.find('Date').text, "%y%m%d")
        self.flowcell_layout = r.find('FlowcellLayout').attrib
        self.reads = r.find('Reads').getchildren()
        return self

    def get_num_reads(self):
        return len(self.reads)

def __absolutize_path(parent, relative_path):
    return os.path.join(parent, relative_path)


def is_finished(path):
    """
    Shortcut function to check whether the run writing to 'path' is finished.
    Equivalent to running::

        run_dir = RunDir(path)
        return run_dir.is_finished()
    """
    run_dir = RunDir(path)
    return run_dir.is_finished()


# vim: expandtab tabstop=4 shiftwidth=4 autoindent smartindent:
