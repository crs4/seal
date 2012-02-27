
import re
import array

from bl.lib.seq.aligner.mapping import Mapping

class SAMMapping(Mapping):
	"""
	A mapping implementation for storing SAM data.

	A SAMMapping object is constructed from a list of SAM fields --
	see http://samtools.sourceforge.net
	"""

	CIGAR_PATTERN = re.compile(r"(\d+)([MIDNSHP])")
	REFID_PATTERN = re.compile(r"\d+$")
	
	def __init__(self, sam_fields):
		"""
		Provide a sam record as a string (it will be split on tabs to get the
		fields) or directly a list of sam fields.
		"""
		super(SAMMapping, self).__init__()
		if type(sam_fields) == str:
			sam_fields = sam_fields.split("\t")
		self.__name = sam_fields[0]
		self.flag = int(sam_fields[1])
		ref_id_match = self.REFID_PATTERN.search(sam_fields[2]) # it's the best we can do without a sam header or the reference annotations
		if ref_id_match is not None:
			self.ref_id = int(ref_id_match.group())
		self.tid = sam_fields[2]
		self.pos = int(sam_fields[3])
		self.qual = int(sam_fields[4])
		self.__cigar = [(int(n), c) for (n, c) in self.CIGAR_PATTERN.findall(sam_fields[5])]
		if sam_fields[6] == '*':  # is this BWA-specific?
			self.mtid = None
		else:
			self.mtid = sam_fields[6]
		self.mpos = int(sam_fields[7])
		self.isize = int(sam_fields[8])
		self.__seq = sam_fields[9]
		self.__ascii_base_qual = sam_fields[10]
		self.__tags = [tuple(t.split(":", 2)) for t in sam_fields[11:]]

	def get_name(self):
		return self.__name

	def get_seq_5(self):
		return self.__seq

	def get_base_qualities(self):
		if not hasattr(self, '__base_qual'):
			self.__base_qual = array.array(
			  'B', [ord(q) - 33 for q in self.__ascii_base_qual]
			  )
		return self.__base_qual

	def get_cigar(self):
		return self.__cigar

	def each_tag(self):
		for t in self.__tags:
			yield t
