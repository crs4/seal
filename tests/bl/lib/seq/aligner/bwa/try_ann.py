# Copyright (C) 2011 CRS4.
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

"""
bntseq.c
--------

void bns_dump(const bntseq_t *bns, const char *prefix)
{
  char str[1024];
  FILE *fp;
  int i;
  { // dump .ann
    strcpy(str, prefix); strcat(str, ".ann");
    fp = xopen(str, "w");
    fprintf(fp, "%lld %d %u\n", (long long)bns->l_pac, bns->n_seqs, bns->seed);
    for (i = 0; i != bns->n_seqs; ++i) {
      bntann1_t *p = bns->anns + i;
      fprintf(fp, "%d %s", p->gi, p->name);
      if (p->anno[0]) fprintf(fp, " %s\n", p->anno);
      else fprintf(fp, "\n");
      fprintf(fp, "%lld %d %d\n", (long long)p->offset, p->len, p->n_ambs);
    }
    fclose(fp);
  }
"""

import sys
import bl.lib.seq.aligner.bwa as bwa

try:
  ROOT = sys.argv[1]
except IndexError:
  sys.exit("USAGE: %s ROOT_REF_NAME (e.g., refseq.fa)")

bnsp, pacseq = bwa.restore_reference(ROOT)


#regenerate .ann file
print bnsp[0].l_pac, bnsp[0].n_seqs, bnsp[0].seed
for i in xrange(bnsp[0].n_seqs):
  p = bnsp[0].anns[i]
  print p.gi, p.name, (p.anno if p.anno else "")
  print p.offset, p.len, p.n_ambs
