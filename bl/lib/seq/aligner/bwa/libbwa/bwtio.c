/*
Copyright (C) 2011-2012 CRS4.

This file is part of Seal.

Seal is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

Seal is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with Seal.  If not, see <http://www.gnu.org/licenses/>.

Imported from bwa (http://bio-bwa.sourceforge.net).  Modified
by: Simone Leo <simone.leo@crs4.it>, Luca Pireddu
<luca.pireddu@crs4.it>, Gianluigi Zanetti <gianluigi.zanetti@crs4.it>.

*/

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include "bwt.h"
#include "utils.h"
#include <sys/mman.h>
#include <fcntl.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

/** magic number for our modified rsax and sax files */
const uint32_t BWA_MMAP_SA_MAGIC = ('B' | 'W' << 8 | 'A' << 16 | 'T' << 24);

void bwt_dump_bwt(const char *fn, const bwt_t *bwt)
{
	FILE *fp;
	fp = xopen(fn, "wb");
	fwrite(&bwt->primary, sizeof(bwtint_t), 1, fp);
	fwrite(bwt->L2+1, sizeof(bwtint_t), 4, fp);
	fwrite(bwt->bwt, sizeof(bwtint_t), bwt->bwt_size, fp);
	fclose(fp);
}

void bwt_dump_sa(const char *fn, const bwt_t *bwt)
{
	FILE *fp;
	fp = xopen(fn, "wb");
	fwrite(&bwt->primary, sizeof(bwtint_t), 1, fp);
	fwrite(bwt->L2+1, sizeof(bwtint_t), 4, fp);
	fwrite(&bwt->sa_intv, sizeof(bwtint_t), 1, fp);
	fwrite(&bwt->seq_len, sizeof(bwtint_t), 1, fp);
	fwrite(bwt->sa + 1, sizeof(bwtint_t), bwt->n_sa - 1, fp);
	fclose(fp);
}

void bwt_restore_sa(const char *fn, bwt_t *bwt)
{
	char skipped[256];
	FILE *fp;
	bwtint_t primary;

	fp = xopen(fn, "rb");
	fread(&primary, sizeof(bwtint_t), 1, fp);
	xassert(primary == bwt->primary, "SA-BWT inconsistency: primary is not the same.");
	fread(skipped, sizeof(bwtint_t), 4, fp); // skip
	fread(&bwt->sa_intv, sizeof(bwtint_t), 1, fp);
	fread(&primary, sizeof(bwtint_t), 1, fp);
	xassert(primary == bwt->seq_len, "SA-BWT inconsistency: seq_len is not the same.");

	bwt->n_sa = (bwt->seq_len + bwt->sa_intv) / bwt->sa_intv;
	bwt->sa = (bwtint_t*)calloc(bwt->n_sa, sizeof(bwtint_t));
	bwt->sa[0] = -1;

	fread(bwt->sa + 1, sizeof(bwtint_t), bwt->n_sa - 1, fp);
	fclose(fp);
}

bwt_t *bwt_restore_bwt(const char *fn)
{
	bwt_t *bwt;
	FILE *fp;

	bwt = (bwt_t*)calloc(1, sizeof(bwt_t));
	fp = xopen(fn, "rb");
	fseek(fp, 0, SEEK_END);
	bwt->bwt_size = (ftell(fp) - sizeof(bwtint_t) * 5) >> 2;
	bwt->bwt = (uint32_t*)calloc(bwt->bwt_size, 4);
	fseek(fp, 0, SEEK_SET);
	fread(&bwt->primary, sizeof(bwtint_t), 1, fp);
	fread(bwt->L2+1, sizeof(bwtint_t), 4, fp);
	fread(bwt->bwt, 4, bwt->bwt_size, fp);
	bwt->seq_len = bwt->L2[4];
	fclose(fp);
	bwt_gen_cnt_table(bwt);

	return bwt;
}


void bwt_destroy(bwt_t *bwt)
{
	if (bwt == 0) return;
	free(bwt->sa); free(bwt->bwt);
	free(bwt);
}


#ifdef BWT_ENABLE_MMAP

void* bwt_mmap_file(const char *fn, size_t size) {
	int fd = open(fn, O_RDONLY);
	xassert(fd > -1, "Cannot open file");
	
	struct stat buf;
	int s = fstat(fd, &buf);
	xassert(s > -1, "cannot stat file");

	off_t st_size = buf.st_size;
	if (size > 0) {
		xassert(st_size >= size, "bad file size");
		st_size = size;
	}

	// mmap flags:
	// MAP_PRIVATE: copy-on-write mapping. Writes not propagated to file. Our mapping is read-only,
	//              so this setting seems natural.
	// MAP_POPULATE: prefault page tables for mapping.  Use read-ahead. Only supported for MAP_PRIVATE
	// MAP_HUGETLB: use huge pages.  Manual says it's only supported since kernel ver. 2.6.32 
	//              and requires special system configuration.
	// MAP_NORESERVE: don't reserve swap space
	int map_flags = MAP_PRIVATE | MAP_POPULATE | MAP_NORESERVE;
	void* m = mmap(0, st_size, PROT_READ, map_flags, fd, 0);
	xassert(m != MAP_FAILED, "cannot mmap");
	madvise(m, st_size, MADV_WILLNEED);
	return m;
}

uint8_t *bwt_restore_reference_mmap_helper(const char *fn, size_t map_size) {
	return (uint8_t*) bwt_mmap_file(fn, map_size);
}

bwt_t *bwt_restore_bwt_mmap(const char *fn)
{
	void* m = bwt_mmap_file(fn, 0);

	int fd = open(fn, O_RDONLY);
	xassert(fd > -1, "Cannot open file");
	
	struct stat buf;
	int s = fstat(fd, &buf);
	xassert(s > -1, "cannot stat file");

	bwt_t *bwt;
	bwt = (bwt_t*)calloc(1, sizeof(bwt_t));
	bwt->bwt_size = (buf.st_size - sizeof(bwtint_t)*5) >> 2;
	bwt->primary = ((bwtint_t*)m)[0];
	bwt->bwt = &((bwtint_t*)m)[5];
	size_t i;
	for(i = 1; i < 5; ++i) {
	  bwt->L2[i] = ((bwtint_t*)m)[i];
	}
	bwt->seq_len = bwt->L2[4];
	bwt_gen_cnt_table(bwt);
	return bwt;
}

void bwt_restore_sa_mmap(const char *fn, bwt_t *bwt)
{
	void* m = bwt_mmap_file(fn, 0);

	int magic = ((bwtint_t*)m)[0];
	xassert(magic == BWA_MMAP_SA_MAGIC, "incompatible file identifier in .sax or .rsax");

	bwtint_t primary = ((bwtint_t*)m)[1];
	xassert(primary == bwt->primary, 
		"SA-BWT inconsistency: primary is not the same.");
	bwt->sa_intv = ((bwtint_t*)m)[6];
	primary = ((bwtint_t*)m)[7];
	xassert(primary == bwt->seq_len, 
		"SA-BWT inconsistency: seq_len is not the same.");
	bwt->n_sa = (bwt->seq_len + bwt->sa_intv) / bwt->sa_intv;

	bwt->sa = &((bwtint_t*)m)[8];
}

void bwt_destroy_mmap(bwt_t *bwt)
{
	if (bwt == 0) return;
	//free(bwt->sa); 
	//free(bwt->bwt);
	free(bwt);
}

#endif
