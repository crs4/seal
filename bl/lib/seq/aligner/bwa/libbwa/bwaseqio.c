/*
Copyright (C) 2011 CRS4.

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

#include <zlib.h>
#include <ctype.h>
#include "bwtaln.h"
#include "utils.h"
#include "bamlite.h"

#include "kseq.h"
KSEQ_INIT(gzFile, gzread)

extern unsigned char nst_nt4_table[256];
static char bam_nt16_nt4_table[] = { 4, 0, 1, 4, 2, 4, 4, 4, 3, 4, 4, 4, 4, 4, 4, 4 };

struct __bwa_seqio_t {
	// for BAM input
	int is_bam, which; // 1st bit: read1, 2nd bit: read2, 3rd: SE
	bamFile fp;
	// for fastq input
	kseq_t *ks;
};

bwa_seqio_t *bwa_bam_open(const char *fn, int which)
{
	bwa_seqio_t *bs;
	bam_header_t *h;
	bs = (bwa_seqio_t*)calloc(1, sizeof(bwa_seqio_t));
	bs->is_bam = 1;
	bs->which = which;
	bs->fp = bam_open(fn, "r");
	h = bam_header_read(bs->fp);
	bam_header_destroy(h);
	return bs;
}

bwa_seqio_t *bwa_seq_open(const char *fn)
{
	gzFile fp;
	bwa_seqio_t *bs;
	bs = (bwa_seqio_t*)calloc(1, sizeof(bwa_seqio_t));
	fp = xzopen(fn, "r");
	bs->ks = kseq_init(fp);
	return bs;
}

void bwa_seq_close(bwa_seqio_t *bs)
{
	if (bs == 0) return;
	if (bs->is_bam) bam_close(bs->fp);
	else {
		gzclose(bs->ks->f->f);
		kseq_destroy(bs->ks);
	}
	free(bs);
}

void seq_reverse(int len, ubyte_t *seq, int is_comp)
{
	int i;
	if (is_comp) {
		for (i = 0; i < len>>1; ++i) {
			char tmp = seq[len-1-i];
			if (tmp < 4) tmp = 3 - tmp;
			seq[len-1-i] = (seq[i] >= 4)? seq[i] : 3 - seq[i];
			seq[i] = tmp;
		}
		if (len&1) seq[i] = (seq[i] >= 4)? seq[i] : 3 - seq[i];
	} else {
		for (i = 0; i < len>>1; ++i) {
			char tmp = seq[len-1-i];
			seq[len-1-i] = seq[i]; seq[i] = tmp;
		}
	}
}

int bwa_trim_read(int trim_qual, bwa_seq_t *p)
{
	int s = 0, l, max = 0, max_l = p->len - 1;
	if (trim_qual < 1 || p->qual == 0) return 0;
	for (l = p->len - 1; l >= BWA_MIN_RDLEN - 1; --l) {
		s += trim_qual - (p->qual[l] - 33);
		if (s < 0) break;
		if (s > max) {
			max = s; max_l = l;
		}
	}
	p->clip_len = p->len = max_l + 1;
	return p->full_len - p->len;
}

static bwa_seq_t *bwa_read_bam(bwa_seqio_t *bs, int n_needed, int *n, int is_comp, int trim_qual)
{
	bwa_seq_t *seqs, *p;
	int n_seqs, l, i;
	long n_trimmed = 0, n_tot = 0;
	bam1_t *b;

	b = bam_init1();
	n_seqs = 0;
	seqs = (bwa_seq_t*)calloc(n_needed, sizeof(bwa_seq_t));
	while (bam_read1(bs->fp, b) >= 0) {
		uint8_t *s, *q;
		int go = 0;
		if ((bs->which & 1) && (b->core.flag & BAM_FREAD1)) go = 1;
		if ((bs->which & 2) && (b->core.flag & BAM_FREAD2)) go = 1;
		if ((bs->which & 4) && !(b->core.flag& BAM_FREAD1) && !(b->core.flag& BAM_FREAD2))go = 1;
		if (go == 0) continue;
		l = b->core.l_qseq;
		p = &seqs[n_seqs++];
		p->tid = -1; // no assigned to a thread
		p->qual = 0;
		p->full_len = p->clip_len = p->len = l;
		n_tot += p->full_len;
		s = bam1_seq(b); q = bam1_qual(b);
		p->seq = (ubyte_t*)calloc(p->len + 1, 1);
		p->qual = (ubyte_t*)calloc(p->len + 1, 1);
		for (i = 0; i != p->full_len; ++i) {
			p->seq[i] = bam_nt16_nt4_table[(int)bam1_seqi(s, i)];
			p->qual[i] = q[i] + 33 < 126? q[i] + 33 : 126;
		}
		if (bam1_strand(b)) { // then reverse 
			seq_reverse(p->len, p->seq, 1);
			seq_reverse(p->len, p->qual, 0);
		}
		if (trim_qual >= 1) n_trimmed += bwa_trim_read(trim_qual, p);
		p->rseq = (ubyte_t*)calloc(p->full_len, 1);
		memcpy(p->rseq, p->seq, p->len);
		seq_reverse(p->len, p->seq, 0); // *IMPORTANT*: will be reversed back in bwa_refine_gapped()
		seq_reverse(p->len, p->rseq, is_comp);
		p->name = strdup((const char*)bam1_qname(b));
		if (n_seqs == n_needed) break;
	}
	*n = n_seqs;
	if (n_seqs && trim_qual >= 1)
		fprintf(stderr, "[bwa_read_seq] %.1f%% bases are trimmed.\n", 100.0f * n_trimmed/n_tot);
	if (n_seqs == 0) {
		free(seqs);
		bam_destroy1(b);
		return 0;
	}
	bam_destroy1(b);
	return seqs;
}

#define BARCODE_LOW_QUAL 13

bwa_seq_t *bwa_read_seq(bwa_seqio_t *bs, int n_needed, int *n, int mode, int trim_qual)
{
	bwa_seq_t *seqs, *p;
	kseq_t *seq = bs->ks;
	int n_seqs, l, i, is_comp = mode&BWA_MODE_COMPREAD, is_64 = mode&BWA_MODE_IL13, l_bc = mode>>24;
	long n_trimmed = 0, n_tot = 0;

	if (l_bc > 15) {
		fprintf(stderr, "[%s] the maximum barcode length is 15.\n", __func__);
		return 0;
	}
	if (bs->is_bam) return bwa_read_bam(bs, n_needed, n, is_comp, trim_qual); // l_bc has no effect for BAM input
	n_seqs = 0;
	seqs = (bwa_seq_t*)calloc(n_needed, sizeof(bwa_seq_t));
	while ((l = kseq_read(seq)) >= 0) {
		if (is_64 && seq->qual.l)
			for (i = 0; i < seq->qual.l; ++i) seq->qual.s[i] -= 31;
		if (seq->seq.l <= l_bc) continue; // sequence length equals or smaller than the barcode length
		p = &seqs[n_seqs++];
		if (l_bc) { // then trim barcode
			for (i = 0; i < l_bc; ++i)
				p->bc[i] = (seq->qual.l && seq->qual.s[i]-33 < BARCODE_LOW_QUAL)? tolower(seq->seq.s[i]) : toupper(seq->seq.s[i]);
			p->bc[i] = 0;
			for (; i < seq->seq.l; ++i)
				seq->seq.s[i - l_bc] = seq->seq.s[i];
			seq->seq.l -= l_bc; seq->seq.s[seq->seq.l] = 0;
			if (seq->qual.l) {
				for (i = l_bc; i < seq->qual.l; ++i)
					seq->qual.s[i - l_bc] = seq->qual.s[i];
				seq->qual.l -= l_bc; seq->qual.s[seq->qual.l] = 0;
			}
			l = seq->seq.l;
		} else p->bc[0] = 0;
		p->tid = -1; // no assigned to a thread
		p->qual = 0;
		p->full_len = p->clip_len = p->len = l;
		n_tot += p->full_len;
		p->seq = (ubyte_t*)calloc(p->len, 1);
		for (i = 0; i != p->full_len; ++i)
			p->seq[i] = nst_nt4_table[(int)seq->seq.s[i]];
		if (seq->qual.l) { // copy quality
			p->qual = (ubyte_t*)strdup((char*)seq->qual.s);
			if (trim_qual >= 1) n_trimmed += bwa_trim_read(trim_qual, p);
		}
		p->rseq = (ubyte_t*)calloc(p->full_len, 1);
		memcpy(p->rseq, p->seq, p->len);
		seq_reverse(p->len, p->seq, 0); // *IMPORTANT*: will be reversed back in bwa_refine_gapped()
		seq_reverse(p->len, p->rseq, is_comp);
		p->name = strdup((const char*)seq->name.s);
		{ // trim /[12]$
			int t = strlen(p->name);
			if (t > 2 && p->name[t-2] == '/' && (p->name[t-1] == '1' || p->name[t-1] == '2')) p->name[t-2] = '\0';
		}
		if (n_seqs == n_needed) break;
	}
	*n = n_seqs;
	if (n_seqs && trim_qual >= 1)
		fprintf(stderr, "[bwa_read_seq] %.1f%% bases are trimmed.\n", 100.0f * n_trimmed/n_tot);
	if (n_seqs == 0) {
		free(seqs);
		return 0;
	}
	return seqs;
}

void bwa_free_read_seq(int n_seqs, bwa_seq_t *seqs)
{
	int i, j;
	for (i = 0; i != n_seqs; ++i) {
		bwa_seq_t *p = seqs + i;
		for (j = 0; j < p->n_multi; ++j)
			if (p->multi[j].cigar) free(p->multi[j].cigar);
		free(p->name);
		free(p->seq); free(p->rseq); free(p->qual); free(p->aln); free(p->md); free(p->multi);
		free(p->cigar);
	}
	free(seqs);
}


#ifdef BWT_EXPORT_LIBRARY_FUNCTIONS
bwa_seq_t *bwa_alloc_seq(int n_needed, int seq_len, int name_len)
{
	bwa_seq_t *seqs, *p;
	int i;
	
	seqs = (bwa_seq_t*)calloc(n_needed, sizeof(bwa_seq_t));
	if (seqs != NULL)
	{
		for (i = 0; i < n_needed; ++i) {
			p = &seqs[i];
			p->tid = -1; // no assigned to a thread
			p->full_len = p->clip_len = p->len = seq_len;
			p->seq = (ubyte_t*)calloc(p->len, 1);
			p->rseq = (ubyte_t*)calloc(p->full_len, 1);
			p->name = (char*)calloc(name_len+1, 1);
			p->qual = (ubyte_t*)calloc(p->len, 1);
		}
	}
	return seqs;
}


/**
 * For internal use.
 * This function is used by bwa_init_sequences to initialize a single
 * bwa_seq_t structure.  Parameters matching bwa_init_sequences have
 * the same meaning.
 * 
 * @param seq:  Pointer to the bwa_seq_t structure which will be populated by this
 *              function.
 * @param name_str:  pointer to NULL-terminated name string to be copied into bwa_seq_t. 
 * @param seq_str:   pointer to NULL-terminated DNA string to be copied into bwa_seq_t. 
 * @param qual_str:  optional ASCII-encoded base quality string to be copied into bwa_seq_t.
 *                   Use q_offset to specify the offset used by the encoding.
 * @param q_offset:  the amount by which the quality bytes have been offset 
 *                   from 0 (e.g. sanger => 33, illumina => 64, etc.)
 * @param trim_qual:  quality score to use for read trimming.  Use 0 to turn off read trimming.
 */
static int bwa_init_seq_t(bwa_seq_t* seq, const char*const name_str, 
		const char*const seq_str, const char*const qual_str, 
		const int q_offset, const int trim_qual)
{ 
	const int name_len = strlen(name_str);
	const int seq_len = strlen(seq_str);
	seq->tid = -1; // no assigned to a thread
	seq->full_len = seq->clip_len = seq->len = seq_len;

	// assign pointers to NULL
	seq->aln = NULL;
	seq->multi = NULL;
	seq->cigar = NULL;
	seq->md = NULL;
	seq->bc[0] = 0; // null-terminate barcode field

	seq->seq = (ubyte_t*)malloc(seq_len);
	seq->rseq = (ubyte_t*)malloc(seq_len);
	seq->name = (char*)malloc(name_len+1); // +1 for \0
	if (qual_str)
		seq->qual = (ubyte_t*)malloc(seq_len);
	else
		seq->qual = NULL;

	if (seq->seq  == NULL || seq->rseq ==  NULL || seq->name ==  NULL || (seq->qual ==  NULL && qual_str != NULL))
	{
		fprintf(stderr, "Unable to allocate memory for sequence\n");
		goto error;
	}
	// else

	// encode sequence and set quality
	int i;
	for (i = 0; i < seq->full_len; ++i)
		seq->seq[i] = nst_nt4_table[(int)seq_str[i]];

	if (seq->qual)
	{
		for (i = 0; i < seq->full_len; ++i)
		{
			seq->qual[i] = (int)qual_str[i] - q_offset + 33; // 33 is the Sanger offset.  BWA expects it this way.
			if (seq->qual[i] > 127)
			{ // qual is unsigned, by Sanger base qualities have an allowed range of [0,94], and 94+33=127
				fprintf(stderr, "Invalid base quality score %d\n", seq->qual[i]);
				goto error;
			}
		}
	}

	// trim if requested
	if (trim_qual >= 1)
		bwa_trim_read(trim_qual, seq);

	// create the complement sequence for the range [0,clip_len)
	// Don't rely on the rseq array to output a correct complement sequence
	// since it doesn't contain the entire sequence.
	memcpy(seq->rseq, seq->seq, seq->clip_len);
	seq_reverse(seq->clip_len, seq->rseq, 1);
	// reverse the sequence too
	seq_reverse(seq->clip_len, seq->seq, 0); // *IMPORTANT*: will be reversed back in bwa_refine_gapped()

	// and finally the name
	strcpy(seq->name, name_str);
	// trim /[12]$
	int t = name_len;
	if (t > 2 && seq->name[t-2] == '/' && (seq->name[t-1] == '1' || seq->name[t-1] == '2'))
		seq->name[t-2] = '\0';
	return 0;
error:
	// In case of error, free any allocated memory and return -1
	free(seq->seq); free(seq->rseq); free(seq->name); free(seq->qual);
	return -1;
}

/**
 * From an n_seqs-long array of (name, seq, qual), allocate and 
 * build a corresponding array of bwa_seq_t.
 *
 * @param data2d:  Nx3 matrix of char*.  Each row is composed of 3 
 *                 null-terminated string pointers: sequence name, 
 *                 DNA fragment, base quality (Optional.  If not provided use NULL).
 * @param n_seqs:  Number sequences (i.e. number of rows in data2d)
 * @param q_offset: The amount by which the base qualities have been offset from 0.
 *                 For instance, Phred-scaled Sanger format adds 33 to each base
 *                 quality score, so provide 33.  Ignored for NULL base quality strings.
 * @param trim_qual:  If > 0, read trimming is performed.  This parameter is equivalent
 *                  to the -q option for BWA.
 *
 * @return  Upon success, a pointer to an array of bwa_seq_t, 
 *   long n_seqs elements.  Free it with bwa_free_read_seq.
 *   In case of an error, the function returns NULL.
 */
bwa_seq_t* bwa_init_sequences(const char**const data2d, const size_t n_seqs, const int q_offset, const int trim_qual)
{
	const int row_size = 3;
	bwa_seq_t* array = calloc(n_seqs, sizeof(bwa_seq_t));

	int i;
	for (i = 0; i < n_seqs; ++i)
	{
		bwa_seq_t* seq = array + i;
		const char** tuple = &data2d[i*row_size];
		int retval = bwa_init_seq_t(seq, tuple[0], tuple[1], tuple[2], q_offset, trim_qual);
		if (retval < 0)
		{
			fprintf(stderr, "bwa_init_sequences: unable to allocate sequence\n");
			// clean-up sequences [0,i) and free array
			bwa_free_read_seq(i, array);
			return NULL;
		}
	}

	return array;
}
#endif
