set default_parallel 48;
run /u/pireddu/Projects/seqpig/scripts/filter_defs.pig

seqal = load '$in1' as
  (name:chararray,
  flags:int,
  refname:chararray,
  start:int,
  mapqual:int,
  cigar:chararray,
  materefname:chararray,
  matestart:chararray,
  insertsize:int,
  read:chararray,
  basequal:chararray,
  tag1:chararray,
  tag2:chararray
);
-- Project the seqal data to keep only alignment information, while:
-- 1) adding a column with the read number,
-- 2) fixing the id to match the style in the bwa data
seqal_alignments = foreach seqal generate REPLACE(REGEX_EXTRACT(name, '([^#]+)', 1), '::', ':') as name, (FirstInPair(flags) ? 1 : 2) as read_num, flags .. cigar, tag1, tag2  ;
rare = load '$in2' as
  (name:chararray,
  flags:int,
  refname:chararray,
  start:int,
  mapqual:int,
  cigar:chararray,
  materefname:chararray,
  matestart:chararray,
  insertsize:int,
  read:chararray,
  basequal:chararray,
  tag1:chararray,
  tag2:chararray
);
-- same treatment as for the seqal data
bwa_alignments = foreach rare generate name, (FirstInPair(flags) ? 1 : 2) as read_num, flags .. cigar, tag1, tag2 ;
-- Now join the two datasets by read id and read number.  We end up with a big table that puts
-- the alignments on the same read by seqal and bwa on the same row.
jj = join seqal_alignments by (name, read_num), bwa_alignments by (name, read_num);
-- Keep only rows for which:
-- 1) one aligner mapped the read and the other one didn't, or
-- 2) either aligner gave a mapq > 0 and any of the alignment fields are different.
diff_alignments_with_mapq_gt_0 = filter jj by
((ReadUnmapped(bwa_alignments::flags) AND not ReadUnmapped(seqal_alignments::flags)) OR (not ReadUnmapped(bwa_alignments::flags) AND ReadUnmapped(seqal_alignments::flags))) OR
( (seqal_alignments::mapqual > 0) OR (bwa_alignments::mapqual > 0) ) AND
((seqal_alignments::flags   != bwa_alignments::flags  ) OR
 (seqal_alignments::refname != bwa_alignments::refname) OR
 (seqal_alignments::start   != bwa_alignments::start  ) OR
 (seqal_alignments::cigar   != bwa_alignments::cigar  ));
-- Now we reorganize the table placing the equivalent columns from the
-- two aligners next to each other.  This way we can more readily compare
-- their answers.
projected_diffs = foreach diff_alignments_with_mapq_gt_0 generate
seqal_alignments::name,
seqal_alignments::read_num,
seqal_alignments::flags,   bwa_alignments::flags,
seqal_alignments::refname, bwa_alignments::refname,
seqal_alignments::start,   bwa_alignments::start,
seqal_alignments::mapqual, bwa_alignments::mapqual,
seqal_alignments::cigar,   bwa_alignments::cigar,
seqal_alignments::tag1,   bwa_alignments::tag1,
seqal_alignments::tag2,   bwa_alignments::tag2
;
-- Sort
projected_diffs_o = order projected_diffs by *;
-- Write
store projected_diffs_o into '$output';
