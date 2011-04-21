Seqal Counters
================

Seqal provides a number of counters that can be consulted as it runs and at the
end of a job.  Here is an explanation of what they mean.

=================================================== ===========================================================
**Counter name**                                    **Explanation**
--------------------------------------------------- -----------------------------------------------------------
DUPLICATE FRAGMENTS                                 Number of unpaired fragments identified as
                                                    duplicates.  The fragment may be "unpaired" because its 
                                                    mate was unmapped.
DUPLICATE PAIRS                                     Number of pairs identified as duplicates.
EMITTED SAM RECORDS                                 SAM records written to the output files.
MAPPED COORDINATES                                  Number of mapped reads.
READS FILTERED: LOW QUALITY                         Number of reads discarded because of mapq score
                                                    under the minimum threshold.
READS FILTERED: UNMAPPED                            Number of reads discarded because they were unmapped.
READS PROCESSED                                     Number of reads processed by Seqal (may not be the
                                                    same as the ones processed by PairReadsQSeq, since it filters as well).
RMDUP UNIQUE FRAGMENTS                              Number of unique unpaired reads written to output after duplicate identification.
RMDUP UNIQUE PAIRS                                  Number of unique read pairs written to output after duplicate identification.
TIME_ANALYZE_PAIRS (CAL_+SW+REFGAP+PROCESS)         Total number of milliseconds (all nodes summed together) spent 
                                                    aligning, filtering, and writing the mapper output.
TIME_BUILD_BWSA                                     Total number of ms spent converting input to BWA in-memory structures.
TIME_CAL_PAC_POS_PE                                 Total number of ms spent generating paired alignments.
TIME_CAL_SA_REG_GAP                                 Total number of ms spent calculating SA position.
TIME_COUNT_BASES                                    Total number of ms spent counting bases.
TIME_DESTROY_SEQUENCES                              Total number of ms spent freeing sequence memory.
TIME_PAIRED_SW                                      Total number of ms spent performing Smith-Waterman alignment.
TIME_REFINE_GAPPED                                  Total number of ms spent refining gapped alignments.
TIME_RESTORE_INDEX                                  Total number of ms spent loading the reference index.
TIME_RESTORE_REFERENCE                              Total number of ms spent loading the reference sequence.
TOTAL BASES                                         Number of bases processes.
TRIMMED BASES                                       Number of bases trimmed (when read trimming is enabled).
=================================================== ===========================================================
