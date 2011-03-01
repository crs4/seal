This is a Java implementation of the pair_reads_qseq application.  Its general 
purpose is to preprocess sequence fragments output from the base calling 
procedure so that it may be fed to the alignment process.

=== Requirements ===
  * Hadoop (tested with version 0.20.2)
  * JDK 1.6
  * ant

=== Building instructions ===

1. Define the HADOOP_HOME environment variable pointing to your Hadoop
installation.
2. Call ant in the root PairReadsQSeq directory.

=== Running ===

hadoop jar dist/PairReadsQSeq.jar [ hadoop options ] <input directory> <output directory>

PairReadsQSeq will process all files in the input directory.
Hadoop options aren't mandatory, it can be handy place to tune Hadoop's
behaviour.  For an example, see test/integration_test/run.sh.

Here's an example invocation that runs well on our test 30-node cluster:



hadoop jar dist/PairReadsQSeq.jar \
-D mapred.reduce.tasks=120 \
-D mapred.tasktracker.reduce.tasks.maximum=4 -D mapred.tasktracker.map.tasks.maximum=7 \
-D mapred.child.java.opts=-Xmx1156m -D io.sort.mb=800 \
-D mapred.job.reduce.input.buffer.percent=0.8 -D mapred.compress.map.output=true \
/user/pireddu/prq_input /user/pireddu/prq_output
