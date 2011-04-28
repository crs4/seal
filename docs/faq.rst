.. _faq:

FAQ
=====

How do I decide how many reduce tasks to use?
-----------------------------------------------

You should follow the standard Hadoop advice to set the number of reduce tasks
in which to split your problem.  Generally, it should be a multiple of the
number of reduce tasks your cluster can run simultaneously, so that they may 
all finish in one iteration.  For instance, if your cluster is configured with 5
reduce slots per node, and has 10 nodes, try using 50 reduce tasks.

If the input is too big, you may find that the reducers require too much memory
to complete the job in one reduce iteration.  In this case, double the number of 
reduce tasks to 100.

Finally, we have found it useful to subtract a small percentage from the total
number of reduce tasks calculated following the instructions above.  In this way
we leave a few free slots to re-run tasks that may fail, keeping stragglers at
the end from disproportionately increasing run times (and from time to time some 
tasks will fail and be re-started automatically by Hadoop).

``run_seqal.sh`` by default uses 6 reduce tasks per active node.  You can override 
that value from the command line if you wish.
