#!/bin/bash

# Copyright (C) 2011-2012 CRS4.
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


##################################################################################
##################################################################################
#
# This script is provided as an example of how to use the various tools
# in the Seal suite to put together a sequencing workflow.
#
# This workflow is currently used by the CRS4 Sequencing and Genotyping Platform.
# It reads the qseq files produced by Illumina sequencers and produces
# recalibrated BAM files.
#
# Requirements:
#   * a Hadoop cluster
#   * Sun Grid Engine queue
#   * GATK
#   * samtools
#   * python, bash
#
# There are many paths and settings that are specific to the set-up at
# CRS4.  You will have to modify these if you hope to use this script directly--e.g.:
#   * binary/library paths
#   * reference paths
#   * Seal path
#   * email address for notifications
# We've tried to highlight them;  search the script for <+customize+>.
#
# Also, this program requires a Hadoop cluster AND a Sun Grid Engine queue.
# We recommend you only use it as an example, or perhaps cut off the serialized
# part and use the Hadoop-ized portion to feed into your current workflow.  We
# are currently planning on re-implementing this program using a higher-level
# workflow framework such as Oozie (http://yahoo.github.com/oozie/) or similar.
#
# Important environment variables:
#   * HADOOP_CONF_DIR:  path to hadoop configuration directory
#   * HADOOP_HOME: path to hadoop installation.
#
# = Workflow Summary =
#
# == Input ==
# This workflow will process files named (s_[lanespec]_*_qseq.txt)
# directly under the sample directory.  Input files must be qseq files
# and must be reads from the same sample.
#
# == Workflow steps ==
#
# Hadoop-ized steps:
# * (optional) distcp from shared storage to HDFS
# * Hadoop PRQ
# * Hadoop seqal (BWA aln, BWA sampe, rmdup)
# * Hadoop read_sort
#
# Serial steps:
# * copy SAM back to local storage and generate BAM
# * compute index for BAM files (samtools_index)
# * compute recalibration table (GATK_table)
# * recalibrate BAM files (GATK_recabBAM)
# * send email notice
#
# == Usage ==
#
# The workflow can read data from a path mounted on the Hadoop
# cluster nodes (--local-input-dir).  In this case, the script with use
# distcp_files (and hadoop distcp) to copy the s_{lanes}_*_qseq.txt files to
# HDFS.  Alternatively, you can use data already on HDFS (--hdfs-input-dir).
#
# Within the dataset, you must specify which lanes to analyze (--lanes) as a
# comma-separated list of numbers (e.g. --lanes 1,2,3,4,5).
#
# You must specify a locally mounted output directory (--output-dir).
# Unfortunately, the output directory name must be in a specific format since this
# script uses it to extract metadata about the experiment (this is a terrible
# legacy issue we will resolve in the new implementation).  The output directory
# name must be of the form:
#   <your prefix>_<date>_<run id>_<your suffix>
# What really matters is that it have at least 4 fields separated by underscores.
#
# You also need to specify a reference to use for the BWA alignment (--genome option).
# Also see the get_reference() function;  the selected genome archive must be on
# HDFS at the path referenced by the RefArchive variable.
#
# Finally, this program needs a file that lists the samples in the input data
# (--lane-content).  Here's an example:
# $ cat LaneContent
# 0:my_sample_001
# 1:my_sample_001
# 2:my_sample_001
# 3:my_sample_001
# 4:my_sample_2
# 5:my_sample_2
# 6:my_sample_2
# 7:my_sample_2
#
# If --rerun is specified, the workflow will try to resume from the last failed
# run.  For this to work, you specify the exact same parameters as for the
# previous attempt, with the exception of the input directory which for a re-run
# will necessarily be on HDFS (and so you must use --hdfs-input-dir).  The input
# directory must also still exist on HDFS (look for "input" under the main working
# directory created on HDFS by the previous run).
#
# If you run the script without arguments you'll get a error message listing all
# the arguments.



#### Safety checks enabled ####
# <+customize+>
# If you're not familiar with these bash options, take a look at the bash man
# page.  errexit causes the script to exit when a command fails (returns an
# exit code different from zero).  nounset causes the script to exit when an
# undefined reference is accessed.  If this script exists without an error, the
# reason is probably one of the above.
set -o errexit
set -o nounset
#set -x
###############################


###### "configuration" #################################

# <+customize+>
# Look over this entire section.

# Place your paths here
export PATH="/ELS/els5/acdc/opt/bin/:${PATH##/ELS/els5/acdc/opt/bin/:}"
export LD_LIBRARY_PATH="/ELS/els5/acdc/opt/lib/:${LD_LIBRARY_PATH##/ELS/els5/acdc/opt/lib/:}"

WorkflowRoot=$(readlink -f $(dirname $0) )
# Read filtering by number of known bases.  See PairReadsQSeq documentation.
KnownBasesPerRead=30

# Comma-separated email addresses.  Only the first address gets qsub messages.
# Everyone gets the end notification
MailRecipient="your.email.address@server.com"

# Sun grid engine queue.  Specify your configuration.
Queue="-l q=genoma -l hostname='node066|node067|node068|node069|node070|node071|node072|node073'"

Samtools="/path/to/samtools-0.x.x/samtools"

# Path to Seal.  Insert the path where you extracted the compiled Seal archive.
SealPath="/home/myuser/seal"

# These are relative to the one above.  You shouldn't have to modify them.
SealBin="${SealPath}/bin"
Distcp="${SealBin}/distcp_files"
Prq="${SealBin}/prq"
Seqal="${SealBin}/seqal"
ReadSort="${SealBin}/read_sort"
MergedSorted="${SealBin}/merge_alignments"


#################### Functions ######################

function log() {
	echo -e $(date +"%F %T") -- $@
}

function run_cmd() {
	log "start>> " $@ >> "${OutputDir}/commands-${LanesSuffix}"
	log "start>> " $(echo $@ | cut -d ' ' -f 1)
	eval $@
	log "end<< " $@ >> "${OutputDir}/commands-${LanesSuffix}"
	log "end<< " $(echo $@ | cut -d ' ' -f 1)
}

function usage_error() {
	if [ $# -gt 0 ]; then
		echo -e "Error: " $* >&2
	fi
	echo # newline
	(
	echo "Usage: $(basename $0) --genome <reference name> --output-dir <output path> ( --local-input-dir <path> | --hdfs-input-dir <hdfs path> ) --lanes <csv> [OPTIONS]"
	echo "Arguments:"
	printf "\t%-23s " "--local-input-dir <d>"; printf "Local directory containing the QSeq input files (mutually exclusive with --hdfs-input-dir).\n"
	printf "\t%-23s " "--hdfs-input-dir <d>";  printf "HDFS directory containing the QSeq input files (mutually exclusive with --local-input-dir).\n"
	printf "\t%-23s " "--lanes <csv lanes>";   printf "Comma-separated list of lanes to process (e.g. 1,2,3; mandatory).\n"
	printf "\t%-23s " "--genome <g>";          printf "Name of the reference to use (mandatory).\n"
	printf "\t%-23s " "--output-dir <d>";      printf "Path to locally mounted output directory.  Will be created if it doesn't exist (mandatory).\n"
	printf "\t%-23s " "--lane-content <file>"; printf "Path to LaneContent file describing the content of the lanes (mandatory).\n"
	printf "\t%-23s " "--qvalue <q>";          printf "q value for read trimming [default: 0].\n"
	printf "\t%-23s " "--base-qualities <bq>"; printf "Base quality encoding, illumina or sanger. [default: illumina].\n"
	printf "\t%-23s " "--rerun";               printf "Re-use existing files.\n"
	)>&2
	exit 1
}

function error() {
	if [ $# -gt 0 ]; then
		echo -e $@ >&2
	else
		echo "Generic error.  Tell Luca to insert a better error message!" >&2
	fi
	exit 1
}

function check_seal() {
	if [ ! -x "${Distcp}" ]; then
		error "Can't find distcp executable ${Distcp}. Please check the configuration."
	fi

	if [ ! -x "${Prq}" ]; then
		error "Can't find prq executable ${Prq}. Please check the configuration."
	fi

	if [ ! -x "${Seqal}" ]; then
		error "Can't find seqal executable ${Seqal}. Please check the configuration."
	fi

	if [ ! -x "${ReadSort}" ]; then
		error "Can't find read_sort executable ${ReadSort}. Please check the configuration."
	fi

	if [ ! -x "${MergedSorted}" ]; then
		error "Can't find merging executable ${MergedSorted}. Please check the configuration."
	fi
}

function check_hadoop() {
	if ! PYTHONPATH="${SealPath}" python -m bl.lib.tools.hadut ; then
		error "Error loading the hadut module"
	fi

	if [ ! -d "${HADOOP_CONF_DIR:-}" ]; then
		error "Please set the HADOOP_CONF_DIR variable to refer to Hadoop cluster configuration directory.\nCurrently HADOOP_CONF_DIR is = ${HADOOP_CONF_DIR:-blank}" >&2
	fi

	Hadoop=$(PYTHONPATH="${SealPath}" python -c "import bl.lib.tools.hadut as hadut; print hadut.hadoop") # set path to executable
	HadoopNumNodes=$(PYTHONPATH="${SealPath}" python -c "import bl.lib.tools.hadut as hadut; print hadut.num_nodes()")
	if [ ${HadoopNumNodes} -eq 0 ]; then
		error "No hadoop worker nodes found.  Something's wrong with the Hadoop cluster configuration"
	fi
}

function wait_for_sge_job() { # pass it job id
	job_id=${1}
	log "waiting for qsub job ${job_id}"
	tic=0
	wait_start=$(date "+%s")
	sleep 10

	while true; do
		qstat_exit_code=0
		qstat_output="$(qstat -j ${job_id} 2>&1)" || qstat_exit_code=1 # must use ||, otherwise errexit will cause our script to terminate.
		if [ $qstat_exit_code -ne 0 ]; then # if qstat failed
			job_exists="false"
			echo "${qstat_output}" | grep "Following jobs do not exist" >/dev/null 2>&1 || job_exists="true"
			if [ "${job_exists}" == "true" ]; then
				log "qmaster problem.  Waiting..."
			else
				log "qsub job ${job_id} finished running"
				return $(qacct -j ${job_id} | grep exit_status | awk '{print $2;}') # return job exit status
			fi
		else #i the job exists and we wait
			now=$(date "+%s")
			wait_time=$(( ${now} - ${wait_start} ))
			printf "\r%20s s " "${wait_time}"
			if [ ${tic} -eq 0 ]; then
				tic=1
				printf "-"
			else
				tic=0
				printf "|"
			fi
		fi
		sleep 10
	done
	printf "\nfinished waiting for job ${job_id}"
}

# <+customize+>
# This function writes a file "parameters-<lanes>" to the output directory.  You
# may customize it however you like.
function write_params() {
	printf -v line "%80s" # sets a variable "line" with 80 blanks
	line="${line// /-}"   # substitute the blanks with '-' to create a line

	(echo "############ PARAMETERS ################"
	echo "Command line: $0 $@"; echo "${line}"
	echo "Workflow Root:  ${WorkflowRoot}"; echo "${line}"
	echo -n "Input: "
 	if [ -n "${InputFiles:-}" ]; then
		echo -n "files "
		# show the unique sample directories, lanes, and reads used.
		echo ${InputFiles} | sed -e 's/\s\+/\n/g' | sed -e 's/\(s_[1-8]_[12]\)_.*$/\1/' | sort -u
	else
		echo "HDFS dir ${HdfsInputDir:-}"
	fi
	echo "${line}"
	echo "ReRun:  ${ReRun:-false}"; echo ${line}
	echo "LaneContentFile:  ${LaneContent}"; echo "${line}"
	echo "Output dir:  ${OutputDir}"; echo "${line}"
	echo "PE: 1"; echo "${line}"
	echo "Genome:  ${Genome}"; echo "${line}"
	echo "Reference archive:  ${RefArchive:-none}"; echo "${line}"
	echo "Reference path:  ${RefPath:-none}"; echo "${line}"
	echo "DbSnpPath: ${DbSnpPath:-none}"; echo "${line}"
	echo "Q value for aln:  ${Qvalue}"; echo "${line}"
	echo "BaseQualities:  ${BaseQualities}"; echo "${line}"
	echo "Required known bases per read:  ${KnownBasesPerRead}" ; echo "${line}"
	echo "SeqalPath:  ${SeqalPath:-}" ; echo "${line}"
	echo "########################################") >> "${OutputDir}/parameters-${LanesSuffix}"
}

# <+customize+>
# Here you can customize the email that's sent when the workflow ends.
function send_end_notice() {
	(   field_width=55 # length of the line below
	    echo "############### BWA hadoop $(basename $0) Pipeline ##############"
	    message="${OutputDir} ${LanesSuffix} finished at $(date)"
	    printf "%*s\n" $(( (${field_width} - ${#message}) / 2)) "${message}"
	    echo "#######################################################"
	    cat "${OutputDir}/parameters-${LanesSuffix}") >> "${OutputDir}/mail-${LanesSuffix}"
	mail -s "bwa on ${OutputDir}, lanes ${LanesSuffix}" ${MailRecipient} < "${OutputDir}/mail-${LanesSuffix}"
}

function check_blank() {
	if [ -z "${1}" ]; then
		usage_error $2
	fi
	return 0
}

function scan_args() {
	while [ $# -gt 0 ]; do
		local token="${1}"
		shift || true # shift arguments, but don't fail if none is there
		case "${token}" in
			--hdfs-input-dir)
				HdfsInputDir="${1}"
				check_blank "${HdfsInputDir}" "missing value for --hdfs-input-dir"
				shift || true # consume the argument value
				;;
			--local-input-dir)
				LocalInputDir="${1}"
				check_blank "${LocalInputDir}" "missing value for --local-input-dir"
				shift || true # consume the argument value
				;;
			--lanes)
				Lanes="${1}"
				check_blank "${Lanes}" "missing value for --lanes"
				shift || true # consume the argument value
				;;
			--output-dir)
				OutputDir="${1}"
				check_blank "${OutputDir}" "missing value for --output-dir"
				shift || true # consume the argument value
				;;
			--genome)
				Genome="${1}"
				check_blank "${Genome}" "missing value for --genome"
				shift || true # consume the argument value
				;;
			--qvalue)
				Qvalue=${1}
				check_blank "${Qvalue}" "missing value for --qvalue"
				shift || true # consume the argument value
				if [ ${Qvalue} -lt 0 ]; then
					usage_error "Qvalue must be greater than or equal to 0 (got ${Qvalue})"
				fi
				;;
			--base-qualities)
				BaseQualities="${1}"
				shift || true # consume the argument value
				check_blank "${BaseQualities}" "missing value for --base-qualities"
				if [ "${BaseQualities}" == "sanger" ]; then
					usage_error "Sorry.  Sanger base quality isn't implemented. Talk to Luca!"
				elif [ "${BaseQualities}" != "illumina" ]; then
					usage_error "Invalid base quality type.  Must be one of \"illumina\" and \"sanger\""
				fi
				;;
			--lane-content)
				LaneContent="${1}"
				shift || true # consume the argument value
				check_blank "${LaneContent}" "missing value for --lane-content"
				if [ ! -r "${LaneContent}" ]; then
					usage_error  "Sorry.  LaneContent file ${LaneContent} isn't readable"
				fi
				;;
			--rerun)
				# TODO
				## Re-running a failed experiment is implemented simplistically.
				ReRun="true"
				;;
			-*)
				usage_error "Unknown option ${token}"
				;;
			*)
				usage_error "Unrecognized argument ${token}"
				;;
		esac
	done

	if [ -z "${OutputDir:-}" ]; then
		usage_error "You must specify the analysis output directory with --output-dir"
	elif [ -z "${Genome:-}" ]; then
		usage_error "You must specify the reference genome for the analysis with --genome"
	elif [ -n "${HdfsInputDir:-}" -a -n "${LocalInputDir:-}" ]; then
		usage_error "You can't specify both --hdfs-input-dir and --local-input-dir"
	elif [ -z "${HdfsInputDir:-}" -a -z "${LocalInputDir:-}" ]; then
		usage_error "You must specify either --local-input-dir or --hdfs-input-dir."
	elif [ -z "${LaneContent:-}" ]; then
		usage_error "You must specify an LaneContent file with --lane-content"
	elif [ -z "${Lanes:-}" ]; then
		usage_error "You must specify a comma-separated list of lanes with --lanes"
	fi

	OutputDir="$(readlink -f "${OutputDir}")" # turn the output path into an absolute path

	# check lanes format
	if ! python -c "import re,sys; sys.exit( 0 if re.match('[1-8](,[1-8])*$', '${Lanes}') else 1)" ; then
		usage_error "Invalid lanes list ${Lanes}"
	fi

	if [ -n "${LocalInputDir:-}" ]; then
		if [ ! -d "${LocalInputDir}" ]; then
			usage_error "Can't find local input directory '${LocalInputDir}"
		fi
	elif ! hdfs_stat "${HdfsInputDir}" ; then
		usage_error "Can't find HDFS input directory '${HdfsInputDir}'"
	fi

	# default base quality encoding:  illumina
	BaseQualities="${BaseQualities:-illumina}"

	# default qvalue:  0
	Qvalue="${Qvalue:-0}"

	# default ReRun value: false
	ReRun="${ReRun:-false}"
}

# <+customize+>
# Place your own reference archives here.  Use 1kg as an example.
function get_reference() {
	case "${Genome}" in
	UCSC|ucsc)
		echo "Sorry.  UCSC not implemented!" >&2
		exit 1
		RefPath="/STORAGE/REFGENOME/Homo_sapiens/UCSC/HG18.fa"
		DbSnpPath="/STORAGE/REFGENOME/Homo_sapiens/UCSC/dbsnp_129_hg18.rod"
		;;
	HG19|hg19)
		echo "Sorry.  HG19 not implemented!" >&2
		exit 1
		RefPath="/STORAGE/REFGENOME/Homo_sapiens/UCSC/HG19/HG19.fa"
		;;
	1KG|1kg)
		RefArchive="human_g1k_v37.fasta.tar"
		RefPath="/STORAGE/REFGENOME/Homo_sapiens/1KG/human_g1k_v37.fasta"
		DbSnpPath="/STORAGE/REFGENOME/Homo_sapiens/1KG/dbsnp_130_b37.rod"
		;;
	*)
		usage_error "Unknown genome name ${Genome}"
	esac
}

function hdfs_stat() {
  if [ $# != 1 ]; then
		echo -e "Error using hdfs_stat.  Got $# arguments\n$@" >&2
		exit 1
	fi
	${Hadoop} dfs -stat "${1}" >/dev/null 2>&1
}

function get_hadoop_log() {
	# parameters:
	# 1. job output dir
	# 2. local destination
	if [ $# -ne 2 ]; then
		error "get_hadoop_log called with $# arguments (expected 2)"
	fi
	${Hadoop} dfs -get "${1}/_logs/history" "${2}"
}

function check_success_from_log() {
	log_dir="${1}"
	log_file="$(find "${log_dir}" ! -name '*.xml' -a -type f)"
	"${WorkflowRoot}/check_success_from_log.py"  "${log_file}"
}

######### Input variables set by scan_args ####
# HdfsInputDir or InputFiles
# OutputDir
# Genome
# Qvalue
# BaseQualities

#################### "main" ######################

check_seal
check_hadoop
scan_args "$@"
get_reference "${Genome}"

LanesSuffix=$(echo ${Lanes} | sed -e 's/,/./g')

HdfsOutput="$(basename ${OutputDir})-${LanesSuffix}"

# does the HdfsOutput directory already exist?
if hdfs_stat ${HdfsOutput}; then
	if [ "${ReRun}" == "true" ]; then
		echo "Warning.  HDFS output path ${HdfsOutput} exists.  Reusing it" >&2
	else
		usage_error "HDFS output path ${HdfsOutput} already exists.  Please remove it, change output path, or re-use it with --rerun."
	fi
fi

##### Make base for output file name before writing anything, just in case there are any errors in the input
if [[ "${Lanes}" == *,* ]]; then # if we have multiple lanes
	# use brace expansion
	LanesPattern="s_{${Lanes}}_*_qseq.txt"
else
	LanesPattern="s_${Lanes}_*_qseq.txt"
fi

if [ -n "${LocalInputDir:-}" ]; then
	files="$(eval echo ${LocalInputDir}/${LanesPattern})"
else
	# Test valitidity of patterns.  If lanes are missing, hadoop dfs -ls will return
	# a non-zero exit status and print out an error, and bash will exit (because of set +errexit).
	${Hadoop} dfs -ls $(eval echo "${HdfsInputDir}/${LanesPattern}") >/dev/null
	files=$(${Hadoop} dfs -ls $(eval echo "${HdfsInputDir}/${LanesPattern}") | sed 1d | awk '{ print $8; }')
fi
log "$(echo ${files} | sed -e 's/\s\+/\n/g' | wc -l) input files"

OutputNameBase="$(echo ${files} | sed -e 's/\s\+/\n/g' | ${WorkflowRoot}/make_output_name.py "${LaneContent}" "$(basename ${OutputDir})")"
log "Output file basename: ${OutputNameBase}"
###################


## make output directory, if it doesn't already exist
if [ -d "${OutputDir}" ]; then
	log "reusing output directory ${OutputDir}"
else
	log "creating output directory:  ${OutputDir}"
	mkdir --parents "${OutputDir}"
fi
write_params "${@}" # write parameters file

# Create HDFS output path, if it doesn't already exist
if ! hdfs_stat "${HdfsOutput}"; then
	${Hadoop} dfs -mkdir "${HdfsOutput}"
	log "created HDFS ${HdfsOutput} directory"
fi

############################################################
# Copy data to hdfs, if necessary
############################################################
if [ -n "${LocalInputDir:-}" ]; then
	HdfsInputDir="${HdfsOutput}/input"
	if hdfs_stat "${HdfsInputDir}"; then
		error "${HdfsInputDir} already exists.  Can't copy data there."
	fi

	log "copying data in ${LocalInputDir} to HDFS path ${HdfsInputDir}"
	run_cmd "${Distcp}" -m $((${HadoopNumNodes} * 4)) ${files} "${HdfsInputDir}"
else
	log "Running from existing HDFS path ${HdfsInputDir}"
fi

############################################################
# PairReadsQSeq (PRQ)
# Pair QSeq reads, converting QSeq to PRQ format
############################################################
PrqOutputDir="${HdfsOutput}/prq-${LanesSuffix}"
LogDir="${OutputDir}/prq_log-${LanesSuffix}"
if [ "${ReRun}" == "true" ] ; then
	if check_success_from_log "${LogDir}"; then
		log "!!! PRQ already done.  Skipping step."
	else
		log "PRQ failed in previous run.  Restarting workflow from here"
		"${Hadoop}" dfs -rmr "${PrqOutputDir}" || true
		ReRun="false" # if ReRun was true, now that we're recomputing a step we're not re-using anything else
	fi
elif hdfs_stat "${PrqOutputDir}"; then
	error "${PrqOutputDir} already exists.  Please remove it or specify --rerun"
fi
if [ "${ReRun}" != "true" ]; then
	log "Running PRQ"
	ReRun="false" # if ReRun was true, now that we're recomputing a step we're not re-using anything else
	rm -rf "${LogDir}"
	run_cmd "${Prq}" -D bl.prq.min-bases-per-read=${KnownBasesPerRead} "${HdfsInputDir}" "${PrqOutputDir}"
	get_hadoop_log "${PrqOutputDir}" "${LogDir}"
fi

############################################################
# Seqal
# BWA aln, BWA sampe, MarkDuplicates
############################################################
SeqalOutputDir="${HdfsOutput}/seqal-${LanesSuffix}"
LogDir="${OutputDir}/seqal_log-${LanesSuffix}"
if [ "${ReRun}" == "true" ]; then
	if check_success_from_log "${LogDir}"; then
		log "!!! Seqal already done.  Skipping step"
	else
		log "Seqal failed in previous run.  Restarting workflow from here"
		"${Hadoop}" dfs -rmr "${SeqalOutputDir}" || true
		ReRun="false" # if ReRun was true, now that we're recomputing a step we're not re-using anything else
	fi
elif hdfs_stat "${SeqalOutputDir}"; then
	error "${SeqalOutputDir} already exists.  Please remove it or specify --rerun"
fi
if [ "${ReRun}" != "true" ]; then
	log "Running Seqal"
	rm -rf "${LogDir}"
	run_cmd "${Seqal}" --trimq ${Qvalue} "${PrqOutputDir}" "${SeqalOutputDir}" "${RefArchive}"
	get_hadoop_log "${SeqalOutputDir}" "${LogDir}"
	run_cmd ${Hadoop} dfs -rmr "${PrqOutputDir}" # delete to save space
fi

############################################################
# Sort
# sort seqal output by coordinate
############################################################
# The way the annotation file name is set below is pretty fragile.
# * The archive name must match the root name of the reference files
#    within it;
# * the symlink created by the distributed cache must be called "reference"
ReadSortOutputDir="${HdfsOutput}/sorted-${LanesSuffix}"
LogDir="${OutputDir}/readsort_log-${LanesSuffix}"
if [ "${ReRun}" == "true" ]; then
	if check_success_from_log "${LogDir}"; then
		log "!!! ReadSort already done.  Skipping step"
	else
		log "ReadSort failed in previous run.  Restarting workflow from here"
		"${Hadoop}" dfs -rmr "${ReadSortOutputDir}" || true
		ReRun="false" # if ReRun was true, now that we're recomputing a step we're not re-using anything else
	fi
elif hdfs_stat "${ReadSortOutputDir}"; then
	error "${ReadSortOutputDir} already exists.  Please remove it or specify --rerun"
fi
if [ "${ReRun}" != "true" ]; then
	log "Running ReadSort"
	archive_name=$(basename ${RefArchive})
	rm -rf "${LogDir}"
	run_cmd "${ReadSort}"  --annotations="file://${RefPath}.ann" "${SeqalOutputDir}" "${ReadSortOutputDir}"
	get_hadoop_log "${ReadSortOutputDir}" "${LogDir}"
	run_cmd ${Hadoop} dfs -rmr "${SeqalOutputDir}"
fi


############################################################
# merge sorted files, download from HDFS, and create bam
############################################################
MergeOutputFile="${OutputDir}/raw_merged-${LanesSuffix}.bam"
if [ -f "${MergeOutputFile}" ]; then
	if [ "${ReRun}" == "true" ]; then
		log "!!! Reusing merge_alignments output in ${MergeOutputFile}"
	else
		error "${MergeOutputFile} already exists.  Please remove it or specify --rerun"
	fi
else
	log "running merge_alignments"
	ReRun="false" # if ReRun was true, now that we're recomputing a step we're not re-using anything else
	run_cmd "${MergedSorted} --annotations=file://${RefPath}.ann ${ReadSortOutputDir} | ${Samtools} view -bST  ${RefPath}.fai /dev/stdin -o ${MergeOutputFile}"
	run_cmd ${Hadoop} dfs -rmr "${ReadSortOutputDir}"
fi

########################################################################
#     Samtools index                                                   #
#     input: "${OutputDir}/merged.bam                                  #
#    output: "${OutputDir}/merged.bam.bai                              #
########################################################################
IndexOutput="${MergeOutputFile}.bai"
if [ ! -f "${IndexOutput}" -o "${ReRun}" != "true" ]; then
	log "running samtools index"
	run_cmd "${Samtools}" index "${MergeOutputFile}"
else
	log "!!! Keeping existing index ${IndexOutput}"
fi

########################################################################
#     GATK calculate recalibration table                               #
#     input: "${OutputDir}/merged.bam                                   #
#    output: "${OutputDir}/recal_table.csv                             #
########################################################################
# <+customize+>
# The entire command given to qsub needs to be customized.
RecalDataFile="${OutputDir}/recal_table-${LanesSuffix}.csv"
if [ ! -f "${RecalDataFile}" -o "${ReRun}" != "true" ]; then
	log "running qsub GATK_table_simple.  Output file: ${RecalDataFile}"
	A=$(qsub ${Queue} -wd "${OutputDir}" -N "GATK-table-${LanesSuffix}" -pe genoma.vl.2t 2 -b y -M ${MailRecipient%%,*} -m beas "module load jdk1.6.0_22;"  "${WorkflowRoot}/GATK_table_simple.sh" "${MergeOutputFile}" "${RecalDataFile}" "${RefPath}" "${DbSnpPath}" "${OutputDir}/commands-${LanesSuffix}")
	job_id=`echo ${A} | awk '{print $3}'`

	log "GATK_table_simple job id: ${job_id}"
	log "Waiting..."
	wait_for_sge_job $job_id
	exit_status=$?
	log "GATK_table_simple exited"
	if [ "${exit_status}" != 0 ]; then
		error "GATK_table_simple exited with exit code ${exit_status}"
	fi
	unset A
	unset job_id
else
	log "!!! Reusing GATK recal_table in ${RecalDataFile}"
fi

########################################################################
#     GATK recalibrate bam                                             #
#     input: "${OutputDir}/{merged.bam,recal_table.csv}                  #
#    output: "${OutputDir}/recal_merged.csv                             #
########################################################################
# <+customize+>
# The entire command given to qsub needs to be customized.
RecalibratedBam="${OutputDir}/recal_${OutputNameBase}.bam"
if [ ! -f "${RecalibratedBam}" -o "${ReRun}" != "true" ]; then
	escaped_output_name="${RecalibratedBam//#/\\#}" # escape hash marks
	log "qsub GATK_recabBAM_simple.  Output file:  ${RecalibratedBam}"
	A=$(qsub ${Queue} -wd "${OutputDir}" -N "GATKrecab-${LanesSuffix}" -pe genoma.vl.2t 2 -b y -M ${MailRecipient%%,*} -m beas "module load jdk1.6.0_22;"  "${WorkflowRoot}/GATK_recabBAM_simple.sh" "${MergeOutputFile}" "${escaped_output_name}" "${RecalDataFile}" "${RefPath}"  "${OutputDir}/commands-${LanesSuffix}")
	job_id=`echo ${A} | awk '{print $3}'`

	log "GATK_recabBAM_simple job id: ${job_id}"
	log "Waiting..."
	wait_for_sge_job $job_id
	exit_status=$?
	log "GATK_recabBAM_simple exited"
	if [ "${exit_status}" != 0 ]; then
		error "GATK_recabBAM_simple exited with exit code ${exit_status}"
	fi
	unset A
	unset job_id
	# rm -f "${OutputDir}/raw_merged.bam"
else
	log "!!! Reusing existing recalibrated bam ${RecalibratedBam}"
fi

log "sending mail"
send_end_notice
log "finished"

# end
