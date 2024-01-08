#!/bin/sh
#####################################################################
# Script:  start_glue_job.sh
# Project: Unicorn - AWS Glue Migration
# Created: 17-Oct-2019
# Developer: Sanjay Pancholi
#
# Usage:   start_glue_job.sh job_name job_arguments aws_region
#
#====================================================================
#  Change Revision
#     YYYY/MM/DD : <developer_name> : <Changes Made> : Bug no
#     2019/10/30 : pancholis : enhanced for usage of common.lib : NA (Enhancement)
#     2019/11/01 : pancholis : moved common stuff to common.lib : NA (Enhancement)
#     2019/11/13 : pancholis : added 3rd param for AWS region   : NA (Enhancement)
#     2019/11/13 : pancholis : updated to use "start-job-run"   : NA (Enhancement)
#                              instead of using Glue Triggers
#     2019/12/02 : pancholis : check job status using job run-id : NA (Enhancement)
#     2019/12/03 : pancholis : Extra logging, removed trigger code : NA (Enhancement)
#     2019/12/09 : pancholis : Consider FAILED state for all except SUCCEEDED : NA (Enhancement)
#     2020/04/21 : pancholis : echo $JOB_STATE to see Glue job status in UC4 : NA (Enhancement)
#     2020/07/16 : pancholis : adding spark param as per AWS team for Glue 2.0 : NA
#     2022/08/17 : Uday V    : Removed --conf parameter for Glue 3.0 : NA
#====================================================================
#####################################################################

echo "Executing script: start_glue_job.sh"

# Local variables
NUM_ARG=$#
REQRD_ARGUMENT_COUNT=3
COMMON_LIB="common.lib.sh"

####Following can't be in common.lib, as need full path here for sourcing.
export ARG_SCRIPT="$(which $0)"
##01-Nov-2019: Instead of APP_BIN_DIR and APP_LOG_DIR, use env variables BATCH_GLUE, LOGS_GLUE
echo "BATCH_GLUE folder: ${BATCH_GLUE}"

#Check whether common.lib.sh is available, and source it.
if [ ! -f "${BATCH_GLUE}/${COMMON_LIB}" ] ; then
    echo "ERROR: $COMMON_LIB not present or not accessible!! Exiting..."
    exit 1
fi
#source $COMMON_LIB
source ${BATCH_GLUE}/${COMMON_LIB}

PID=$$
export RUN_SCRIPT=`basename $ARG_SCRIPT`
export g_LOG_FILE="${LOGS_GLUE}/${RUN_SCRIPT}_`date ${DATE_FORMAT}-${PID}`.log"
echo "LOG_FILE: $g_LOG_FILE"

## Log file: check and rename existing log file (from previous run).
if [ -f $g_LOG_FILE ] ; then
    #Move the previously run batches log file. This is applicable only in case of re-run.
    #There is a possibility of colliding, but chances are very rare in PROD.
    PID=$$
    mv $g_LOG_FILE ${g_LOG_FILE}_$PID
fi

Log_Msg $C_INFO "Starting Script: $ARG_SCRIPT"

## Check number of Arguments, and exit if not as expected.
g_MSG="Required number of Arguments: ${REQRD_ARGUMENT_COUNT} and Not ${NUM_ARG} to ${RUN_SCRIPT} \\
      $RUN_SCRIPT <job_name> <job_arguments> <aws_region> \\
      EXAMPLE: start_glue_job.sh conv_posval_distinct_stub_rec {--BATCH_DATE='2019-01-01',--ENV='dev'} us-east-1 \\
      Exiting due to incorrect number of arguments."
Validate_Argument_Count $NUM_ARG $REQRD_ARGUMENT_COUNT "$g_MSG"

JOB_NAME=$1
JOB_ARGU=$2
g_AWS_REGION=$3

JOB_ARGU="${JOB_ARGU}"

echo "GLUE JOB_NAME = ${JOB_NAME}"
echo "GLUE JOB_ARGU = ${JOB_ARGU}"
echo "GLUE g_AWS_REGION = ${g_AWS_REGION}"
Log_Msg $C_INFO "GLUE JOB_NAME = ${JOB_NAME}"
Log_Msg $C_INFO "GLUE JOB_ARGU = ${JOB_ARGU}"
Log_Msg $C_INFO "GLUE g_AWS_REGION = ${g_AWS_REGION}"

#[02-Dec-2019] Need to get the job-run-id, so can't call this generic function!!
g_UNIX_CMD="aws glue start-job-run --region ${g_AWS_REGION} --job-name ${JOB_NAME} --arguments=${JOB_ARGU}"
#UNIX_Cmd_Exec
#ll_RETURN_STATUS=$?
Log_Msg $C_INFO "COMMAND: $g_UNIX_CMD"
ll_RETURN_STATUS=`$g_UNIX_CMD `

g_MSG="start-glue-job-run status: $ll_RETURN_STATUS"
Log_Msg $C_INFO "$g_MSG"

#[02-Dec-2019] Check whether the Glue command returned JobRunId or not, to check success!
JOB_STATUS=`echo $ll_RETURN_STATUS | grep JobRunId `
if [ -z "${JOB_STATUS}" ] ; then
    Log_Msg $C_ERROR "Couldn't start Glue Job: $JOB_NAME."
    exit 1
fi

#[02-Dec-2019] Get Glue Job Id and check it's status instead of the latest Glue Job.
JOB_RUN_ID=`echo $ll_RETURN_STATUS | grep JobRunId | gawk -F: '{ print $2 }' | gawk -F\" '{ print $2 }'`
g_MSG="Job Run Id: $JOB_RUN_ID"
Log_Msg $C_INFO "$g_MSG"

##Check status of the GLUE job and wait till it ends. Capture the job status and convert to 0 or 1.
g_MSG="Started job... checking its status."
echo "$g_MSG"
Log_Msg $C_INFO "$g_MSG"

#while STATE="$(aws glue get-job-runs --region ${g_AWS_REGION} --job-name ${JOB_NAME} --output text --query 'JobRuns[0].{state:JobRunState}')"; test "$STATE" = "RUNNING"

Log_Msg $C_INFO "COMMAND: aws glue get-job-run --region ${g_AWS_REGION} --job-name ${JOB_NAME} --run-id ${JOB_RUN_ID}"
while STATE="$(aws glue get-job-run --region ${g_AWS_REGION} --job-name ${JOB_NAME} --run-id ${JOB_RUN_ID} --output text --query 'JobRun.{state:JobRunState}')"; test "${STATE}" = "RUNNING" -o -z "${STATE}"
do
  sleep 5; echo -n '.'; Log_Msg $C_INFO "STATE=$STATE"
done; echo ""

Log_Msg $C_INFO "After while: STATE=$STATE."

ll_RETURN_STATUS=1
if [ $STATE == "SUCCEEDED" ] ; then
  ll_RETURN_STATUS=0
fi

#[03-Dec-2019] Extra logging to check STATE issue!!
sleep 5
JOB_STATE=`aws glue get-job-run --region ${g_AWS_REGION} --job-name ${JOB_NAME} --run-id ${JOB_RUN_ID} `
Log_Msg $C_INFO "JOB_STATE: $JOB_STATE "
if [ $STATE != "SUCCEEDED" ] ; then
  echo "JOB_STATE: $JOB_STATE "
fi

g_MSG="aws glue job status: job-state=${STATE}, ll_RETURN_STATUS=${ll_RETURN_STATUS}"
Print_Status $ll_RETURN_STATUS "$g_MSG"

g_MSG="Completed start_glue_job.sh!!"
echo "$g_MSG"
Log_Msg $C_INFO "$g_MSG"
