#!/bin/sh
#####################################################################
# Script:  hua_stg_holding.sh
# Project: Unicorn - AWS Glue Migration
# Created: 05-OCT-2020
# Developer: UDAY V
#
# Usage:   hua_stg_holding.sh date env record_type client_number aws_region
#
#================================================================
#  Change Revision
#     YYYY/MM/DD : <developer_name> : <Changes Made> : Bug no
#================================================================
#####################################################################

#Script/job name:
JOB_NAME="hua_stg_holding"
echo "Executing script: ${JOB_NAME}.sh"

# Local variables
NUM_ARG=$#
REQRD_ARGUMENT_COUNT=5
export EXEC_SCRIPT=$JOB_NAME
COMMON_LIB="common.lib.sh"
GLUE_JOB_TRIGGER_SCRIPT="start_glue_job.sh"

####Following can't be in common.lib, as need full path here for sourcing.
export ARG_SCRIPT="$(which $0)"
##01-Nov-2019: Instead of APP_BIN_DIR and APP_LOG_DIR, use env variables BATCH_GLUE, LOGS_GLUE
echo "BATCH_GLUE folder: ${BATCH_GLUE}"

#Check whether common.lib.sh is available, and source it.
if [ ! -f "${BATCH_GLUE}/${COMMON_LIB}" ] ; then
    echo "ERROR: $COMMON_LIB not present or not accessible!! Exiting..."
    exit 1
fi
source ${BATCH_GLUE}/${COMMON_LIB}

export RUN_SCRIPT=`basename $ARG_SCRIPT`
export g_LOG_FILE="${LOGS_GLUE}/${RUN_SCRIPT}_`date ${DATE_FORMAT}`.log"
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
      USAGE:   ${RUN_SCRIPT} batch-date environment record_type client_Number aws_region \\
      EXAMPLE: ${RUN_SCRIPT} YYYY-MM-DD dev record_type client_num us-east-1 \\
      Exiting due to incorrect number of arguments."
Validate_Argument_Count $NUM_ARG $REQRD_ARGUMENT_COUNT "$g_MSG"

BATCH_DATE=$1
ENV=$2
RECORD_TYPE=$3
CLIENT_NUMBER=$4
AWS_REGION=$5

g_MSG="BATCH_DATE=${BATCH_DATE}, ENV=${ENV}, RECORD_TYPE=${RECORD_TYPE}, CLIENT_NUMBER=${CLIENT_NUMBER}, AWS_REGION=${AWS_REGION}"
echo "$g_MSG"
Log_Msg $C_INFO "$g_MSG"

##Need to append "_br_unipru" and "$ENV" to JOB_NAME, to help understand diff env. of job and log msgs.
JOB_NAME="${JOB_NAME}${C_JOB_POSTFIX}${ENV}"
g_MSG="JOB_NAME= ${JOB_NAME}"
echo "$g_MSG"
Log_Msg $C_INFO "$g_MSG"

#Create Arguments for Glue Job.
JOB_ARGUMENTS="--BATCH_DATE=\"$BATCH_DATE\",--ENV=\"$ENV\",--RECORD_TYPE=\"$RECORD_TYPE\",--CLIENT_NUMBER=\"$CLIENT_NUMBER\""
g_MSG="JOB_ARGUMENTS= ${JOB_ARGUMENTS}"
echo "$g_MSG"
Log_Msg $C_INFO "$g_MSG"

Log_Msg $C_INFO "Glue Job Trigger Script = ${GLUE_JOB_TRIGGER_SCRIPT}"

## Check if the script to be executed exists
g_MSG="do not have either file or permission: ${GLUE_JOB_TRIGGER_SCRIPT}, to execute, so exiting."
if [ ! -f "${GLUE_JOB_TRIGGER_SCRIPT}" ] ; then
    echo "ERROR: $g_MSG"
    Log_Msg $C_ERROR "$g_MSG"
    exit $C_FAILURE
fi

##Call script to create and execute a trigger for this job. ####Not returning correct status!!
#g_UNIX_CMD="./${GLUE_JOB_TRIGGER_SCRIPT} ${JOB_NAME} ${JOB_ARGUMENTS}"
#UNIX_Cmd_Exec
Log_Msg $C_INFO "Executing: ./${GLUE_JOB_TRIGGER_SCRIPT} ${JOB_NAME} ${JOB_ARGUMENTS} ${AWS_REGION}"
./${GLUE_JOB_TRIGGER_SCRIPT} ${JOB_NAME} ${JOB_ARGUMENTS} ${AWS_REGION}

l_RETURN_STATUS=$?
echo "${GLUE_JOB_TRIGGER_SCRIPT} l_RETURN_STATUS: ${l_RETURN_STATUS}"
if [ ${l_RETURN_STATUS} -ne 0 ] ; then
  g_MSG="ERROR in creating or executing trigger. Please check log of AWS GLUE or UC4 job."
  echo "$g_MSG"
  Log_Msg $C_ERROR "$g_MSG"
  exit ${C_FAILURE}
fi

#g_MSG="${GLUE_JOB_TRIGGER_SCRIPT}"
#Print_Status $l_RETURN_STATUS "$g_MSG"
g_MSG="Completed execution of script: ${EXEC_SCRIPT}."
echo "$g_MSG"
Log_Msg $C_INFO "$g_MSG"
