#!/bin/sh
# Script to stop glue job from execution. This script takes three parameters. Job name, job run id and aws region.
#-------------------------------------------------------------------------------
#                      **History**
#Date          Version No    Change                             Developer
#-------       ----------    ------                             ---------
#07/25/2022       0.1        Initial Version                    Rajalakshmi R
#******************************************************************************/
echo "Executing script: stop_glue_job.sh"

# Local variables
NUM_ARG=$#
REQRD_ARGUMENT_COUNT=3
COMMON_LIB="common.lib.sh"

####Following can't be in common.lib, as need full path here for sourcing.
export ARG_SCRIPT="$(which $0)"
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

Log_Msg $C_INFO "terminating Script: $ARG_SCRIPT"

#Parameters required 
JOB_NAME=$1
JOB_RUN_ID=$2
g_AWS_REGION=$3

echo "GLUE JOB_NAME = ${JOB_NAME}"
echo "GLUE JOB_RUN_ID = ${JOB_RUN_ID}"
echo "GLUE g_AWS_REGION = ${g_AWS_REGION}"
Log_Msg $C_INFO "GLUE JOB_NAME = ${JOB_NAME}"
Log_Msg $C_INFO "GLUE JOB_RUN_ID = ${JOB_RUN_ID}"
Log_Msg $C_INFO "GLUE g_AWS_REGION = ${g_AWS_REGION}"

TERMINATE_RUN(){
Log_Msg $C_INFO "Getting job run details"

#Command to stop the glue job
aws glue batch-stop-job-run --job-name "${JOB_NAME}" --job-run-id ${JOB_RUN_ID} --region ${g_AWS_REGION}
Log_Msg $C_INFO "COMMAND: aws glue batch-stop-job-run --job-name "${JOB_NAME}" --job-run-id ${JOB_RUN_ID} --region ${g_AWS_REGION}"

sleep 5

Log_Msg $C_INFO "COMMAND: aws glue get-job-run --region ${g_AWS_REGION} --job-name ${JOB_NAME} --run-id ${JOB_RUN_ID}"
while STATE="$(aws glue get-job-run --region ${g_AWS_REGION} --job-name ${JOB_NAME} --run-id ${JOB_RUN_ID} --output text --query 'JobRun.{state:JobRunState}')"; test "${STATE}" = "STOPPING" -o -z "${STATE}"
do
  sleep 5; echo -n '.'; Log_Msg $C_INFO "STATE=$STATE"
done; echo ""

Log_Msg $C_INFO "After while: STATE=$STATE."

sleep 5
#Validating the status of glue job after applying stop command
aws glue get-job-run --region ${g_AWS_REGION} --job-name ${JOB_NAME} --run-id ${JOB_RUN_ID}
Log_Msg $C_INFO "aws glue get-job-run --region ${g_AWS_REGION} --job-name ${JOB_NAME} --run-id ${JOB_RUN_ID}"

if [ $STATE != "STOPPED" ] ; then
  exit
fi

RC=$?
   if [ $RC -ne 0 ]; then
	  echo "[Terminating Glue Job] - - [ FAILED ]"
	  echo "Glue Job - Error code: $RC"
	  exit $RC
   fi
Log_Msg $C_INFO "Successfully Terminated Glue Job"
}
###########################################################
# Main Part
##########################################################
TERMINATE_RUN

exit
