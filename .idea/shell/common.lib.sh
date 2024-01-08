#!/bin/sh
#This script has common functions that will be used in start_glue_job and other scripts.
#---------------------------------------------------------------------------------------
#                     **History**
#Date         Version No   Change                                       Developer
#----------   ----------   -----------------------------------------    ---------------
#10/18/2019   1.0          Version copied from redshift's               Sanjay Pancholi
#                          common.lib.sh, v1.1, without SQL
#11/01/2019   1.1          Renamed & updated as discussed with Barath   Sanjay Pancholi
#                          Global variables, Validate Arg function
#11/13/2019   1.2          Added global variable for AWS Region         Sanjay Pancholi
#**************************************************************************************/

################################################################
# Global Env. Variables & Constants - used in all caller scripts
################################################################
# Global variables defined and accessed within the SHELL scripts.
export g_MSG=
export g_UNIX_CMD=
export g_PARAM=
export g_LOG_FILE_NAME_PATTERN=
export g_LOG_FILE=
export g_AWS_REGION=

# Constants
export C_JOB_POSTFIX="_br_unipru"
export C_ERROR="ERROR"
export C_INFO="INFO"
export C_SUCCESS=0
export C_FAILURE=1
export C_SKIP=10
export DATE_FORMAT="+%Y%m%d-%H%M%S"

export UNIX_USR=`whoami`
export HOST_SHORT_NM=`hostname`

####Following need to be in caller script, as sourcing needs full-path.
#export ARG_SCRIPT="$(which $0)"
#export APP_BIN_DIR=`dirname $ARG_SCRIPT`
#export APP_LOG_DIR="${APP_BIN_DIR}/../../logs/glue"
#
#export RUN_SCRIPT=`basename $ARG_SCRIPT`
#export g_LOG_FILE_NAME_PATTERN="aws_glue_${RUN_SCRIPT}"
#export g_LOG_FILE="${APP_LOG_DIR}/aws_glue_${RUN_SCRIPT}_`date ${DATE_FORMAT}`.log"


################################################################
# Function: Print_Status
# Description: Print status and exit.
################################################################
Print_Status() {
#RED="\033[31m"
#GREEN="\033[32m"
#YELLOW="\033[33m"
#NUETRAL="\033[m"

STATUS=$1
TEXT="$2"
if [ $STATUS -eq 0 ] ; then
    echo -e "INFO: $TEXT - [ SUCCESS ]"
    Log_Msg $C_INFO "$TEXT - [ SUCCESS ]"
elif [ $STATUS -eq 10 ] ; then
    echo -e "INFO: $TEXT - [ SKIPPED ]"
    Log_Msg $C_INFO "$TEXT - [ SKIPPED ]"
else
    echo -e "ERROR: $TEXT - [ FAILED ]"
    Log_Msg $C_ERROR "$TEXT - [ FAILED ]"
    exit $C_FAILURE
#    return 1
fi
return $C_SUCCESS

}

################################################################
# Function: Print_Status_NoExit
# Description: Print status and return. Do not exit.
################################################################
Print_Status_NoExit() {

STATUS=$1
TEXT="$2"
if [ $STATUS -eq 0 ] ; then
    echo -e "$TEXT - [ SUCCESS ]"
    Log_Msg $C_INFO "$TEXT - [ SUCCESS ]"
elif [ $STATUS -eq 10 ] ; then
    echo -e "$TEXT - [ SKIPPED ]"
    Log_Msg $C_INFO "$TEXT - [ SKIPPED ]"
else
    echo -e "$TEXT - [ FAILED ]"
    Log_Msg $C_ERROR "$TEXT - [ FAILED ]"
#   exit 1
    return $C_FAILURE
fi
return $C_SUCCESS

}

################################################################
# Function: Log_Msg
# Description: Log the message in log file.
################################################################
Log_Msg(){

if [ $# -eq 2 ] ; then
    g_MSG="$2"
fi

g_MSG_TYPE=$1

echo "`date "+%Y-%m-%d %H-%M-%S.%s"`: $g_MSG_TYPE : $g_MSG" >>$g_LOG_FILE
l_STATUS=$?
if [ $l_STATUS -ne 0 ] ; then
    echo " $C_ERROR an error occurred while writing to Log file : $g_LOG_FILE \
    Disk might be full or permission issue"
    return $C_FAILURE
fi

return $C_SUCCESS

}


################################################################
# Function: UNIX_Cmd_Exec
# Description: Execute the UNIX Command.
################################################################
UNIX_Cmd_Exec() {

if [ $# -eq 1 ] ; then
    g_UNIX_CMD="$1"
fi

echo "COMMAND: ${g_UNIX_CMD}"
Log_Msg $C_INFO "COMMAND: $g_UNIX_CMD"

g_RET_VAL=`$g_UNIX_CMD 2>> ${g_LOG_FILE} 2>&1`
l_STATUS=$?

#echo execution status: $l_STATUS
#if [ $l_STATUS == $C_SUCCESS ] ; then
if [ $l_STATUS -eq 0 ] ; then
    Log_Msg $C_INFO "g_RET_VAL: $g_RET_VAL"
    return $C_SUCCESS
else
    Log_Msg $C_ERROR " Above Unix command FAILED "
    return $C_FAILURE
fi

}

################################################################
# Function: Status_Logger
# Description: Log the status of execution
################################################################
Status_Logger() {

if [ $# -ne 2 ] ; then
  Log_Msg $C_ERROR "Arguments required to log status are 2, passed were $# , unable to log the Status in $g_STATUS_TRACKER_FILE, so exiting"
  echo "$C_ERROR: Arguments required to log status are 2, passed were $# , unable to log the Status in $g_STATUS_TRACKER_FILE, so exiting"
  exit 1
fi

END_TM=`date "+%Y-%m-%d %H-%M-%S.%s"`
echo "$1|$2|$END_TM|SUCCESS" >> $g_STATUS_TRACKER_FILE
l_STATUS=$?
if [ $l_STATUS -eq 0 ] ; then
    Log_Msg $C_INFO "Logged [$1] status: $g_STATUS_TRACKER_FILE"
    return $C_SUCCESS
else
    Log_Msg $C_ERROR "Logging [$1] status has FAILED , so exiting "
    echo "$C_ERROR: Logging [$1] status to $g_STATUS_TRACKER_FILE"
    exit 1
fi

}

################################################################
# Function: Validate_Argument_Count
# Description: Compare first and second arguments, and log message & exit if not matching.
################################################################
Validate_Argument_Count() {

REQUIRED=$1
GIVEN=$2

if [ -z "$g_MSG" ] ; then
    g_MSG="Arguments count didn't match REQUIRED ${REQUIRED}, GIVEN:${GIVEN} "
fi

if [ $REQUIRED -ne $GIVEN ] ; then
    echo "$C_ERROR : $g_MSG"
    Log_Msg $C_ERROR "$g_MSG"
    exit 1
fi

}

################################################################
# Function: Check_NOVALUE
# Description: Check value of the passed argument
################################################################
Check_NOVALUE() {

VAR="${1}"

echo "${VAR}" | grep -q "NOVALUE"
RC=$?

if [ $RC -ne 0 ]; then
    TMP=`echo "${VAR}" | sed -e "s/~#~#~/ /g"`
    g_PARAM="${TMP}"
else
    g_PARAM=null
fi

}

################################################################
# Function: Set_NOVALUE
# Description: Set value of the passed argument
################################################################
Set_NOVALUE() {

VAR="${1}"

NOVALUE='NOVALUE'

if [ "${VAR}" = null ]; then
   g_PARAM="${NOVALUE}"
else
   g_PARAM="${VAR}"
fi

}
