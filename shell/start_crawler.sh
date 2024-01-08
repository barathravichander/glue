#!/bin/sh
#####################################################################
# Script:  start_crawler.sh
# Project: Unicorn - AWS Glue Migration
# Created: 06-Jan-2020
# Developer: Sanjay Pancholi
#
# Usage:   start_crawler.sh crawler_name aws_region
#
#================================================================
#  Change Revision
#     YYYY/MM/DD : <developer_name> : <Changes Made> : Bug no
#================================================================
#####################################################################

echo "Executing script: start_crawler.sh"

# Local variables
NUM_ARG=$#
REQRD_ARGUMENT_COUNT=2
COMMON_LIB="common.lib.sh"

####Following can't be in common.lib, as need to set APP_BIN_DIR here for sourcing.
export ARG_SCRIPT="$(which $0)"
export APP_BIN_DIR=`dirname $ARG_SCRIPT`
export APP_LOG_DIR="${APP_BIN_DIR}/../../logs/glue"

echo "APP_BIN_DIR: ${APP_BIN_DIR}"

#Check whether common.lib.sh is available, and source it.
if [ ! -f "${APP_BIN_DIR}/${COMMON_LIB}" ] ; then
    echo "ERROR: $COMMON_LIB not present or not accessible!! Exiting..."
    exit 1
fi
source ${APP_BIN_DIR}/${COMMON_LIB}

export RUN_SCRIPT=`basename $ARG_SCRIPT`
export g_LOG_FILE="${APP_LOG_DIR}/${RUN_SCRIPT}_`date ${DATE_FORMAT}`.log"
echo "LOG_FILE: $g_LOG_FILE"

## Log file: check and rename existing log file (from previous run).
if [ -f $g_LOG_FILE ] ; then
    #Move the previously run batches log file. This is applicable only in case of re-run.
    #There is a possibility of colliding, but chances are very rare in PROD.
    PID=$$
    mv $g_LOG_FILE ${g_LOG_FILE}_$PID
fi

Log_Msg $C_INFO "Starting Script: $RUN_SCRIPT"

## Check number of Arguments, and exit if not as expected.
g_MSG="Required number of Arguments: ${REQRD_ARGUMENT_COUNT} and Not ${NUM_ARG} to ${RUN_SCRIPT} \\
      $RUN_SCRIPT <crawler_name> <aws region> \\
      EXAMPLE: $RUN_SCRIPT crawler_name us-east-1 \\
      Exiting due to incorrect number of arguments."
Validate_Argument_Count $NUM_ARG $REQRD_ARGUMENT_COUNT "$g_MSG"

CRAWLER_NAME=$1
AWS_REGION=$2
echo "GLUE CRAWLER_NAME = $CRAWLER_NAME"
echo "AWS_REGION = $AWS_REGION"
Log_Msg $C_INFO "GLUE CRAWLER_NAME = $CRAWLER_NAME"
Log_Msg $C_INFO "AWS_REGION = $AWS_REGION"

Log_Msg $C_INFO "COMMAND: aws glue get-crawler --name $CRAWLER_NAME --region $AWS_REGION --query 'Crawler.LastCrawl.Status' --output text "
CRAWLER_STATUS=`aws glue get-crawler --name $CRAWLER_NAME --region $AWS_REGION --query 'Crawler.LastCrawl.Status' --output text `
g_MSG="get-crawler: CRAWLER_STATUS= $CRAWLER_STATUS"
echo $g_MSG
Log_Msg $C_INFO "$g_MSG"

if [ -z $CRAWLER_STATUS ] ; then
        g_MSG="Crawler details not found. Exiting..."
        Print_Status 1 "$g_MSG"
fi

#Check Crawler's status and state, and run it if it is not RUNNING and is in READY state.
if [ $CRAWLER_STATUS != "RUNNING" ] ; then
        Log_Msg $C_INFO "Crawler $CRAWLER_NAME not running, so checking it's STATE to start..."

        Log_Msg $C_INFO "COMMAND: aws glue get-crawler --name $CRAWLER_NAME --region $AWS_REGION --query 'Crawler.State' --output text "
        CRAWLER_STATE=`aws glue get-crawler --name $CRAWLER_NAME --region $AWS_REGION --query 'Crawler.State' --output text `
        g_MSG="get-crawler: CRAWLER_STATE= $CRAWLER_STATE"
        echo $g_MSG
        Log_Msg $C_INFO "$g_MSG"

        if [ $CRAWLER_STATE != "READY" ] ; then
                g_MSG="Crawler not READY. STATE=$CRAWLER_STATE. Exiting..."
                Print_Status 1 "$g_MSG"
        fi

        Log_Msg $C_INFO "COMMAND: aws glue start-crawler --name $CRAWLER_NAME --region $AWS_REGION "
        CRAWLER_START_STATUS=`aws glue start-crawler --name $CRAWLER_NAME --region $AWS_REGION `

        ##"start-crawler" returns nothing if successful, otherwise an exception in JSON.
        if [ ! -z $CRAWLER_START_STATUS ] ; then
                g_MSG="start-crawler: CRAWLER_START_STATUS= $CRAWLER_START_STATUS"
                echo $g_MSG
                Log_Msg $C_INFO "$g_MSG"
                g_MSG="start-crawler failed, existing..."
                Print_Status 1 "$g_MSG"
        fi

        g_MSG="Started crawler $CRAWLER_NAME... checking its status."
        echo $g_MSG
        Log_Msg $C_INFO $g_MSG
fi

while STATE="$(aws glue get-crawler --name $CRAWLER_NAME --region $AWS_REGION --query 'Crawler.LastCrawl.Status' --output text)"; test "$STATE" = "RUNNING" -o -z "${STATE}"; do
  sleep 5; echo -n '.'; Log_Msg $C_INFO "STATUS=$STATE"
done; echo "Crawler STATUS= $STATE"

#Need to check Crawler's STATE as well, as the STOPPING takes time.
g_MSG="Crawler started... checking it's STATE now."
echo $g_MSG
Log_Msg $C_INFO $g_MSG
while STATE="$(aws glue get-crawler --name $CRAWLER_NAME --region $AWS_REGION --query 'Crawler.State' --output text)"; test "$STATE" = "RUNNING" -o "$STATE" = "STOPPING"; do
  sleep 5; echo -n '.'; Log_Msg $C_INFO "STATE=$STATE"
done; echo ""

Log_Msg $C_INFO "After while loop: Crawler STATE=$STATE."

l_RETURN_STATUS=1
if [ $STATE == "READY" ] ; then
  l_RETURN_STATUS=0
fi
g_MSG="aws glue get-crawler STATE=${STATE}, l_RETURN_STATUS=${l_RETURN_STATUS}"
Print_Status $l_RETURN_STATUS "$g_MSG"

CRAWLER_STATUS=`aws glue get-crawler --name $CRAWLER_NAME --region $AWS_REGION --query 'Crawler.LastCrawl.Status' --output text `
if [ $CRAWLER_STATUS != "SUCCEEDED" ] ; then
  l_RETURN_STATUS=1
  Err_Msg=`aws glue get-crawler --name $CRAWLER_NAME --region $AWS_REGION --query 'Crawler.LastCrawl.ErrorMessage' `
  Log_Msg $C_INFO "$Err_Msg"
fi
g_MSG="aws glue get-crawler: CRAWLER_STATUS= $CRAWLER_STATUS"
Print_Status $l_RETURN_STATUS "$g_MSG"

echo "Completed $RUN_SCRIPT!!"
Log_Msg $C_INFO "Completed $RUN_SCRIPT!!"
