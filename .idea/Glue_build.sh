#!/bin/bash

#/******************************************************************************
# This script will build DB artifacts to a tar or a zip and promote to nexus
#-------------------------------------------------------------------------------
#                      **History**
#Date          Version No    Change                             Developer
#-------       ----------    ------                             ---------
#5/07/2019        0.1       Created                             Barath R
#******************************************************************************/

#source version.properties

echo "Glue Build script started pwd is- `pwd`"
echo "ARTIFACT_NAME = ${ARTIFACT_NAME}"
#echo "DB_ARTIFACT_NAME = ${DB_ARTIFACT_NAME}"
echo "NEXUS_URL = ${NEXUS_URL}"
echo "DEPLOY_USER_ID = ${DEPLOY_USER_ID}"
echo "DBA_SCRIPT = ${DBA_SCRIPT}"
echo "DB_version = ${db_scripts_version}"

#Glue_ARTIFACT_NAME=Glue_${db_scripts_version}_${BUILD_NUMBER}_${GIT_NUM}.zip
WORKSPACE1="${WORKSPACE}/GLUE"

if [[ -z "${ARTIFACT_NAME}" ]]; then
    echo "ERROR: ARTIFACT_NAME is required to be set as an environment variable"
    
fi

rm -rf $ARTIFACT_NAME; RC=$?
if [[ $RC != 0 ]]; then
	echo "FAILURE! There was a problem deleting the artifact directory $ARTIFACT_NAME"
fi
  
echo "creating the artifact directories"
pwd

cp ${WORKSPACE}/CM/${CM_BUILD}.tar ${WORKSPACE1}/.

tar -xvf ${CM_BUILD}.tar

rm -rf ${CM_BUILD}.tar

cd ${CM_BUILD}

chmod 777 *

cd ${WORKSPACE1}

pwd


ls -ltr

if [[ $RC != 0 ]]; then
	echo "FAILURE! There was a problem creating the artifact directory"
	exit 1
fi

echo "completed creating artifact directories"
echo "Artifact Name:"$ARTIFACT_NAME

mkdir $ARTIFACT_NAME

cp -R ${WORKSPACE1}/configs $ARTIFACT_NAME/; RC=$?
if [[ $RC != 0 ]]; then
	echo "FAILURE! There was a problem copying configs dir"
	exit 1
fi

cp -R ${WORKSPACE1}/constant $ARTIFACT_NAME/; RC=$?
  if [[ $RC != 0 ]]; then
  	echo "FAILURE! There was a problem copying constant dir"
  	exit 1
  fi

cp -R ${WORKSPACE1}/jobs/ $ARTIFACT_NAME/; RC=$?
if [[ $RC != 0 ]]; then
	echo "FAILURE! There was a problem copying jobs dir"
	exit 1
fi

cp -R ${WORKSPACE1}/shell/ $ARTIFACT_NAME/; RC=$?
if [[ $RC != 0 ]]; then
	echo "FAILURE! There was a problem copying Shell dir"
	exit 1
fi

cp -R ${WORKSPACE1}/common/ $ARTIFACT_NAME/; RC=$?
if [[ $RC != 0 ]]; then
	echo "FAILURE! There was a problem copying Shell dir"
	exit 1
fi

cp -R ${WORKSPACE1}/utils/ $ARTIFACT_NAME/; RC=$?
if [[ $RC != 0 ]]; then
	echo "FAILURE! There was a problem copying utils dir"
	exit 1
fi

chmod -R 755 ${WORKSPACE1}/lambda_functions/*

chmod -R 755 ${WORKSPACE1}/lambda_functions/.libs_cffi_backend/libffi-806b1a9d.so.6.0.4

cd ${WORKSPACE1}/lambda_functions/

#find . -type f -print0 | xargs -0 dos2unix

cp .libs_cffi_backend/libffi-806b1a9d.so.6.0.4 .

zip -r lambda_functions.zip * .[^.]*

#zip -9 -y -r -q lambda_functions.zip * .[^.]*

cp lambda_functions.zip ../.

cd -

chmod 755 ${WORKSPACE1}/lambda_functions.zip

cp ${WORKSPACE1}/lambda_functions.zip $ARTIFACT_NAME/

cp ../CM/${CM_BUILD}/global/env.properties $ARTIFACT_NAME/configs/

cd ${WORKSPACE1}

chmod 755 setup.py

python3 setup.py bdist_egg

cp dist/*.egg $ARTIFACT_NAME/

cd $ARTIFACT_NAME

cp ../__init__.py .

zip -r dependencies.zip utils/ jobs/ constant/ common/ __init__.py

rm -rf __init__.py

cd ..

pwd

echo ${CM_BUILD}

GLUE_VERSION=`cat ${CM_BUILD}/version.properties | grep glue_version | awk -F '=' '{print $2}' | awk '{print $1}'`
GLUE_ARTIFACT_NAME=unipruGL_${GLUE_VERSION}_RC_${GIT_NUM}.tar

tar -cvf ${GLUE_ARTIFACT_NAME} $ARTIFACT_NAME
if [[ $RC != 0 ]]; then
	echo "FAILURE! There was a problem creating the artifact zip"
	
fi

echo "Successfully created: GLTar=$GLUE_ARTIFACT_NAME"
curl -u $1:$2 -s -o /dev/null --write-out "HTTPSTATUS:%{http_code}:SUTATSPTTH" --upload ${GLUE_ARTIFACT_NAME} https://artifacts.devops.bfsaws.net/artifactory/UNIPRU-TEST-DEV/${GLUE_ARTIFACT_NAME} > curl.log 2>&1
HTTP_STATUS=`(cat curl.log | grep "HTTPSTATUS:.*:SUTATSPTTH")` 
echo $HTTP_STATUS
if [ "$HTTP_STATUS" == "HTTPSTATUS:201:SUTATSPTTH"  ];then echo "Successfully uploaded GLUE ARTIFACT to Jfrog";else echo "Artifact upload failed for GLUE"; exit 6;fi
