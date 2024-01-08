import json
import boto3
import tarfile
import os
import tempfile
import shutil
from os import walk

s3_client = boto3.client('s3')
sdlc = os.environ['ENV']
alias1 = os.environ['ALIAS']
BUCKET_NAME = 'br-unipru'+sdlc+'-unipru'+alias1+'-app-us-east-1-s3'              
PREFIX = os.environ['DEPLOY_PREFIX']
PREFIX_GLUE_ARTIFACT = os.environ['GLUE_ARTIFACT_PREFIX']
LOCAL_FILE_NAME = '/tmp/PM_GLUE_ARTIFACT.tar'
DEPLOY_DIR = 'glue_app'

def glue_deployment(event, context):
    s3 = boto3.client('s3')
    s3.download_file(BUCKET_NAME, PREFIX_GLUE_ARTIFACT, LOCAL_FILE_NAME)
    my_tar = tarfile.open('/tmp/PM_GLUE_ARTIFACT.tar')
    my_tar.extractall('/tmp/')
    my_tar.close()
    os.chdir('/tmp')
    isdir1 = os.path.isdir(DEPLOY_DIR)
    if isdir1 == True:
        shutil.rmtree(DEPLOY_DIR, ignore_errors=True)
        
    os.rename("Glue_Artifact", DEPLOY_DIR)
    mypath = DEPLOY_DIR
    
    filelist = []
    for root, dirs, files in os.walk(mypath):
        for file in files:
            filelist.append(os.path.join(root,file))

    for name in filelist:
        print(name)
        s3_client.upload_file(name,BUCKET_NAME ,PREFIX+name)
