import boto3
import os
import json

sdlc = os.environ['ENV']
region = os.environ['REGION']
ssm_current_batch_execution = '/unipru/'+sdlc+'/asg/batch_execution'

sfn_client = boto3.client('stepfunctions',region_name=region)
ssm_client = boto3.client('ssm',region_name=region)

def batch_status(event, context):
    try: 
        batch_execution=bool(ssmclient.get_parameter(Name=ssm_current_batch_execution)['Parameter']['Value'])
    except:
        batch_execution=False

    if batch_execution == True:
        #todo : write content to s3 bucket
        try:
            if event['taskToken'] != '':
                sfn_client.send_task_failure(taskToken=event['taskToken'], error='Batch execution', cause='lambda is failed as batch is executing')
        except:
            print('not from step function call')
        return {
        'statusCode': 400,
        'body': json.dumps('lambda is failed as batch is executing')
        }
    else :
        try:
            if event['taskToken'] != '':
                sfn_client.send_task_success(taskToken=event['taskToken'], output='Batch execution is complete')
        except:
            print('not from step function call')
        return {
        'statusCode': 200,
        'body': json.dumps('Batch execution is complete')
        }
