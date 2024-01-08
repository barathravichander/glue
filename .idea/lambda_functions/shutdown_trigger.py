import json
import boto3
import os

#Reading env variables
sdlc = os.environ['ENV']
region = os.environ['REGION']

# Making connection to aws services
ssm_client = boto3.client('ssm',region_name=region)
sfn_client = boto3.client('stepfunctions',region_name='us-east-1' )

# Declaring required ssm parameter names
ssm_stepfn_execution_arn = '/unipru/'+sdlc+'/ASG/stepfunction_infra_shutdown_execution_arn'
execution_confirmation = '/unipru/'+sdlc+'/ASG/execute_infra_shutdown'

def shutdown_trigger(event, context):
    # TODO implement
    
    # Reading exection permission from ssm parameter store
    try:
        execute = ssm_client.get_parameter(Name = execution_confirmation)['Parameter']['Value']
    except:
        execute = 'true'
    
    # Checking the permission to execute
    if(execute.lower() != 'true'):
        return {
            'statusCode': 200,
            'body': json.dumps('Automatic shutdown is off!')
        }
    
    #Reading aws account id
    account_id=boto3.client('sts').get_caller_identity().get('Account')
    
    # Declaring step function arn
    state_machine_arn = 'arn:aws:states:'+region+':'+str(account_id)+':stateMachine:br-unipru'+sdlc+'-gemfire-state-machine-Shutdown_Gemfire'
    
    #Getting the list of currently running execution of the stepfunction
    response = sfn_client.list_executions(stateMachineArn=state_machine_arn, statusFilter='RUNNING',maxResults=10)
    
    #Checking if step function has any running executions if any executions are running aborting the lambda execution
    for execution in response['executions']:
        if(execution['status'] == 'RUNNING'):
            return {
                'statusCode': 200,
                'body': json.dumps('Step Function is already on running state')
            }
    
    # Starting the execution of stepfunction
    response=sfn_client.start_execution(stateMachineArn=state_machine_arn)
    
    #Writing the execution arn to paramter st
    ssm_client.put_parameter(Name=ssm_stepfn_execution_arn, Value=response['executionArn'], Type='String',Overwrite=True)

    return {
    'statusCode': 200,
    'body': json.dumps('successfully started stepfunction')
    } 
