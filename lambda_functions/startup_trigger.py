import json
import boto3
import os

#getting env variables

sdlc = os.environ['ENV']
region = os.environ['REGION']

#connecting to aws services
ssm_client = boto3.client('ssm',region_name=region)
sfn_client = boto3.client('stepfunctions',region_name='us-east-1' )

#required ssm parameters names
ssm_stepfn_execution_arn = '/unipru/'+sdlc+'/asg/stepfunction_infra_startup_execution_arn'
execution_confirmation = '/unipru/'+sdlc+'/ASG/execute_infra_startup'

def startup_trigger(event, context):
    # TODO implement
    
    #Reading permission to execte the lambda from parameter store
    try:
        execute = ssm_client.get_parameter(Name = execution_confirmation)['Parameter']['Value']
    except:
        execute = 'true'
    
    #Checking permission to execute the lambda
    if(execute.lower() != 'true'):
        return {
            'statusCode': 200,
            'body': json.dumps('Automatic startup is off!')
        }
        
    #Reading account id
    account_id=boto3.client('sts').get_caller_identity().get('Account') 
    
    # Declaring stepfunction arn
    state_machine_arn = 'arn:aws:states:'+region+':'+str(account_id)+':stateMachine:br-unipru'+sdlc+'-gemfire-state-machine-Restart_Gemfire'
    
    #Getting the list of currently running executions
    response = sfn_client.list_executions(stateMachineArn=state_machine_arn, statusFilter='RUNNING',maxResults=1)
    
    print(response)
    
    #checking if any running executions if stepfunction is in running state aborting the lambda
    for execution in response['executions']:
        if(execution['status'] == 'RUNNING'):
            return {
                'statusCode': 200,
                'body': json.dumps('Step Function is already on running state')
            }
            
    #Starting the step function execution
    response=sfn_client.start_execution(stateMachineArn=state_machine_arn)
    
    # Writing the execution arn to parameter store
    ssm_client.put_parameter(Name=ssm_stepfn_execution_arn, Value=response['executionArn'], Type='String',Overwrite=True)

    return {
    'statusCode': 200,
    'body': json.dumps('successfully started stepfunction')
    } 