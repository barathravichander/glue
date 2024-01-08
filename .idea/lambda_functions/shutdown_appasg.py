import boto3
import os
import json

sdlc = os.environ['ENV']        #environment parameters
region = os.environ['REGION']

ssm_ignore_asg_location = '/unipru/'+sdlc+'/ASG/ignore_list'
execution_confirmation = '/unipru/'+sdlc+'/ASG/execute_infra_shutdown'
ssm_asg_list_location =  '/unipru/'+sdlc+'/ASG/ASG_names'
ssm_stop_instance_Names  = '/unipru/'+sdlc+'/ASG/instance_names'

# ssm_current_batch_execution = '/unipru/'+sdlc+'/asg/batch_execution'
# s3_bucket_name = 'br-unipru'+sdlc+'-unipru-app-'+region+'-s3'

asg_search_text = 'AutoScalingGroups[] | [?contains(Tags[?ResourceId].Value, `{}`)]'

autoscaling_client = boto3.client('autoscaling',region_name=region)
ssm_client = boto3.client('ssm',region_name=region)
rds_client = boto3.client('rds', region_name = region )
ec2_client = boto3.client('ec2', region_name = region)

def shutdown_appasg(event, context):
    # TODO implement
    
    #check if lambda has permission to execute
    
    ssm_asg_list = ''
    
    try:
        execute = ssm_client.get_parameter(Name = execution_confirmation)['Parameter']['Value']
    except:
        execute = 'true'
    
    if(execute.lower() != 'true'):
        return {
            'statusCode': 200,
            'body': json.dumps('Automatic shutdown is off!')
        }
    
    # Fetch and filter ASGs in the env
    
    paginator = autoscaling_client.get_paginator('describe_auto_scaling_groups')
    page_iterator = paginator.paginate(PaginationConfig={'PageSize': 100})
    filtered_asgs = page_iterator.search(asg_search_text.format(sdlc))

    # fetch ASGs to ignore the execution
    
    try:
        ignore_list = ssm_client.get_parameter(Name = ssm_ignore_asg_location )['Parameter']['Value'].rsplit(",")
    except:
        ignore_list = []
        
    #shutting down ASGs

    for asg in filtered_asgs:
        asg_name=asg['AutoScalingGroupName']

        asg_execute=False

        if '-unipru'+sdlc+'-' in asg_name:                            #checking if the asg has sdlc in it
            asg_execute=True
            print("Igonre list  :   ", ignore_list )
            for asg_key in ignore_list:                 #checking if the asg has to be ignored
                if '-'+asg_key+'-' in asg_name:
                    asg_execute = False
                    break
                
            if asg['MinSize'] == 0 and asg['DesiredCapacity'] == 0:
                asg_execute = False
                
        print(asg_name,"  ",asg_execute)
        
        if asg_execute==False:
            continue
        
        
        ssm_asg_list = ssm_asg_list+','+asg_name
        
        
        ssm_asg_min_size='/unipru/'+sdlc+'/ASG/'+asg_name+'/MinSize'                    # minsize parameter name in param store
        ssm_asg_desired_capacity='/unipru/'+sdlc+'/ASG/'+asg_name+'/DesiredCapacity'    # desired size parmeter name in param store
        ssm_asg_max_size = '/unipru/'+sdlc+'/ASG/'+asg_name+'/MaxSize'                  # max size parameter name in param store
        min_size=str(asg['MinSize'])
        desired_capacity=str(asg['DesiredCapacity'])
        max_size = str(asg['MaxSize'])

        
        print(asg_name+' min size:' + min_size + ' desired:' + desired_capacity)

        if asg['MinSize'] > 0:
            ssm_client.put_parameter(Name=ssm_asg_min_size, Value=min_size, Type='String',Overwrite=True)   #updating minsize of the asg in param store
        if asg['DesiredCapacity'] > 0:    
            ssm_client.put_parameter(Name=ssm_asg_desired_capacity, Value=desired_capacity, Type='String',Overwrite=True)   # updating desiredsize of the asg in param store
        
        ssm_client.put_parameter(Name=ssm_asg_max_size, Value=max_size, Type='String',Overwrite=True) #updating max size in param store

        response = autoscaling_client.update_auto_scaling_group(AutoScalingGroupName=asg_name,MinSize=0,DesiredCapacity=0,MaxSize = 0) #updating asg
    
    try:
        ssm_client.put_parameter(Name = ssm_asg_list_location, Value = ssm_asg_list, Type = "StringList", Overwrite = True) #updating asg names in the param store for startup
    except:
        pass
    # shutting down rds clusters

    instances = rds_client.describe_db_instances()      #getting the list of rds clusters in the account

    if(sdlc == 'dev'):      # shutting down all the clusters in the dev
        for instance in instances['DBInstances']:
            cluster_id = instance["DBClusterIdentifier"]
            try:
                response = rds_client.stop_db_cluster(DBClusterIdentifier = cluster_id)
                print(" cluster : ", cluster_id, "   is stopped")
            except:
                continue
    else:           # shutting the all the corresponding cluster in the env
        for instance in instances['DBInstances']:
            cluster_id = instance['DBClusterIdentifier']

            if sdlc in cluster_id:
                try:
                    response = rds_client.stop_db_cluster(DBClusterIdentifier = cluster_id)
                    print("cluster : ", cluster_id, "    is stopped")
                except:
                    continue

########################## stoping instances #######################################

    instance_ids = []        # instances id to stop
    try:                    #getting the names of the instances to stop
        instances_names = ssm_client.get_parameter(Name = ssm_stop_instance_Names)['Parameter']['Value'].rsplit(",")
    except:
        instances_names = []

    response = ec2_client.describe_instances(   #getting the information about the instances to stop
        Filters = [
            {
                'Name':'instance-state-name',   # filtering  the instances which are running
                'Values':['running'] 
            },
            {
                'Name' :'tag:Name',             # filtering the instances which has names specified in ssm_stop_instanc_Names in param store
                'Values' : instances_names
            }
        ]
    )

    fields = response['Reservations']       # assigning value associated with Reservations key in response
    
    for field in fields:                    # iterating through the fields
        for instance in field['Instances']:     # iterating through isntances in filed whith key word Instances
            if instance['State']['Name'] == 'running':      # checking if the instance is running
                print("instance id : ", instance['InstanceId']) 
                for tag in instance['Tags']:
                    if tag['Key'] == 'Name':
                        print('instance name : ', tag['Value'])
                print("Stopping the instance")
                print("#######################################################################")
                instance_ids.append(instance['InstanceId']) # appending the instance id to list of instances which have to be stopped
    
    try:
        ec2_client.stop_instances(InstanceIds = instance_ids)
    except:
        print("Can't stop some instances")
    
    return {
        'statusCode': 200,
        'body': json.dumps('ENV is shutting down')
    }
