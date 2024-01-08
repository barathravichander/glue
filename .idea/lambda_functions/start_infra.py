import boto3
import json
import os


sdlc = os.environ['ENV']
region = os.environ['REGION']

autoscaling_client = boto3.client('autoscaling',region_name=region)
ssm_client = boto3.client('ssm',region_name=region)
rds_client = boto3.client('rds',region_name=region)
ec2_client = boto3.client('ec2', region_name = region)

ssm_ignore_asg_location = '/unipru/'+sdlc+'/ASG/ignore_list'
# ssm_current_batch_execution = '/unipru/'+sdlc+'/asg/batch_execution'
ssm_asg_list_location = '/unipru/'+sdlc+'/ASG/ASG_names'
ssm_execution_confirmation = '/unipru/'+sdlc+'/ASG/execute_infra_startup'
ssm_start_instance_Names  = '/unipru/'+sdlc+'/ASG/instance_names'

# asg_search_text = 'AutoScalingGroups[] | [?contains(Tags[?ResourceId].Value, `{}`)]'

def start_infra(event, context):
    # TODO implement
    # check if execution for lambda confirmed
    
    excution_confirmation = ssm_client.get_parameter(Name = ssm_execution_confirmation)['Parameter']['Value']
    if excution_confirmation.lower() != 'true':
        return{
            'statusCode' : 200,
            'body' : json.dumps('Autamated startup is disabled')
        }
    
    # get the startup list from parameter store which is updated by shutdown lambda
    
    startup_asg_list = ssm_client.get_parameter(Name = ssm_asg_list_location)['Parameter']['Value'].rsplit(",")

    # get the asgs to ignore form parameter store
    try:
        ignore_asgs=ssmclient.get_parameter(Name=ssm_ignore_asg_location)['Parameter']['Value'].split(',')
    except:
        ignore_asgs=[]
        
    # starting the asgs one by one
    
    for asg_name in startup_asg_list:
        
        if(asg_name == ''):
            continue

        asg_execute=True            # asg start condition
        
        for asg_key in ignore_asgs:     #checking if the asg is in ignore list
            if '-'+asg_key+'-' in asg_name:
                asg_execute=False
                break
        
        if asg_execute==False:      # if permission to start asg is not sufficient  go to next asg
            continue

        ssm_asg_min_size='/unipru/'+sdlc+'/ASG/'+asg_name+'/MinSize'                    #ssm parameter name for minsize of asg
        ssm_asg_desired_capacity='/unipru/'+sdlc+'/ASG/'+asg_name+'/DesiredCapacity'    #ssm parameter name for desiredsize of asg
        ssm_asg_max_size = '/unipru/'+sdlc+'/ASG/'+asg_name+'/MaxSize'
        
        # get minsize value from parameter store
        try: 
            min_size=int(ssm_client.get_parameter(Name=ssm_asg_min_size)['Parameter']['Value'])
        except:
            min_size=1
            
        #get desiredsize value from parameter store
        try: 
            desired_size=int(ssm_client.get_parameter(Name=ssm_asg_desired_capacity)['Parameter']['Value'])
        except:
            desired_size=1

        # get max size value from paramter store
        try: 
            max_size = int(ssm_client.get_parameter(Name=ssm_asg_max_size)['Parameter']['Value'])
        except:
            max_size = 1
        
        print("ASG name : ", asg_name)
        print("MinSize : ", min_size," DesiredSize : ",desired_size, " MaxSize : ", max_size)
        print("#############################################################################################")
        # deleting the minsize and maxsize parameters from param store because of no further use 
        # try:
        #     ssm_client.delete_parameters(Names = [ ssm_asg_min_size, ssm_asg_desired_capacity])
            
        # except:
            
        #     print("parameters are not found or cannot delete")
        
        
        autoscaling_client.update_auto_scaling_group(AutoScalingGroupName=asg_name,MinSize=min_size,DesiredCapacity=desired_size, MaxSize = max_size)

        


####################### starting db instances #####################
    instances = rds_client.describe_db_instances()

    if(sdlc == 'dev'):      #starting all the clusters in rds in dev
        for instance in instances['DBInstances']:
            cluster_id = instance['DBClusterIdentifier']
            try:
                response = rds_client.start_db_cluster( DBClusterIdentifier = cluster_id )
                print("cluster : ", cluster_id, "    is starting")
            except:
                continue
    else:                   #starting all the respective rds clusters
        for instance in instances['DBInstances']:
            cluster_id = instance['DBClusterIdentifier']

            if sdlc in cluster_id:
                try:
                    response = rds_client.start_db_cluster( DBClusterIdentifier = cluster_id)
                    print("cluster : ", cluster_id, "   is starting")
                except:
                    continue

############################  starting Instances #######################

    intance_ids = []        # instances id to start
    try:                    #getting the names of the instances to start
        instances_names = ssm_client.get_parameter(Name = ssm_start_instance_Names)['Parameter']['Value'].rsplit(",")
    except:
        instances_names = []

    response = ec2_client.describe_instances(   #getting the information about the instances to start
        Filters = [
            {
                'Name':'instance-state-name',   # filtering  the instances which are stopped
                'Values':['stopped'] 
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
            if instance['State']['Name'] == 'stopped':      # checking if the instance is stopped
                print("instance id : ", instance['InstanceId']) 
                for tag in instance['Tags']:
                    if tag['Key'] == 'Name':
                        print('instance name : ', tag['Value'])
                print("Starting the instance")
                instance_ids.append(instance['InstanceId']) # appending the instance id to list of instances which have to be stopped
    
    try:
        ec2_client.start_instances(InstanceIds = instance_ids)
    except:
        print("Can't start some instances")
                
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
