import boto3
import json
import os

sdlc = os.environ['ENV']
region = os.environ['REGION']

autoscaling_client = boto3.client('autoscaling',region_name=region)
ssm_client = boto3.client('ssm',region_name=region)
rds_client = boto3.client('rds',region_name=region)
redshift_client = boto3.client('redshift',region_name=region)
s3 = boto3.resource('s3',region_name=region)

s3_bucket_name = 'br-unipru'+sdlc+'-unipru-app-'+region+'-s3'
rds_name = 'br-unipru'+sdlc+'-unipru-aurora-db1-rds-cls'
redshift_name = 'br-unipru'+sdlc+'-redshift-wmap-unipru'

ssm_ignore_asg_location = '/unipru/'+sdlc+'/asg/ignore_list'
ssm_current_batch_execution = '/unipru/'+sdlc+'/asg/batch_execution'
asg_search_text = 'AutoScalingGroups[] | [?contains(Tags[?ResourceId].Value, `{}`)]'

def shutdown_batchinfra(event, context):
    try: 
        batch_execution=bool(ssmclient.get_parameter(Name=ssm_current_batch_execution)['Parameter']['Value'])
    except:
        batch_execution=False

    if batch_execution == True:
        new_file = open('/tmp/status','w')
        new_file.write('False')
        new_file.close()

        s3.meta.client.upload_file('/tmp/status', s3_bucket_name, 'pr_ubsfi/backup/restart/status')

        return {
        'statusCode': 200,
        'body': json.dumps('lambda is skipped as batch is executing')
        }
    
    paginator = autoscaling_client.get_paginator('describe_auto_scaling_groups')
    page_iterator = paginator.paginate(PaginationConfig={'PageSize': 100})
    filtered_asgs = page_iterator.search(asg_search_text.format(sdlc))
    try: 
        ignore_asgs=ssmclient.get_parameter(Name=ssm_ignore_asg_location)['Parameter']['Value'].split(',')
        ignore_asgs=ignore_asgs+['web','ecs']
    except:
        ignore_asgs=['web','ecs']

    for asg in filtered_asgs:
        asg_name=asg['AutoScalingGroupName']
        asg_execute=True
        
        for asg_filter in ignore_asgs:
            if asg_filter in asg_name:
                asg_execute=False
                break
        
        if asg_execute==False:
            continue

        ssm_asg_min_size='/unipru/'+sdlc+'/ASG/'+asg_name+'/MinSize'
        ssm_asg_desired_capacity='/unipru/'+sdlc+'/ASG/'+asg_name+'/DesiredCapacity'
        min_size=str(asg['MinSize'])
        desired_capacity=str(asg['DesiredCapacity'])

        
        print(asg_name+' min size:' + min_size + ' desired:' + desired_capacity)

        if asg['MinSize'] > 0:
            ssm_client.put_parameter(Name=ssm_asg_min_size, Value=min_size, Type='String',Overwrite=True)
        if asg['DesiredCapacity'] > 0:    
            ssm_client.put_parameter(Name=ssm_asg_desired_capacity, Value=desired_capacity, Type='String',Overwrite=True)
        
        response = autoscaling_client.update_auto_scaling_group(AutoScalingGroupName=asg_name,MinSize=0,DesiredCapacity=0)

        
    #todo: check if RDS and Redshift are up and then shutdown/handle exceptions
    redshift_client.pause_cluster(ClusterIdentifier=redshift_name)
    rds_client.stop_db_cluster(DBClusterIdentifier=rds_name)

    new_file = open('/tmp/status','w')
    new_file.write('True')
    new_file.close()

    s3.meta.client.upload_file('/tmp/status', s3_bucket_name, 'pr_ubsfi/backup/restart/status')
    return {
    'statusCode': 200,
    'body': json.dumps('success from Lambda!')
    }
