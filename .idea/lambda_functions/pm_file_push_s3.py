#/****************************************************************************************************
# This script will transfer the files from PRU to PM.
#-----------------------------------------------------------------------------------------------------
#                      **History**
#Date          Version No    Change                             Developer
#-------       ----------    ------                             ---------
#07/29/2020        1.0       Created                          Akshay Gundale
#******************************************************************************/

import json
import boto3
import os

#defining boto3 clients
source_client = boto3.client('s3')
destination_client = boto3.client('s3')

def pm_file_push_s3(event, context):
    #fetching values from environment variables
    sdlc = os.environ['ENV']

    destination_bucket_prefix = ''

    #initialization of required variables
    keys=[]
    file_names=[]
    j=''

    #source and destination buckets
    source_bucket = 'br-unipru'+sdlc+'-unipru-app-us-east-1-s3'
    destination_bucket = 'br-bpsuspm'+sdlc+'-bpsuspm-app-us-east-1-s3'

    print("Event  : ")
    print(event)

    path_in_event = event['Records'][0]['s3']['object']['key']

    split_path_in_event = path_in_event.rsplit("/",1)
    source_bucket_prefix = split_path_in_event[0]+"/"

    ############### define/append source and destination prefixes here ##################
    if source_bucket_prefix == 'pr_ubsfi/outgoing/extracts/pm/pm_benchmark/':
        destination_bucket_prefix = 'pm_ubsfi/incoming/today/pru_benchmark/'

    if source_bucket_prefix == 'pr_ubsfi/outgoing/extracts/pm/pm_style_benchmark/':
        destination_bucket_prefix = 'pm_ubsfi/incoming/today/pm_style_benchmark/'

    if source_bucket_prefix == 'pr_ubsfi/outgoing/extracts/pm/pm_blend_benchmark/':
        destination_bucket_prefix = 'pm_ubsfi/incoming/today/pru_blended_benchmark/'

    ##################################################################################

    if destination_bucket_prefix!= '':

        print("The files from source : {}/{} will be copied to destination : {}/{}.".format(source_bucket,source_bucket_prefix,
                                                                                destination_bucket,destination_bucket_prefix))

        #fetching the metadata of all the objects at source prefix in source bucket
        response_list_bucket_key=source_client.list_objects(Bucket=source_bucket,
                                                            Prefix=source_bucket_prefix)

        #Fetching all the keys with source prefix in the source bucket
        for file_keys in response_list_bucket_key['Contents']:
            print(file_keys['Key'])
            keys.append(file_keys['Key'])

        #removing the keys who are ending with '/'
        for i in keys:

            if i.endswith('/'):
                keys.remove(i)

        #retrieving only files names
        for k in keys:

            j=k.split('/')[-1]
            file_names.append(j)

        #actual copy to destination bucket
        for files in file_names:

            source_key=source_bucket_prefix+files

            #reading the content of the files
            source_response = source_client.get_object(
                Bucket=source_bucket,
                Key=source_key
            )

            destination_key = destination_bucket_prefix+files
            #create a file at destination with the same name as source and write it's content
            try:
                destination_client.upload_fileobj(
                    source_response['Body'],
                    destination_bucket,
                    destination_key,ExtraArgs={'ACL':'bucket-owner-full-control'}
                )

                print("Succesfully copied {}/{} to {}/{}. ".format(source_bucket,source_key,destination_bucket,destination_key))

            except Exception as e:
                print(" Caught exception while copying. ")
                print(" Exception message    :  {}".format(e))

    return {
        'statusCode': 200,
        'body': json.dumps('Complete!')
    }