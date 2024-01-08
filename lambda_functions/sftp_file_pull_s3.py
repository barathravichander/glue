#/****************************************************************************************************
# This script will fetch the files from SFTP server and place them in a respective s3 incoming folder.
#-----------------------------------------------------------------------------------------------------
#                      **History**
#Date          Version No    Change                             Developer
#-------       ----------    ------                             ---------
#07/20/2020        1.0       Created                          Akshay Gundale
#******************************************************************************/

import json
import paramiko
import boto3
import os
import socket
import io

def sftp_file_pull_s3(event, context):

    #dynamic values
    sdlc = event['ENV']
    files_to_be_pulled = event['FILES_TO_PULL']  #This paramer can have multiple EXACT MATCHING file names, separated by comma ONLY
    sftp_directory = event['SFTP_DIRECTORY']
    print('Environ values assigned as below : ')
    print("ENV = {}".format(sdlc))
    print("FILES_TO_PULL = {}".format(files_to_be_pulled))
    print("Directory selected at SFTP server = {}".format(sftp_directory))

    # creating boto3 clients
    print('Creating boto3 clients!')
    ssm = boto3.client('ssm')
    s3 = boto3.client('s3')

    #assigning default values
    ftp_port = 22
    s3_destination_key_incoming_stage = 'pr_ubsfi/incoming/stage/'

    #parameters from ssm paramstore
    print('Fetching values from paramstore!')

    ssm_bucket_name = ssm.get_parameter(Name= '/unipru/'+sdlc+'/ftp/bucketname')
    s3_destination_bucket_name = ssm_bucket_name['Parameter']['Value']
    print("Bucket name fetched:   {} . ".format(s3_destination_bucket_name))

    ssm_ftp_host = ssm.get_parameter(Name= '/unipru/'+sdlc+'/ftp/host_ip')
    ftp_host = ssm_ftp_host['Parameter']['Value']
    print("IP address fetched:   {} . ".format(ftp_host))

    ssm_ftp_username = ssm.get_parameter(Name= '/unipru/'+sdlc+'/ftp/username')
    ftp_username = ssm_ftp_username['Parameter']['Value']
    print("Username fetched:   {} . ".format(ftp_username))

    ssm_s3_ftp_priv_key = ssm.get_parameter(Name= '/unipru/'+sdlc+'/ftp/key_to_server.key')
    s3_ftp_priv_key = ssm_s3_ftp_priv_key['Parameter']['Value']
    print("Fetched private key. ")

    print("All the parameters from paramstore have been fetched successfully..")

    print("Trying to connect to server..")
    transport = paramiko.Transport(ftp_host, ftp_port)
    print('Able to Connect!')

    print("Retrieving the key")
    key_tmp = io.StringIO(s3_ftp_priv_key)
    s3_ftp_priv_key_retrived = paramiko.RSAKey.from_private_key(key_tmp)
    print("Key retrieval successful.!")

    try:
        print('Trying to authenticate with username {}.'.format(ftp_username))
        transport.connect(username=ftp_username, pkey=s3_ftp_priv_key_retrived)
        print('SFTP Authentication Successful!')

    except Exception as e:
        print(" Occurred issue while connecting to  {} . ".format(ftp_host))
        print(" Exception message    :  {}".format(e))
        return

    print('Creating SFTP client.')
    try:
        ftp_conn = paramiko.SFTPClient.from_transport(transport)
        print('SFTP Client created!')
    except Exception as e:
        print(" Occurred issue while creating sftp client . ")
        print(" Exception message    :  {}".format(e))
        return

    ftp_conn.chdir(sftp_directory)
    print('SFTP directory selected!')

    count=0

    #empty lists
    list_of_files_to_be_pulled = []
    no_matching_file_found_at_sftp = []

    #separating out the names of the files to be pulled, removine the leading and trailing spaces and putting into new list
    temp_list_of_files_to_be_pulled = files_to_be_pulled.split(',')

    for file_to_pull in temp_list_of_files_to_be_pulled:
        file_to_pull = file_to_pull.strip()
        list_of_files_to_be_pulled.append(file_to_pull)

    # for filename in ftp_conn.listdir():
    for filename in list_of_files_to_be_pulled:

        if filename in ftp_conn.listdir():

            sftpfile = sftp_directory + filename
            localfile = '/tmp/' + filename
            ftp_conn.get(sftpfile, localfile)
            print('File {} copied locally!'.format(filename))

            s3file = s3_destination_key_incoming_stage + filename
            try:
                s3.upload_file(Filename=localfile, Bucket=s3_destination_bucket_name, Key=s3file)
                print('File {} successfully copied to  {}/{}.'.format(filename,s3_destination_bucket_name,s3_destination_key_incoming_stage))
                count=count+1
            except Exception as e:
                print(" Occurred issue while copying file  {} . ".format(filename))
                print(" Exception message    :  {}".format(e))
                return

        else:

            no_matching_file_found_at_sftp.append(filename)

    print("Number of files downloaded : {}".format(count))
    print("The files with matching names : {},  NOT found at SFTP server {}!".format(no_matching_file_found_at_sftp, ftp_host))

    return {
        'statusCode': 200,
        'body': json.dumps('Success!')
    }