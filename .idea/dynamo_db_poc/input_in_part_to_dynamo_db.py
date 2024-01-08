import boto3
from decimal import Decimal
import sys
from awsglue.utils import getResolvedOptions
import time
import json
import concurrent.futures
import boto3
"""
Here we received the part files and downloading them in Glue cluster and using thread to write into Dynamo db
"""
s3 = boto3.client("s3")

def file_download(file_name):
    print("Downloading...{}".format(file_name))
    s3.download_file(Bucket="br-uniprudev-us-east-1-ubsfi-glue-dev-store-s3", Key="pr_ubsfi/glue_app/vikas/dynamo_db_test/part_file/" + file_name, Filename=file_name)
    print("Downloaded...{}".format(file_name))

with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
    task1 = executor.submit(file_download, ('part1.json'))
    task1 = executor.submit(file_download, ('part2.json'))
    task1 = executor.submit(file_download, ('part3.json'))
    task1 = executor.submit(file_download, ('part4.json'))
    task1 = executor.submit(file_download, ('part5.json'))
    task1 = executor.submit(file_download, ('part6.json'))
    task1 = executor.submit(file_download, ('part7.json'))
    task1 = executor.submit(file_download, ('part8.json'))
    task1 = executor.submit(file_download, ('part9.json'))
    task1 = executor.submit(file_download, ('part10.json'))

def funtirecord_insert(file_name):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('Performance_By_Account_Request')
    with open(file_name) as infile:
        with table.batch_writer() as batch:
            for line in infile:
                dict_l = json.loads(line)
                print(dict_l["id"])
                batch.put_item(Item=dict_l)

with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
    task1 = executor.submit(funtirecord_insert, ('part1.json'))
    task1 = executor.submit(funtirecord_insert, ('part2.json'))
    task1 = executor.submit(funtirecord_insert, ('part3.json'))
    task1 = executor.submit(funtirecord_insert, ('part4.json'))
    task1 = executor.submit(funtirecord_insert, ('part5.json'))
    task1 = executor.submit(funtirecord_insert, ('part6.json'))
    task1 = executor.submit(funtirecord_insert, ('part7.json'))
    task1 = executor.submit(funtirecord_insert, ('part8.json'))
    task1 = executor.submit(funtirecord_insert, ('part9.json'))
    task1 = executor.submit(funtirecord_insert, ('part10.json'))
# with open("ouput.json") as infile:
#     with table.batch_writer() as batch:
#         for line in infile:
#             dict_l = json.loads(line)
#             print (dict_l['id'])
#             batch.put_item(Item=dict_l)

