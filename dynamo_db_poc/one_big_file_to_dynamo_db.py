import concurrent.futures
import json


import boto3
from decimal import Decimal
import sys
from awsglue.utils import getResolvedOptions
import time
"""
Here we received the big files and downloading them in Glue cluster and then passing exclusive chunk of data into thread for processing
"""
s3 = boto3.client("s3")
s3.download_file(Bucket="br-uniprudev-us-east-1-ubsfi-glue-dev-store-s3", Key="pr_ubsfi/glue_app/vikas/dynamo_db_test/big_fil1.json", Filename="output.json")

fo = open('output.json', 'r')
infile = fo.readlines()
print (len(infile))
def function(lines):
    print("------------------")
    table = dynamodb.Table('Performance_By_Account_Request')
    with table.batch_writer() as batch:
        for line in lines:
            dict_l = json.loads(line)
            print(dict_l['id'])
            batch.put_item(Item=dict_l)


with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
    task1 = executor.submit(function, (infile[0:20000]))
    task2 = executor.submit(function, (infile[20000:40000]))
    task3 = executor.submit(function, (infile[40000:60000]))
    task4 = executor.submit(function, (infile[60000:80000]))
    task5 = executor.submit(function, (infile[80000:100000]))
    task6 = executor.submit(function, (infile[100000:120000]))
    task7 = executor.submit(function, (infile[120000:140000]))
    task8 = executor.submit(function, (infile[140000:160000]))
    task9 = executor.submit(function, (infile[160000:180000]))
    task10 = executor.submit(function, (infile[180000:200000]))




import boto3
from decimal import Decimal
import sys
from awsglue.utils import getResolvedOptions
import time
import json

import boto3

s3 = boto3.client("s3")
s3.download_file(Bucket="br-uniprudev-us-east-1-ubsfi-glue-dev-store-s3", Key="pr_ubsfi/glue_app/vikas/dynamo_db_test/big_fil1.json", Filename="output.json")
dynamodb = boto3.resource('dynamodb')

table = dynamodb.Table('Performance_By_Account_Request')
fo = open('output.json', 'r')
infile = fo.readlines()
with table.batch_writer() as batch:
    for line in infile:
        dict_l = json.loads(line)
        print(dict_l['id'])
        batch.put_item(Item=dict_l)
