from pprint import pprint
import boto3
import json
from boto3.dynamodb.conditions import Key
import amazondax
import os, sys, time
import botocore.session


params = {
    'TableName': 'stg_fact_holding',
    'FilterExpression': '#acc = :a',
    'ExpressionAttributeValues':{
            ':a': {
                'S': 'V642416-3-1'
                }
        },
        'ExpressionAttributeNames':{
            '#acc': 'account_id'
        }
    }

region = 'us-east-1'
session = botocore.session.get_session()
dynamodb = session.create_client('dynamodb', region_name=region)

endpoint = 'dynamotest.fnphz6.clustercfg.dax.use1.cache.amazonaws.com:8111'
dax = amazondax.AmazonDaxClient(session, region_name=region, endpoints=[endpoint])

client = dax

def lambda_handler(event, context):
   
    done = False
    start_key = None
    while not done:
        if start_key:
            params['ExclusiveStartKey'] = start_key
        response = client.scan(**params)
        items = response['Items']
        if items:
            for Items in items:
                return response['Items']
        start_key = response.get('LastEvaluatedKey', None)
        done = start_key is None