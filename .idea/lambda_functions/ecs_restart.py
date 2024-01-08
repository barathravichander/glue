import boto3
import json
import os

client  = boto3.client('ecs')
def restart_service(event, context):
    tasks = client.list_tasks(cluster='br_unipru'+os.environ['ENV']+'_unipru_app_ecs_cluster')['taskArns']
    for task in tasks:
        print(task)
        client.stop_task(cluster='br_unipru'+os.environ['ENV']+'_unipru_app_ecs_cluster', task=task)
    return {
        'statusCode': 200,
        'body': json.dumps('Success!')
    }
