import json
import boto3
import os
import pprint
import re

def send_plain_email(event, context):
    host = os.environ['uc4Agent_hostname']
    hostAZ =  []
    uc4Agent = ""
#host = sys.argv[1]
    ec2 = boto3.client('ec2',region_name="us-east-1")
    response = ec2.describe_instances(Filters=[{'Name':'tag:Name','Values':[os.environ['uc4Agent_hostname']]}])
    for r in response['Reservations']:
        for inst in r['Instances']:
           hostAZ.append(inst['Placement'])
    for i in hostAZ:
       instAZ = i.get('AvailabilityZone')
#   print(instAZ)
    if (host == "br-uniprudev-unipru-controlserver-asg" and instAZ == "us-east-1a"):
       print("us-east-1a")
       uc4Agent = "UC4_DEV_CONTROLSRVR_18"
    elif (host == "br-uniprudev-unipru-controlserver-asg" and instAZ == "us-east-1b"):
       print("us-east-1b")
       uc4Agent = "Dev"
    elif (host == "br-unipruqa-unipru-controlserver-asg" and instAZ == "us-east-1a"):
       print("us-east-1a")
       uc4Agent = "unipru-qa-uc4-71"
    elif (host == "br-unipruqa-unipru-controlserver-asg" and instAZ == "us-east-1b"):
       print("us-east-1b")
       uc4Agent = "unipru-qa-uc4-120"
    elif (host == "br-unipruuat-unipru-controlserver-asg" and instAZ == "us-east-1a"):
       print("us-east-1a")
       uc4Agent = "uat"
    elif (host == "br-unipruuat-unipru-controlserver-asg" and instAZ == "us-east-1b"):
       print("us-east-1b")
       uc4Agent = "unipru-uat-uc4"
    elif (host == "br-unipruprd-unipru-controlserver-asg" and instAZ == "us-east-1a"):
       print("us-east-1a")
       uc4Agent = "UC4_PRD_CONTROLSRVR_85"
    elif (host == "br-unipruprd-unipru-controlserver-asg" and instAZ == "us-east-1b"):
       print("us-east-1b")
       uc4Agent = "UC4_PRD_CONVERSIONSRVR_118"
    else:
       print("Not Valid Control Server Host:"+host)
    
    ses_client = boto3.client("ses", region_name="us-east-1")
    CHARSET = "UTF-8"
    email_sender = "AspireIntegrationHYD@broadridge.com"
    email_to = "AspireOpsHYD@broadridge.com"
    email_to1 = "AspireOpsAnalystHYD@broadridge.com"
    email_cc = "AspireIntegrationHYD@broadridge.com"
    email_cc1 = "WMAP-PRU-ParallelSupport@broadridge.com"
    email_cc2 = "PRUDevSupportHYD@broadridge.com"
    email_subject = ("UC4Agent : " + uc4Agent )
    #email_subject = (hs)
    email_body = ("OPS, \r\n"
             "Can you please restart the below daemons.\r\n"
             "-	NH.\r\n"
             "-	Batch Daemon.\r\n" 
            "-	Standard Upload Daemon.\r\n" 
            "-	Report Runner Daemon.\r\n"
            "-	UI Extract Daemon.\r\n")

    response = ses_client.send_email(
        Destination={
            "ToAddresses": [
                 email_to,email_to1,
            ],
            "CcAddresses": [
                 email_cc,email_cc1,email_cc2,
            ],               
        },
        Message={
            "Body": {
                "Text": {
                    "Charset": CHARSET,
                    "Data": email_body,
                }
            },
            "Subject": {
                "Charset": CHARSET,
                "Data": email_subject,
            },
        },
        Source=email_sender,
    )