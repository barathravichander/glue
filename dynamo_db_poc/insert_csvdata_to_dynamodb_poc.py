import time
import json
import csv
import concurrent.futures
import boto3
import codecs
from boto3.s3.transfer import TransferConfig
from constant.property_constant import *
from pyshell_utils.common_pyshell_util import *
from pyshell_utils.pyshell_job_base import *
from utils.JobContext import *
import concurrent.futures
from decimal import Decimal

class csv_json_dynamodb(PyshellJobBase):

    def _get_logger_name(self):
        return "csv_json_dynamodb"

    def _execute_job(self):

        self.logger.info("************ Starting job : csv_json_dynamodb  ***************")

        targetbucket = 'br-uniprudev-us-east-1-ubsfi-glue-dev-store-s3'
        csvkey = 'pr_ubsfi/glue_app/Utkarsh/DDB_testing/Files/stg_hldg.csv'

        s3 = boto3.client("s3")
        s3.download_file(Bucket=targetbucket, Key=csvkey, Filename="output.csv")
        self.logger.info("File Downloaded...!")
        #csv_object = s3.Object(targetbucket, csvkey)
        #csv_content = csv_object.get(PartNumber=123)['Body'].read().splitlines()
		
		self.logger.info("Reading CSV File")
        #fo = open('output.csv', 'r')
        no_lines_processed = 0
        infile = []
        self.logger.info("Reading File...!")
        with open('output.csv') as f:
            for lines in read_in_chunks(f):
                for line in lines:
                    line.replace("/n","")
                    infile.append(line)
                #print("\nlength of file: "+str(len(infile)))
                if len(infile) > 1000:
                    i=1
                    start = 1
                    end = 100
                    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                        for x in range(10):
                            print('calling read executor ' + str(i) + '...')
                            if(x<9):
                                executor.submit(worker_thread, (infile[start:end]),i)
                            else:
                                executor.submit(worker_thread, (infile[start:]),i)
                            i=i+1
                            start = end
                            end = end + 100
                    no_lines_processed = no_lines_processed + len(infile)
                    self.logger.info('no. of lines processed in file : {}'.format(no_lines_processed))
                    infile = []

##########################
#### Worker Thread #######
##########################
def worker_thread(infile):
    i=1
    start = 1
    end = 100

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        for x in range(10):
            print('calling function executor ' + str(i) + '...')
            if(x<9):
                executor.submit(function, (infile[start:end]),i)
            else:
                executor.submit(function, (infile[start:]),i)
            i=i+1
            start = end
            end = end + 100



def read_in_chunks(file_object, chunk_size=1024):
    ###############################
    ##read a file piece by piece.##
    ##Default chunk size: 1k."""###
    ###############################
    while True:
        data = file_object.readlines(chunk_size)
        #print("\ndata: "+str(data))
        if not data:
            break
        yield data

#############################
# Insert data into DynamoDB #
#############################
def function(lines,i):
    targetbucket = 'br-uniprudev-us-east-1-ubsfi-glue-dev-store-s3'
    jsonkey = 'pr_ubsfi/glue_app/Utkarsh/DDB_testing/JSON_Files/test' + str(i) + '.json'
    list = []
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('test')

    print("\n executor " + str(i) + " called")
    for line in lines:
        #print("line: "+line)
        x = line.split(',')
        id = str(x[0])
        account_id = str(x[1])
        instrument_id = str(x[2])
        sleeve_id = str(x[3])
        account_type_id = str(x[4])
        currency_cd = str(x[5])
        adv_flg = str(x[6])
        effective_dt = str(x[7])
        quantity_val = Decimal(str(x[8]))
        mv_val = Decimal((x[9]))
        ai_val = str(x[10]).rstrip("\n")
        ai_val = Decimal(ai_val)

        table.put_item(
            Item = {
                'id': id,
                'account_id': account_id,
                'instrument_id': instrument_id,
                'sleeve_id': sleeve_id,
                'account_type_id': account_type_id,
                'currency_cd': currency_cd,
                'adv_flg': adv_flg,
                'effective_dt': effective_dt,
                'quantity_val': quantity_val,
                'mv_val': mv_val,
                "ai_val": ai_val
            }
        )

    print("\n executor " + str(i) + " completed...!")


def main():
    x = csv_json_dynamodb()
    x.execute()

if __name__ == '__main__':
    main()

