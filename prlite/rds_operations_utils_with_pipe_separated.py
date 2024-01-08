from pg import DB
import logging
import boto3
import sys
import datetime
import concurrent.futures

from constant.common_constant import *
from constant.property_constant import *
from pyshell_utils.PyshellJobContext import *

##################################
# RETRIEVE S3 FILE AND PASS TO QUERY
##################################
s3 = boto3.client('s3')


def get_connection_details(p_connection_nm, p_region_nm):
    PyShellJobContext.logger.info("Fetching connection details")

    client = boto3.client('glue', region_name= p_region_nm)
    response = client.get_connection(Name=p_connection_nm)
    l_conn_properties = response["Connection"]["ConnectionProperties"]

    PyShellJobContext.logger.info("Connection details fetched")
    return l_conn_properties


##################################
# SET DB CONNECTION
##################################

def rds_db_connection():

    try:
        PyShellJobContext.logger.info("Getting Connection entities")
        connection_dict = get_connection_details(PyShellJobContext.get_property(C_REDSHIFT_CONNECTION_NAME_PROP_KEY),
                                                 C_REGION_NM)

        url_host = connection_dict["JDBC_CONNECTION_URL"].replace("//","").split(":")[2]
        url_port = int(connection_dict["JDBC_CONNECTION_URL"].replace("//","").split(":")[3].split("/")[0])

        PyShellJobContext.logger.info("Db connection set up")

        db = DB(dbname=PyShellJobContext.get_property(C_REDSHIFT_DB_NAME_PROP_KEY), host=url_host, port=url_port,
                user=connection_dict["USERNAME"], passwd=connection_dict["PASSWORD"])
        return db

    except Exception as E:
        PyShellJobContext.logger.info(E)
        return None
        
#################################
# Set DB CONNECTION for GLUE RDS
#################################
def glue_rds_db_connection():

    try:
        PyShellJobContext.logger.info("Getting Connection entities")
        connection_dict = get_connection_details(PyShellJobContext.get_property(C_RDS_CONNECTION_NAME_PROP_KEY),
                                                 C_REGION_NM)

        url_host = connection_dict["JDBC_CONNECTION_URL"].replace("//","").split(":")[2]
        url_port = int(connection_dict["JDBC_CONNECTION_URL"].replace("//","").split(":")[3].split("/")[0])

        PyShellJobContext.logger.info("Db connection set up")

        db = DB(dbname=PyShellJobContext.get_property(C_RDS_DB_NAME_PROP_KEY), host=url_host, port=url_port,
                user=connection_dict["USERNAME"], passwd=connection_dict["PASSWORD"])
        return db

    except Exception as E:
        PyShellJobContext.logger.info(E)
        return None


##################################
# Get All files from a specified path.
##################################

def get_all_s3_keys(s3_path):
    try:
        PyShellJobContext.logger.info("Getting all s3 keys from given path {}".format(s3_path))
        keys = []

        if not s3_path.startswith('s3://'):
            s3_path = 's3://' + s3_path

        bucket = s3_path.split('//')[1].split('/')[0]
        prefix = '/'.join(s3_path.split('//')[1].split('/')[1:])

        kwargs = {'Bucket': bucket, 'Prefix': prefix}
        while True:
            resp = s3.list_objects_v2(**kwargs)

            for obj in resp['Contents']:
                print("file {} is of size: {}".format(obj['Key'], obj['Size']))
                if obj['Size'] > 0:
                    keys.append(obj['Key'])
                else:
                    print ("File {} is empty".format(obj['Key']))
            try:
                kwargs['ContinuationToken'] = resp['NextContinuationToken']
            except KeyError:
                break
        return keys
    except Exception as e:
        PyShellJobContext.logger.info("Failed due to following reason")
        PyShellJobContext.logger.info("Error {} ".format(e))
        return None

##################################
# Load data from s3 to Rds Table.
##################################

def load_s3_to_rds(path, table, col=''):

    PyShellJobContext.logger.info(" Getting  all file names in a list from s3 path")
    file_list = get_all_s3_keys(path)

    if file_list is None:
        PyShellJobContext.logger.info("There is no such file or folder exists in {} path".format(path))
        return

    PyShellJobContext.logger.info(" Following s3 keys are read : ")
    print (file_list)

    PyShellJobContext.logger.info("Calling rds db connection")
    db = rds_db_connection()

    l_bucket_nm = PyShellJobContext.bucket_name

    PyShellJobContext.logger.info("*** Bucket : {} ***".format(l_bucket_nm))

    if len(file_list) == 0:
        PyShellJobContext.logger.info("file is empty, hence query is not executed")
    else:
        try:
            PyShellJobContext.logger.info("Running query to import from s3 to Rds table")
            length = len(file_list)

            for i in range(length):
                query = """SELECT
                    aws_s3.table_import_from_s3 (
                      '{}',
                      '{}',
                      'CSV HEADER',
                      aws_commons.create_s3_uri(
                      '{}',
                      '{}',
                      'us-east-1'
                    ));""".format(table, col, l_bucket_nm, file_list[i])
                PyShellJobContext.logger.info("Showing Query For Import To Rds :  {}".format(query))
                db.query(query)
                PyShellJobContext.logger.info("Query Execution For File - {} Finished".format(file_list[i]))

        except Exception as E:
            PyShellJobContext.logger.info("failed due to following reason : ")
            PyShellJobContext.logger.info(E)

########################################################
# Load data from s3 to Rds Table using multi-threading.
########################################################
def multi_load_s3_to_rds(path, table, col=''):

    PyShellJobContext.logger.info(" Getting  all file names in a list from s3 path")
    file_list = get_all_s3_keys(path)

    if file_list is None:
        PyShellJobContext.logger.info("There is no such file or folder exists in {} path".format(path))
        return

    PyShellJobContext.logger.info(" Following s3 keys are read : ")
    print (file_list)

    PyShellJobContext.logger.info("Calling rds db connection")
    
    #Get bucket name
    l_bucket_nm = PyShellJobContext.bucket_name

    PyShellJobContext.logger.info("*** Bucket : {} ***".format(l_bucket_nm))
    
    #Get no. of files
    length = len(file_list)
    
    #Assign no. of worker according to no. of files
    #Here we are processing the files in two bacthes
    num_worker = length/2 + 1
    
    if len(file_list) == 0:
        PyShellJobContext.logger.info("file is empty, hence query is not executed")
    else:
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_worker) as executor:
            for i in range(length):
                task = executor.submit(load_query, table, col, l_bucket_nm, file_list[i])


def load_query(table, col, l_bucket_nm, file_name):

    db = glue_rds_db_connection()
    try:
        PyShellJobContext.logger.info("Running query to import from s3 to Rds table")

        query = """SELECT
                        aws_s3.table_import_from_s3 (
                          '{}',
                          '{}',
                          '(DELIMITER ''|'', NULL '''')',
                          aws_commons.create_s3_uri(
                          '{}',
                          '{}',
                          'us-east-1'
                        ));""".format(table, col, l_bucket_nm, file_name)
        PyShellJobContext.logger.info("Showing Query For Import To Rds :  {}".format(query))
        db.query(query)
        PyShellJobContext.logger.info("Query Execution For File - {} Finished".format(file_name))

    except Exception as E:
        PyShellJobContext.logger.info("failed due to following reason : ")
        PyShellJobContext.logger.info(E)