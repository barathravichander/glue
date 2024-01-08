# --------------------------------------------------------------------------
#                       History
# Date            Version No    Developer                 Change
# -------          ------       ---------                 ------
# 11/22/2022         0.1        Uday V                   Order of fields are shuffled at information_schema, jobs failed
#                                                        UAT, now ordering by ordinal_position.
# --------------------------------------------------------------------------

from pg import DB
import logging
import boto3
import sys
import datetime
import concurrent.futures
from threading import Event, Thread
from constant.common_constant import *
from constant.property_constant import *
from pyshell_utils.PyshellJobContext import *

##################################
# RETRIEVE S3 FILE AND PASS TO QUERY
##################################
s3 = boto3.client('s3')
resource_s3 = boto3.resource('s3')

def get_connection_details(p_connection_nm, p_region_nm):

    client = boto3.client('glue', region_name= p_region_nm)
    response = client.get_connection(Name=p_connection_nm)
    l_conn_properties = response["Connection"]["ConnectionProperties"]

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
def glue_rds_db_connection(rds_dbname, rds_connection_name):

    try:
        connection_dict = get_connection_details(rds_connection_name, C_REGION_NM)

        url_host = connection_dict["JDBC_CONNECTION_URL"].replace("//","").split(":")[2]
        url_port = int(connection_dict["JDBC_CONNECTION_URL"].replace("//","").split(":")[3].split("/")[0])

        db = DB(dbname=rds_dbname, host=url_host, port=url_port,
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
        raise Exception("There is no such file or folder exists from source")

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
def multi_load_s3_to_rds(path, table, col='', no_of_connections=300):

    PyShellJobContext.logger.info(" Getting  all file names in a list from s3 path")
    file_list = get_all_s3_keys(path)

    if file_list is None:
        raise Exception("There is no such file or folder exists in source")

        PyShellJobContext.logger.info("There is no such file or folder exists in {} path".format(path))
        return

    PyShellJobContext.logger.info(" Following s3 keys are read : ")
    print (file_list)

    PyShellJobContext.logger.info("Calling rds db connection")

    #Get bucket name
    l_bucket_nm = PyShellJobContext.bucket_name

    PyShellJobContext.logger.info("*** Bucket : {} ***".format(l_bucket_nm))

    #prefix of the source file
    input_prefix = path.split("://",10)
    input_prefix = input_prefix[1].split('/',1)
    source_key = input_prefix[1]
    PyShellJobContext.logger.info('Prefix of source file:  {}'.format(source_key))

    #Get no. of files
    length = len(file_list)

    #Assign no. of worker according to no. of files
    #Here we are processing the files in two bacthes
    num_worker = no_of_connections

    #RDS Connection details
    rds_dbname = PyShellJobContext.get_property(C_RDS_DB_NAME_PROP_KEY)
    rds_connection_name = PyShellJobContext.get_property(C_RDS_CONNECTION_NAME_PROP_KEY)

    #Flag to check whether all files completed successfully
    completion_flag = False

    #Retry attempts for new threads
    retry = 2

    #Defining Total rows imported from s3 to rds
    row_sum = 0

    if len(file_list) == 0:
        PyShellJobContext.logger.info("file is empty, hence query is not executed")
    else:

        while completion_flag is False:

            #Task status list
            task_list = []

            with concurrent.futures.ThreadPoolExecutor(max_workers=num_worker) as executor:
                for i in range(length):
                    task = executor.submit(load_query, table, col, l_bucket_nm, file_list[i], rds_dbname, rds_connection_name)
                    task_list.append(task)
                completion_flag = True

            resp = s3.list_objects(Bucket=l_bucket_nm, Prefix=source_key, Delimiter='/',MaxKeys=1)
            folder_check = 'Contents' in resp

            #Row count list
            row_count_list = []

            #Add row count for completed tasks
            for future in concurrent.futures.as_completed(task_list):
                row_count_list.append(future.result())
            row_sum = row_sum + sum(row_count_list)

            if folder_check is False:
                PyShellJobContext.logger.info("All files were loaded successfully...")
            else:
                if retry>0:
                    retry = retry - 1
                    PyShellJobContext.logger.info("Retry Attempt - {}".format(2-retry))
                    completion_flag = False
                    file_list = get_all_s3_keys(path)
                    length = len(file_list)
                    if length ==0:
                        completion_flag = True
                    num_worker = length
                else:
                    raise Exception("Retry Attempt threshold reached...")

    #Writing total count of rows imported to s3
    PyShellJobContext.logger.info("Total count of rows imported to s3 {}".format(row_sum))

    Key_value = source_key + table +'_count_' + str(row_sum)
    result = resource_s3.meta.client.put_object(Body=str(row_sum), Bucket=l_bucket_nm, Key=Key_value)

    res = result.get('ResponseMetadata')

    if res.get('HTTPStatusCode') == 200:
        print('File Uploaded Successfully')
    else:
        print('File Not Uploaded')


def load_query(table, col, l_bucket_nm, file_name, rds_dbname, rds_connection_name):

    #Creating rds db connection
    db = glue_rds_db_connection(rds_dbname, rds_connection_name)

    #Define a initial row count with 0
    row_count = 0

    try:
        PyShellJobContext.logger.info("Running query For File - {}".format(file_name))

        query = """SELECT
                        aws_s3.table_import_from_s3 (
                          '{}',
                          '{}',
                          'CSV',
                          aws_commons.create_s3_uri(
                          '{}',
                          '{}',
                          'us-east-1'
                        ));""".format(table, col, l_bucket_nm, file_name)

        response = db.query(query)
        PyShellJobContext.logger.info("Response - {}".format(response))

        query_response = str(response).split("\n")[2]
        row_count = query_response.split(" ")[0]

        if response == None:
            raise Exception("Query for File - {} Failed".format(file_name))
        else:
            PyShellJobContext.logger.info("Query Execution For File - {} Finished".format(file_name))
            resource_s3.Object(l_bucket_nm,file_name).delete()

    except Exception as E:
        PyShellJobContext.logger.info("failed due to following reason : ")
        PyShellJobContext.logger.info(E)

    return int(row_count)


def delete_acc_from_rds(table, accounts):

    #RDS Connection details
    rds_dbname = PyShellJobContext.get_property(C_RDS_DB_NAME_PROP_KEY)
    rds_connection_name = PyShellJobContext.get_property(C_RDS_CONNECTION_NAME_PROP_KEY)

    #Creating rds db connection
    db = glue_rds_db_connection(rds_dbname, rds_connection_name)

    try:
        PyShellJobContext.logger.info("Running delete query for accounts - {}".format(accounts))

        query = """DELETE FROM {} WHERE account_id IN ({});""".format(table, accounts)

        db.query(query)
        PyShellJobContext.logger.info("Query Execution Finished")

    except Exception as E:
        PyShellJobContext.logger.info("failed due to following reason : ")
        PyShellJobContext.logger.info(E)

def truncate_from_rds(table):

    #RDS Connection details
    rds_dbname = PyShellJobContext.get_property(C_RDS_DB_NAME_PROP_KEY)
    rds_connection_name = PyShellJobContext.get_property(C_RDS_CONNECTION_NAME_PROP_KEY)

    #Creating rds db connection
    db = glue_rds_db_connection(rds_dbname, rds_connection_name)

    try:
        PyShellJobContext.logger.info("Running truncate query for table - {}".format(table))

        query = """TRUNCATE TABLE {};""".format(table)

        db.query(query)
        PyShellJobContext.logger.info("Query Execution Finished")

    except Exception as E:
        PyShellJobContext.logger.info("failed due to following reason : ")
        PyShellJobContext.logger.info(E)


def get_column_name_from_rds_table(table):

    #RDS Connection details
    rds_dbname = PyShellJobContext.get_property(C_RDS_DB_NAME_PROP_KEY)
    rds_connection_name = PyShellJobContext.get_property(C_RDS_CONNECTION_NAME_PROP_KEY)

    #Creating rds db connection
    db = glue_rds_db_connection(rds_dbname, rds_connection_name)

    schema_name = PyShellJobContext.get_property(C_RDS_SCHEMA_NAME_PROP_KEY)

    try:
        PyShellJobContext.logger.info("Running get columns query for table - {}".format(table))

        query = """SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = '{}'
        AND table_name   = '{}' order by ordinal_position;""".format(schema_name, table)

        response = db.query(query)
        PyShellJobContext.logger.info("Query Execution for fetching columns finished")

        #Get required columns from the response
        query_response = str(response).split("\n")[2:-1]
        l_columns = ''
        for column in query_response:
            l_columns = l_columns + column.strip(' ') + ','

        PyShellJobContext.logger.info("Columns for table {} - {}".format(table, l_columns))

        return l_columns[:-1]

    except Exception as E:
        PyShellJobContext.logger.info("failed due to following reason : ")
        PyShellJobContext.logger.info(E)


#This function is defined to check idle connections and terminate them in background while the load is going
def check_idle_connections(interval):

    #RDS Connection details
    rds_dbname = PyShellJobContext.get_property(C_RDS_DB_NAME_PROP_KEY)
    rds_connection_name = PyShellJobContext.get_property(C_RDS_CONNECTION_NAME_PROP_KEY)

    #Creating rds db connection
    db = glue_rds_db_connection(rds_dbname, rds_connection_name)

    PyShellJobContext.logger.info("Checking idle connections...")

    stopped = Event()
    try:
        def loop():
            while not stopped.wait(interval): # the first call is in 'interval' secs

                query = """WITH inactive_connections AS (
                           SELECT * FROM pg_stat_activity
                            WHERE
                                --Get idle connections
                                pid in (select pid from pg_stat_activity where state = 'idle')
                            AND
                                --Connections created from glue do not have application name, therefore we want to terminate those connections only which have application_name as NULL
                                application_name = ''
                            AND
                                -- Include connections to the same database the thread is connected to
                                datname = current_database() 
                            AND
                                -- Include connections using the same thread username connection
                                usename = current_user 
                            AND
                                -- Include inactive connections only
                                state in ('idle', 'idle in transaction', 'idle in transaction (aborted)', 'disabled') 
                            AND
                                --Check those connections if state changes in interval of 5 minutes
                                current_timestamp - state_change > interval '5 minutes' 
                        )
                        SELECT
                            pg_terminate_backend(pid)
                        FROM
                            inactive_connections 
                """

                db.query(query)

        Thread(target=loop).start()

    except Exception as E:
        PyShellJobContext.logger.info("failed due to following reason : ")
        PyShellJobContext.logger.info(E)

    return stopped.set



