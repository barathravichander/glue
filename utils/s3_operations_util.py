###############################
# This Script will hold various S3 related functions like Read etc.
###############################
#                       History
# Date            Version No    Developer         Change
# -------         ------        ---------         ------
# 07/20/2022        1           Uday V             Now exceptions data will overwrite like staging.
# --------------------------------------------------------------------------

from awsglue.dynamicframe import DynamicFrame
from constant.common_constant import C_OLD_REC_INDICATOR_CLM
from constant.property_constant import C_GLUE_S3_DB_NAME_PROP_KEY, C_S3_REDSHIFT_LOAD_PATH, C_S3_READSHIFT_LOAD_FILES_BASE_LOCATION, \
    C_S3_LOOKUP_BASE_LOCATION_PROP_KEY, C_SOURCE_SYSTEM_NAME_PROP_KEY, C_S3_DIR_CLEANUP_BEFORE_SAVE, C_S3_EXCP_DIR_CLEANUP_BEFORE_SAVE
from utils.JobContext import JobContext
from pyspark.sql import functions as sf
from pyspark.sql.types import *
###############################
# Function to read Data using catalog and convert it into Dataframe
###############################
from utils.common_util import transform_boolean_clm, formatted_batch_date, validate_null_or_empty,current_time_stamp_udf
from utils.schema_file_parser_util import SchemaCache



def read_from_s3_catalog_convert_to_df(p_database, p_table_name, p_fields_to_select=None, p_filter_condition=None,p_quoteChar='"',p_escaper='\\'):
    JobContext.logger.info(
        "Starting Reading Data from Catalog with parameters: p_database: {}, p_table_name: {}, p_fields_to_select: {}, p_filter_condition:{}".format(
            p_database, p_table_name, p_fields_to_select, p_filter_condition))
    l_table_config = JobContext.table_config_map.get(p_table_name)
    base_lookup_loc= JobContext.get_property(C_S3_LOOKUP_BASE_LOCATION_PROP_KEY)
    table_loc = base_lookup_loc + "/" + p_table_name
    if l_table_config and l_table_config.partitioned_tbl:
        partition_cond= get_partition_column()
        table_loc = table_loc + "/" + partition_cond
        JobContext.logger.info("Table is partitioned, applying partition condition {}".format(partition_cond))

    schema = SchemaCache.get_spark_schema(p_table_name)
    JobContext.logger.info("Reading Table {} from location: {}".format(p_table_name,table_loc))
    table_df = JobContext.glue_context.spark_session.read.schema(schema).csv(table_loc, header=True,quote=p_quoteChar,escape=p_escaper)

    JobContext.logger.info("Used schema is...")
    JobContext.logger.info(table_df._jdf.schema().treeString())
    # select the fields if passed
    if (p_fields_to_select is not None):
        table_df = table_df.select(p_fields_to_select)

    JobContext.logger.info("Found schema resolution configuration {}".format(l_table_config))
    if l_table_config and l_table_config.include_stub:
        if p_fields_to_select is None:
            p_fields_to_select = l_table_config.column_list
            JobContext.logger.info("Using supplied Column list {}".format(p_fields_to_select))
        return __include_stub_records(table_df, p_table_name, schema, p_fields_to_select)


    JobContext.logger.info("Completed Reading Data from Catalog")

    return table_df

###############################
#  This method will  read corresponding stub output location and take union
###############################
def __include_stub_records(catalog_df, p_table_name, schema, p_fields_to_select=None,p_quoteChar='"',p_escaper='\\'):
    base_location = JobContext.get_property(C_S3_READSHIFT_LOAD_FILES_BASE_LOCATION)

    # as stub will create subdirectory for file_type so using /*
    # ex. s3://<>/dtl_instrument_ref/rimes, s3://<>/dtl_instrument_ref/fact_set, we need to take all of them
    stub_location = base_location + p_table_name + "/*/"
    JobContext.logger.info("Reading stub records from location {}".format(stub_location))

    try:
        stub_rec_df = JobContext.glue_context.spark_session.read.schema(schema).csv(stub_location, header=True,quote=p_quoteChar,escape=p_escaper)
        if p_fields_to_select is not None:
            stub_rec_df = stub_rec_df.select(*p_fields_to_select)

        if (stub_rec_df is not None) & (stub_rec_df.first() is not None):
            lookup_with_stub_rec_df = catalog_df.union(stub_rec_df)
            JobContext.logger.info("Took union of Lookup and stub records")
        else:
            JobContext.logger.info("No stub records found")
            lookup_with_stub_rec_df = catalog_df

    except Exception as e:
        JobContext.logger.warn("Fail to read directory {}".format(stub_location))
        JobContext.logger.error("Union Error - {}".format(str(e)))
        # print(e.message)
        lookup_with_stub_rec_df = catalog_df

    JobContext.logger.info("Completed Reading Data from Catalog")
    return lookup_with_stub_rec_df



def read_from_s3(p_s3_location, p_with_header= True, p_sep=",", source_system_partition=False, read_all_common_source=True, p_schema=None):
    #TODO: Below API skips Header even after passing withHeader= True, need to investigate more on it, till then using Spark API to read it
    # g_glueContext.create_dynamic_frame_from_options(connection_type="s3", connection_options={'paths': [p_s3_location]},  format="csv", format_options={"withHeader:": p_with_header}).toDF()

    validate_null_or_empty(p_s3_location,"S3 Location")

    if not partitioned_output_not_required(p_s3_location):
        if source_system_partition == True:
            src_sys = JobContext.get_property(C_SOURCE_SYSTEM_NAME_PROP_KEY)
            if read_all_common_source:
                src_sys_part = '*_' + src_sys
            else:
                src_sys_part = JobContext.file_being_processed + '_' + src_sys
                src_sys_part = '/file_type='+ src_sys_part + '/'
            partition_loc = get_partition_column()
            p_s3_location = p_s3_location + partition_loc +'/'+ src_sys_part + '/'
            JobContext.logger.info("Reading S3 location {}".format(p_s3_location))
        else:
            partition_loc = get_partition_column()
            p_s3_location = p_s3_location + partition_loc
            JobContext.logger.info("Reading S3 location {}".format(p_s3_location))
    try:
        # read file and check if directory is present

        if p_schema is not None:
            JobContext.logger.info("user defined schema is being used")
            src_df = JobContext.glue_context.spark_session.read.csv(p_s3_location, header=p_with_header,sep=p_sep, schema=p_schema)
        else:
            src_df = JobContext.glue_context.spark_session.read.csv(p_s3_location, header=p_with_header,sep=p_sep)
        # check if source is empty or not
        if (src_df is None) | (src_df.first() is None):
            src_df = None
            JobContext.logger.info("Directory {} is empty".format(p_s3_location))
    except Exception as e:
        JobContext.logger.warn("Fail to read directory {}".format(p_s3_location))
        JobContext.logger.error("Read Error - {}".format( str(e)))
        src_df = None
    JobContext.logger.info("S3 location {} read successfully".format(p_s3_location))
    return src_df

###############################
# Function to save Dataframe into given S3 location, by default it will save in CSV format
###############################
def save_output_to_s3(p_data_frame, p_name_of_dyanmic, p_location, p_format="csv", repartition_count=1,source_system_partition=False,p_quoteChar='"',p_escaper='\\', use_spark_api=False):

    JobContext.logger.info("Saving data at S3 location {} in format {}".format(p_location, p_format))
    if p_location.endswith("/") == False:
        p_location = p_location +  "/"

    if partitioned_output_not_required(p_location):
        JobContext.logger.info("Output is not partitioned")
        l_location = p_location + JobContext.file_being_processed
        JobContext.logger.info("Output is not partitioned {}".format(l_location))
    else:
        if source_system_partition == True:
            src_sys = JobContext.get_property(C_SOURCE_SYSTEM_NAME_PROP_KEY)
            if (src_sys is None) or (src_sys == ""):
                src_sys_part = JobContext.file_being_processed
            else:
                src_sys_part = JobContext.file_being_processed + '_' + src_sys
            partition_loc = get_partition_column()
            l_location = p_location + partition_loc +'/file_type='+ src_sys_part + '/'
            JobContext.logger.info("Output is partitioned {}".format(l_location))
        else:
            partition_loc = get_partition_column()
            l_location = p_location + partition_loc + '/'
            JobContext.logger.info("Output is partitioned {}".format(l_location))

    clean_up_dir=JobContext.get_property(C_S3_DIR_CLEANUP_BEFORE_SAVE)
    clean_up_dir_excp=JobContext.get_property(C_S3_EXCP_DIR_CLEANUP_BEFORE_SAVE)
    JobContext.logger.info(f"Configured cleanup directory: {clean_up_dir}")
    # Idea is to put all dir common separate, split it here create array and iterate, for now we are implementing it in simple way
    clean_up_dir = "/"+clean_up_dir+"/"
    clean_up_dir_excp = "/"+clean_up_dir_excp+"/"
    if p_location.find(clean_up_dir)!=-1:
        # Currently Glue does not supports override option, we have tried Spark API to write DF with override,
        # found that it has some different behaviour for Decimal compare to Glue API, so we stick to Glue API ,Which is tested approch.
        clean_dir(l_location)
    elif p_location.find(clean_up_dir_excp)!=-1:
        clean_dir(l_location)

    if repartition_count is not None:
        JobContext.logger.info("Reparitioning datframe to {}".format(repartition_count))
        p_data_frame = p_data_frame.repartition(repartition_count)



    # TODO - For audit - will remove in final commit
    import time
    start_time = time.time()

    if use_spark_api:
        JobContext.logger.info("Using Spark API to write data")
        p_data_frame.write.mode("append").csv(l_location, header=True)

    else:
        JobContext.logger.info("Using Gleu API to write data")
        l_dynamic_frame = DynamicFrame.fromDF(p_data_frame, JobContext.glue_context, p_name_of_dyanmic)
        JobContext.glue_context.write_dynamic_frame.from_options(frame=l_dynamic_frame, connection_type="s3",
                                                                 connection_options={"path": l_location}, format=p_format,
                                                                 format_options={"quoteChar": p_quoteChar,"escaper":p_escaper})
    t = (time.time() - start_time)
    JobContext.logger.info("Time taken for data at S3 location {} in time {}".format(p_location, t))
    # TODO - For audit - will remove in final commit
    JobContext.logger.info("Saved Dataframe to S3 location")


def clean_dir(loc):
    import boto3
    s3 = boto3.resource('s3')

    chunked_loc = loc.split("/",3)
    s3_buket = chunked_loc[2]
    prefix= chunked_loc[3]
    JobContext.logger.info("From location found S3 bucket: {}, prefix: {}".format(s3_buket,prefix))
    bucket = s3.Bucket(s3_buket)

    JobContext.logger.info("Delete {} directory from S3 buket {}".format(prefix,s3_buket))
    present_dir = bucket.objects.filter(Prefix=prefix)
    present_dir_count=len(list(present_dir))
    JobContext.logger.info(f"Deleting {present_dir_count} objects from {loc}")
    present_dir.delete()

def get_partition_column():
    return "pr_batch_dt=" + formatted_batch_date()

def partitioned_output_not_required(p_s3_location):
    paths = p_s3_location.split('/')
    today = (JobContext.get_property("s3.source.input.path") in paths)
    return (JobContext.get_property(C_S3_REDSHIFT_LOAD_PATH) in paths) | (JobContext.get_property("s3.source.input.path") in paths)


def save_output_to_s3_first_old_then_new(p_data_frame, p_name_of_dyanmic, p_location, p_format="csv", repartition_count=10):
    JobContext.logger.info("Saving Dataframe to S3 location {} in format {}, it will first save old records then new records ".format(p_location, p_format))
    p_data_frame.cache()
    old_data_df = p_data_frame.filter(C_OLD_REC_INDICATOR_CLM).drop(C_OLD_REC_INDICATOR_CLM)\
        .withColumn("created_ts", current_time_stamp_udf().cast(TimestampType()))\
        .withColumn('created_ts', sf.from_utc_timestamp(sf.col('created_ts'), "EST"))
    save_output_to_s3(old_data_df,p_name_of_dyanmic, p_location, p_format, repartition_count)
    new_data_df = p_data_frame.filter(~sf.col(C_OLD_REC_INDICATOR_CLM)).drop(C_OLD_REC_INDICATOR_CLM)\
        .withColumn("created_ts", current_time_stamp_udf().cast(TimestampType()))\
        .withColumn('created_ts', sf.from_utc_timestamp(sf.col('created_ts'), "EST"))
    save_output_to_s3(new_data_df,p_name_of_dyanmic, p_location, p_format, repartition_count)
    JobContext.logger.info("Saved Dataframe to S3 location")



def read_from_catalog_and_filter_inactive_rec(table_name, p_fields_to_select = None):
    table_df = read_from_s3_catalog_convert_to_df(JobContext.get_property(C_GLUE_S3_DB_NAME_PROP_KEY), table_name, p_fields_to_select)
    table_df = transform_boolean_clm(table_df, "active_flg").filter('active_flg')
    return table_df

def read_from_s3_as_text(p_prop_location):
    JobContext.logger.info("Reading text file - Starts ")
    ###############################
    #  Reading source File
    ###############################
    # read file and check if file is present
    try:
        l_src_df = JobContext.glue_context.spark_session.read.text(JobContext.get_property(p_prop_location))
        # if l_src_df is not empty, cache it to reduce the overhead of action first()
        if l_src_df:
            l_src_df.cache()
        # check if source is empty or not
        if (l_src_df is None) | (l_src_df.first() is None):
            JobContext.logger.info("File is empty from source".format(p_prop_location))
            return None
    except Exception as e:
        JobContext.logger.warn("Fail to read file {}".format(p_prop_location))
        return None
    return l_src_df



def read_json_from_s3(s3_loc):
    JobContext.logger.info("Processing of ETF json file is starting...")
    try:

        src_df = JobContext.glue_context.spark_session.read.json(s3_loc,allowUnquotedControlChars=True)
        if (src_df is None) | (src_df.first() is None):
            JobContext.logger.info("File is empty")
            return None
        return src_df
    except Exception as e:
        JobContext.logger.info(f"Fail to read the file location ")
        # This is not good approc to segregate exception, but Spark throw org.apache.spark.sql.AnalysisException
        # for dir empty and invalid JSON. So we are checking exception message if message contains path it means file is empty of dir not present,
        # if not we will throw exception and fail job
        if s3_loc is not None  and (s3_loc[:-1] in str(e)):
            # log and skipJ
            JobContext.logger.error("Read Error - {}".format(str(e)))
        else:
            JobContext.logger.error("File is not a valid Json".format(str(e)))
            raise Exception("File is not a valid Json")

        return None

def read_from_s3_staging(p_s3_location, p_with_header= True, p_sep=",", source_system_partition=False, read_all_common_source=True, p_schema=None):
    #TODO: Below API skips Header even after passing withHeader= True, need to investigate more on it, till then using Spark API to read it
    # g_glueContext.create_dynamic_frame_from_options(connection_type="s3", connection_options={'paths': [p_s3_location]},  format="csv", format_options={"withHeader:": p_with_header}).toDF()

    validate_null_or_empty(p_s3_location,"S3 Location")

    if not partitioned_output_not_required(p_s3_location):
        if source_system_partition == True:
            src_sys = JobContext.get_property(C_SOURCE_SYSTEM_NAME_PROP_KEY)
            if read_all_common_source:
                src_sys_part = '*_' + src_sys
            else:
                src_sys_part = JobContext.file_being_processed + '_' + src_sys
                src_sys_part = '/file_type='+ src_sys_part + '/'
            partition_loc = get_partition_column()
            p_s3_location = p_s3_location + partition_loc +'/'+ src_sys_part + '/'
            JobContext.logger.info("Reading S3 location {}".format(p_s3_location))
        else:
            p_s3_location = p_s3_location
            JobContext.logger.info("Reading S3 location {}".format(p_s3_location))
    try:
        # read file and check if directory is present

        if p_schema is not None:
            JobContext.logger.info("user defined schema is being used")
            src_df = JobContext.glue_context.spark_session.read.csv(p_s3_location, header=p_with_header,sep=p_sep, schema=p_schema)
        else:
            src_df = JobContext.glue_context.spark_session.read.csv(p_s3_location, header=p_with_header,sep=p_sep)
        # check if source is empty or not
        if (src_df is None) | (src_df.first() is None):
            src_df = None
            JobContext.logger.info("Directory {} is empty".format(p_s3_location))
    except Exception as e:
        JobContext.logger.warn("Fail to read directory {}".format(p_s3_location))
        JobContext.logger.error("Read Error - {}".format( str(e)))
        src_df = None
    JobContext.logger.info("S3 location {} read successfully".format(p_s3_location))
    return src_df
