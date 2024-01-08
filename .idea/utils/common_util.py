###############################
# This script will hold all common util method
###############################
from constant.common_constant import C_TIME_STAMP_FORMAT
from constant.property_constant import C_CLIENT_NBR_GL_PROP_KEY, C_RECORD_TYPE_GL_PROP_KEY, C_ENVIRONMENT_GL_PROP_KEY
from pyspark.sql.functions import udf
from pyspark.sql.types import *
from pyspark.sql import functions as sf
from utils.JobContext import JobContext
from pyspark.sql import Column
from constant.common_constant import *
import datetime
import re


def auto_str(cls):
    def __str__(self):
        return '%s(%s)' % (type(self).__name__, ', '.join('%s=%s' % item for item in vars(self).items()))

    cls.__str__ = __str__
    return cls

###############################
#  Return current time stamp wrapped in Spark Column class
###############################
def get_time_stamp():
    return sf.from_unixtime(sf.unix_timestamp(None, C_TIME_STAMP_FORMAT)).cast(TimestampType())


###############################
#  This will add new column in Dataframw with supplied default values
###############################
def add_default_clm(p_df, p_list_of_clm):
    JobContext.logger.info("Adding default value to Dataframe  list of column: {} ".format(p_list_of_clm))
    for l_clm, l_clm_val in p_list_of_clm.items():
        if l_clm_val is None:
            p_df = p_df.withColumn(l_clm, sf.lit(None))
        elif isinstance(l_clm_val, Column):
            p_df = p_df.withColumn(l_clm, l_clm_val)
        else:
            p_df = p_df.withColumn(l_clm, sf.lit(l_clm_val))
    JobContext.logger.info("Added default values")
    return p_df

def formatted_batch_date(format="%Y%m%d"):
    return datetime.datetime.strptime(JobContext.batch_date, C_BATCH_DATE_FORMAT).strftime(format)

###############################
#  This will format the batch date and if its not as valid it will break the flow
###############################
def format_batch_date(p_batch_date):
    # Check if date is valid
    JobContext.logger.info("Formating Batch Date: {} ".format(p_batch_date))
    try:
        datetime.datetime.strptime(p_batch_date, C_BATCH_DATE_FORMAT)
    except ValueError:
        raise ValueError("Incorrect data format for batch_date, should be YYYY-MM-DD")
    formated_date = sf.to_date(sf.lit(p_batch_date), C_DATE_FORMAT)
    JobContext.logger.info("Batch Date formatted")
    return formated_date

def current_time_stamp():
    return datetime.datetime.now().__str__()

current_time_stamp_udf = udf(current_time_stamp, StringType())

###############################
#  Add Default Audit column
###############################

def add_audit_columns(df, p_batch_date_clm, add_modification_clm=False, for_red_shift=False, created_user_nm = C_CREATED_USER_NM, created_progrm_nm = None):
    JobContext.logger.info("Adding Audit columns ")
    if(for_red_shift):
        df = df.withColumn("active_flg", sf.lit(True))
    else:
        df = df.withColumn("active_flg", sf.lit(C_BOOLEAN_DEFAULT_VALUE))

    if created_progrm_nm is None:
        created_progrm_nm = JobContext.get_property("JOB_NAME")[:20]
    elif len(created_progrm_nm)>20:
        JobContext.logger.info("Provided created_program_nm is greater than 20 char, selecting first 20 char")
        created_progrm_nm = created_progrm_nm[:20]
        # Using only 20 char for created_program_nm as redshift column can have only 20 char
    df = df.withColumn("batch_dt", p_batch_date_clm).withColumn("created_program_nm", sf.lit(created_progrm_nm.upper() )) \
        .withColumn("created_user_nm", sf.lit(created_user_nm)) \
        .withColumn("created_ts", current_time_stamp_udf().cast(TimestampType()))\
        .withColumn('created_ts', sf.from_utc_timestamp(sf.col('created_ts'), "EST"))

    if add_modification_clm:
        df = df.withColumn("modified_program_nm",
                           sf.lit(None).cast(StringType())).withColumn("modified_user_nm",
                                                                       sf.lit(None).cast(StringType())).withColumn("modified_ts",
                                                                                                                   sf.lit(None).cast(TimestampType()))
    JobContext.logger.info("Audit columns added ")
    return df

#############################################
# add_cpgm_nm_in_audit_columns to append
# input file details with created_program_nm
#############################################
def add_cpgm_nm_in_audit_columns(df, p_batch_date_clm, add_modification_clm=False, for_red_shift=False, created_user_nm = C_CREATED_USER_NM, created_progrm_nm = None):
    JobContext.logger.info("Adding Audit columns ")
    if(for_red_shift):
        df = df.withColumn("active_flg", sf.lit(True))
    else:
        df = df.withColumn("active_flg", sf.lit(C_BOOLEAN_DEFAULT_VALUE))

    g_df = df.groupby("account_id").agg(sf.concat_ws("_", sf.collect_set(df['rec_type_cd'])).alias('rec_type_cd'))

    n_df = df.drop('rec_type_cd')
    df = n_df.join(g_df, n_df["account_id"] == g_df["account_id"], "left") \
            .select(n_df["*"], g_df["rec_type_cd"])

    df.dropDuplicates()

    df = df.withColumn("batch_dt", p_batch_date_clm).withColumn('created_program_nm',sf.concat( sf.lit('STUB'),sf.lit('_'), sf.col('rec_type_cd'))) \
        .withColumn("created_user_nm", sf.lit(created_user_nm)) \
        .withColumn("created_ts", current_time_stamp_udf().cast(TimestampType()))\
        .withColumn('created_ts', sf.from_utc_timestamp(sf.col('created_ts'), "EST"))
    df = df.withColumn('created_program_nm', sf.substring(df.created_program_nm, 1, 18))

    if add_modification_clm:
        df = df.withColumn("modified_program_nm",
                           sf.lit(None).cast(StringType())).withColumn("modified_user_nm",
                                                                       sf.lit(None).cast(StringType())).withColumn("modified_ts",
                                                                                                                   sf.lit(None).cast(TimestampType()))
    JobContext.logger.info("Audit columns added by appending the input file details")
    return df

###############################
#  Convert boolean coulmns with values as 'T', 't' to True else value will be considered as False
# Redshift store Boolean as t and same is loaded to S3, Spark doesnt consider t or T as boolean, this method will convert them into boolean
###############################
def transform_boolean_clm(p_df, p_clm_name):
    return p_df.withColumn(p_clm_name, sf.when(sf.lower(sf.col(p_clm_name)).isin(TRUE_VALUES), True).when(sf.lower(sf.col(p_clm_name)).isin(FALSE_VALUES), False).otherwise(None))

###############################
#  This will accept the Dataframe and Map in below format and create new Dataframe with given column
# CLM_NME_TO_DATA_POSITION_MAP = {'cu_ref': [1, 9], 'eligible_cd': [10, 10], 'filler': [11, 11], 'fired_cd': [12, 12]}
###############################
def parse_fixed_len_data(df,clm_to_data_position_map):
    cols = []
    for clm, position in clm_to_data_position_map.items():
        cols.append(sf.trim(df.value.substr(position[0],position[1]-position[0]+ 1)).alias(clm))
    return df.select(*cols)


###############################
#  add Exception column in Base column list
###############################
def prepare_exception_clm_list(base_clm_list):
    l_fact_cls_entity_clm_list = base_clm_list[:]
    l_fact_cls_entity_clm_list.extend(C_EXCP_DATA_COLUMN_LIST)
    return l_fact_cls_entity_clm_list

def format_time_stamp(df, clm):
    return df


###############################
#  Format the data column to "yyyy-MM-dd" format
###############################
def format_date(df, clm):
    return df.withColumn(clm, sf.to_date(sf.col(clm), C_DATE_FORMAT))


def parse_fixed_len_data_with_start_len(df,clm_to_data_position_map, p_rec_type_clm=None,p_client_nbr_clm=None):
    JobContext.logger.info("Fixed length record parsing - Starts ")
    ###############################
    #  Parsing data based on provided position and length
    ###############################
    cols = []
    for clm, position in clm_to_data_position_map.items():
        cols.append(sf.trim(df.value.substr(position[0],position[1])).alias(clm))
    df = df.select(*cols)
    JobContext.logger.info("Fixed length record parsing - Ends ")
    JobContext.logger.info("Record type and client number validation - Starts ")
    ###############################
    #  fetching job parameters values - File(Record) Type and Client Number
    ###############################
    l_rec_type_val = JobContext.get_property(C_RECORD_TYPE_GL_PROP_KEY)
    l_client_nbr_val = JobContext.get_property(C_CLIENT_NBR_GL_PROP_KEY)
    ###############################
    #  Record type value validation
    ###############################
    if(p_rec_type_clm!=None):
        df = df.cache()
        l_rec_type_count = df.select(p_rec_type_clm).where(df[p_rec_type_clm]!=l_rec_type_val).count()
        if(l_rec_type_count!=0):
            raise Exception("Record Type Code in the file is not matching with the input file: {}".format(l_rec_type_val))
    ###############################
    #  Client Number validation
    ###############################
    if(p_client_nbr_clm!=None):
        df = df.cache()
        l_client_nbr_count = df.select(p_client_nbr_clm).where(df[p_client_nbr_clm]!=l_client_nbr_val).count()
        if(l_client_nbr_count!=0):
            raise Exception("Client Number in the file is not matching for the client, Input File- {}".format(l_client_nbr_val))
    JobContext.logger.info("Removing UTF-8 null char from data")
    df = remove_unwanted_char(df, C_UTF_NULL_CHAR)
    JobContext.logger.info("Record type and client number validation - Ends ")
    return df

#################
# This method will remove unwated char from record, for Ex:  BPSA files, has consecutive null utf-8 char, so when we load this in redshift it failes evenr with
# null as '\000', as it is expectin gonly once and gets more than that
##############
def remove_unwanted_char(src_df, char_to_remove):
    list_clm = src_df.schema.names
    for clm in list_clm:
        src_df = src_df.withColumn(clm, sf.regexp_replace(sf.col(clm), char_to_remove, ''))
    return src_df

def read_fixed_len_file_validate_header_footer(p_prop_location, p_date_format, p_header_matcher='%DATE=%', p_footer_matcher='%COUNT=%',p_include_header_footer_for_count=True, \
                                               split_char='=',p_require_batch_date_validation=True,old_appro=True):
    """

    :param p_prop_location:
    :param p_date_format:
    :param p_header_matcher:
    :param p_footer_matcher:
    :param p_include_header_footer_for_count:
    :param split_char:
    :param p_require_batch_date_validation:
    :param old_appro: - Default value in true, if set to false it will use new approch, in this, it will use spark's monotonically_increasing_id api
                        to generate unique ID. then pick first and last record. For double check it will then check for header and footer match.
                        This will work properly only if this is the first operation after read operation.
                        This API has limitations for huge file(as it shotres ID in 64 bits and split 32 for unique partition ID and 32 bit for unique records in that partition).
                        Before using it for in tera bytes, we should check if its working properly.
    :return:
    """
    JobContext.logger.info("Header/Footer validation - Starts ")
    l_file_validation_err_msg = ""
    ###############################
    #  Reading source File
    ###############################
    # read file and check if file is present
    try:
        """
        from pyspark.sql import Row
        
        # Read Byte data into RDD
        fixed_lengh_rdd = g_sc.textFile(<input_loc>, use_unicode=False)
        
        #Now encode binary data using iso-8859-1 or any required encoding
        rdd_encoded = fixed_lengh_rdd.map(lambda x: x.decode("iso-8859-1"))
       
        # converting RDD to Dataframe, passing Row object to understand the schema of record, as its fixed length column we need only one column, 
        # After this code will rename same
        encoded_df = rdd_encoded.map(Row("value")).toDF()
       
        # Now we can put our split logic here
        encoded_df = encoded_df.select(sf.col("value").substr(1, 50).alias('rec_type_cd'),sf.col("value").substr(51, 200).alias('client_nbr'))
        
        # Change needed in save time also. We need to use sam encoding which we have used to read 
        #  encoding="ISO-8859-1" is required to keep encoding intact,
        TODO: Currently, I am not aware how to pass encoding option in Glue API
        encoded_df.repartition(1).write.csv(path=<out_loc>, header="true", mode="overwrite", encoding="ISO-8859-1")
        """
        l_src_df = JobContext.glue_context.spark_session.read.text(JobContext.get_property(p_prop_location))
        # if l_src_df is not empty, cache it to reduce the overhead of action first()
        if l_src_df:
            l_src_df.cache()
        # check if source is empty or not
        if (l_src_df is None) | (l_src_df.first() is None):
            l_src_df = None
            JobContext.logger.info("File is empty from source".format(p_prop_location))
    except Exception as e:
        JobContext.logger.warn("Fail to read file {}".format(p_prop_location))
        l_src_df = None

    if l_src_df is None:
        return None

    ###############################
    #  Fetching Record count
    ###############################
    if p_include_header_footer_for_count:
        l_record_count = l_src_df.count()
    elif p_include_header_footer_for_count==False:
        l_record_count = l_src_df.count()-2

    if old_appro:
        l_src_df = l_src_df.withColumn('header_footer_flg', sf.lit(l_src_df.value.like(p_header_matcher)
                                                               | l_src_df.value.like(p_footer_matcher)))

        l_src_df.cache()
        ###############################
        #  Segregating Header-Footer and data records
        ###############################
        l_header_footer = l_src_df.filter(l_src_df.header_footer_flg).collect()
        l_src_df= l_src_df.filter(~l_src_df.header_footer_flg).drop('header_footer_flg')
        ###############################
        #  Fetching Header date and converting it Batch date format
        ###############################

        l_header_date = l_header_footer[0].value.strip().split(split_char)
        l_footer_count = l_header_footer[1].value.strip().split(split_char)[1]

    else:
        l_src_df = l_src_df.withColumn("clm_seq", sf.monotonically_increasing_id())
        min, max=l_src_df.agg(sf.min("clm_seq"), sf.max("clm_seq")).collect()[0]
        l_src_df = l_src_df.withColumn("first_last",sf.col("clm_seq").isin([min, max]))
        header_footer = l_src_df.filter(sf.col("first_last")).withColumn("head_flg", sf.lit(sf.col("value").like(p_header_matcher))) \
            .withColumn("footer_flg", sf.lit(sf.col("value").like(p_footer_matcher)))

        l_src_df = l_src_df.filter(~sf.col("first_last"))
        header_list = header_footer.filter(sf.col("head_flg")).collect()
        if header_list is None or len(header_list)==0:
            raise Exception(f"Header not found in file, used format is '{p_header_matcher}'")
        l_header_date = header_list[0].value.strip().split(split_char)
        JobContext.logger.info(f"Found header list: {str(l_header_date)}")
        footer_lis = header_footer.filter(sf.col("footer_flg")).collect()
        if footer_lis is None or len(footer_lis)==0:
            raise Exception(f"Footer not found in file, used format is '{p_footer_matcher}'")
        l_footer_count = footer_lis[0].value.strip().split(split_char)[1]
        JobContext.logger.info(f"Source Footer : {str(l_footer_count)}")


    if len(l_header_date)<=1:
        l_header_date = l_header_date[0]
    else:
        l_header_date = l_header_date[1]
    l_header_date_regex_arr=re.findall(r"[0-9]+[0-9,/,\-]+[0-9]+",l_header_date)
    if len(l_header_date_regex_arr) == 0:
        raise Exception("Header date not found")

    l_header_date = datetime.datetime.strptime(l_header_date_regex_arr[0], p_date_format).strftime(C_BATCH_DATE_FORMAT)
    ###############################
    #  Fetching trailer's record count
    ###############################
    l_batch_date = JobContext.batch_date

    l_footer_count_regex_arr= re.findall(r"\d+", l_footer_count)
    if len(l_footer_count_regex_arr) == 0:
        raise Exception("Trailer count not found")
    l_footer_count = int(l_footer_count_regex_arr[0])

    ###############################
    #  File level Validations
    ###############################
    if(l_header_date==l_batch_date):
        JobContext.logger.info(" The Source File header date : {} matches with the input Batch Date: {}".format(l_header_date,l_batch_date))

    if(l_footer_count==l_record_count):
        JobContext.logger.info(" Trailer count : {} matches with file record count : {}".format(l_footer_count,l_record_count))

    if(l_header_date!=l_batch_date and p_require_batch_date_validation == True):
        l_file_validation_err_msg += "| The Source File header date : {} does not match with the Input Batch Date: {}".format(l_header_date,
                                                                                                                              l_batch_date)
    if(l_footer_count!=l_record_count):
        l_file_validation_err_msg += "| Trailer count : {} does not match with file record count : {}".format(l_footer_count,
                                                                                                              l_record_count)
    if(l_file_validation_err_msg!=""):
        raise Exception(l_file_validation_err_msg)

    l_src_df.cache()
    if(l_src_df.count()==0):
        l_src_df = None
    JobContext.logger.info("Header/Footer validation - Ends ")
    return l_src_df



def trim_space(df, colum_list):
    JobContext.logger.info("Trimming the columns {}".format(colum_list))
    for clm in colum_list:
        df = df.withColumn(clm, sf.trim(sf.col(clm)))
    return df

###############################
#  Util method to cast column to their correct datatype
#  When we read data from flat files like CSV, TSV or fixed length format, for most of the case spark by default consider String
###############################
def cast_columns(p_df, p_clm_list, p_dt_format):
    for l_clm, l_clm_type in p_clm_list.items():
        JobContext.logger.info("Casting column - {} to {} - Starts".format(l_clm,l_clm_type))
        if (l_clm_type == DateType()):
            p_df = p_df.withColumn(l_clm, sf.to_date(sf.col(l_clm), p_dt_format))
        else:
            p_df = p_df.withColumn(l_clm, sf.col(l_clm).cast(l_clm_type))
        JobContext.logger.info("Casting column - {} to {} - Ends".format(l_clm,l_clm_type))
    return p_df

###############################
#  Util method to skip header and footer while reading data from input file
###############################
def read_fixed_len_file_skip_header_footer(p_prop_location, p_header_matcher='%DATE=%',
                                           p_footer_matcher='%CNT=%'):
    JobContext.logger.info("Header and Footer skipping - Starts ")
    ###############################
    # Reading data from input
    ###############################
    try:
        l_src_df = JobContext.glue_context.spark_session.read.text(JobContext.get_property(p_prop_location))
        # if l_src_df is not empty, cache it to reduce the overhead of action first()
        if l_src_df:
            l_src_df.cache()
        # check if source is empty or not
        if (l_src_df is None) | (l_src_df.first() is None):
            l_src_df = None
            JobContext.logger.info("File is empty from source".format(p_prop_location))
    except Exception as e:
        JobContext.logger.warn("Fail to read file {}".format(p_prop_location))
        l_src_df = None

    if l_src_df is None:
        return None
    ###############################
    #  Fetching header and footer
    ###############################
    l_src_df = l_src_df.withColumn('header_footer_flg', sf.lit(l_src_df.value.like(p_header_matcher)
                                                               | l_src_df.value.like(p_footer_matcher)))
    ###############################
    #  Segregating Header-Footer and data records
    ###############################
    l_src_df = l_src_df.filter(~l_src_df.header_footer_flg).drop('header_footer_flg')

    JobContext.logger.info("Header and Footer removed")

    return l_src_df

###############################
#  Util method to parsing data based on provided position and length
###############################
def parse_fixed_len_data_with_start_len_skip_client_nbr_rcd_typ(df,clm_to_data_position_map):
    JobContext.logger.info("Fixed length record parsing - Starts ")
    ###############################
    #  Parsing input data
    ###############################
    cols = []
    for clm, position in clm_to_data_position_map.items():
        cols.append(sf.trim(df.value.substr(position[0],position[1])).alias(clm))
    df = df.select(*cols)
    JobContext.logger.info("Fixed length record parsing - Ends ")
    return df

####################################################
#  Util method to convert the date in yyyy-mm-dd
####################################################

def date_cast(df, clms, reqd_date_format=C_DATE_FORMAT):
    for c in clms:
        df = df.withColumn(c, sf.to_date(sf.col(c), reqd_date_format))
    return df

####################################
#  Util method to remove the commas
####################################
def comma_remove(df, columns_to_consider):
    for clm in columns_to_consider:
        df = df.withColumn(clm, sf.regexp_replace(sf.col(clm), ",", ""))
    return df


def validate_null_or_empty(value, prop_name):
    if value is None or (len(value) == 0):
        exp_msg = "Property {} is empty".format(prop_name)
        JobContext.logger.info(exp_msg)
        raise Exception(exp_msg)

def replace_null_with_empty(*cols):
    updated_clm = []
    for clm in cols:
        if isinstance(clm, str):
            clm=sf.col(clm)
        updated_clm.append(sf.when(clm.isNull(), sf.lit("")).otherwise(clm))
    return updated_clm

###################################
#  This will create Dataframe schema bases on passed columns, it default set datatype to String
####################################
def generate_schema(p_col_list):
    schemaFeilds=[]
    for i in p_col_list:
        schemaFeilds.append(StructField(i, StringType(), True))
    return StructType(schemaFeilds)

def create_empty_dataframe(p_col_list):
    JobContext.logger.info("Creating empty dataframe with columns : {}".format(str(p_col_list)))
    g_empty_df_schema = generate_schema(p_col_list)
    ss = JobContext.glue_context.spark_session
    empty_df = ss.createDataFrame(ss.sparkContext.emptyRDD(),schema=g_empty_df_schema)
    JobContext.logger.info("Empty dataframe created")
    return empty_df

def when_condition_prepare(condition_map, clm_name):
    base_condition= None
    for key, value in condition_map.items():
        if key:
            if base_condition is None:
                base_condition = sf.when(sf.col(clm_name)==key,value)
            else:
                base_condition = base_condition.when(sf.col(clm_name)==key,value)
        else:
            base_condition = base_condition.otherwise(value)
    return base_condition

def lower_case(str):
    if str is not None:
        return str.lower()
    else:
        return None
        
###############################
# Below remove_nul_for_header is used, because file received for FM has NUL before and after header and ASCII character for 'NUL' is '\x00'
###############################
def date_validater_for_fixed_len_file(p_prop_location, p_date_format, remove_nul_for_header=True):
    # this function only validates date and not the footer of fixed length file. Date format : ex :      'xxxxxxx     040320'
    JobContext.logger.info("Parsing the date only from the fixed length file from the location: {}".format(p_prop_location))
    l_file_validation_err_msg = ""
    src_df = JobContext.glue_context.spark_session.read.text(JobContext.get_property(p_prop_location))
    if remove_nul_for_header is True:
        src_df_first = src_df.first()[0].replace('\x00', '').strip()
    else:
        src_df_first = src_df.first()[0].strip()
    JobContext.logger.info("First row of the file is  {}".format(src_df_first))
    parsed_date=src_df_first[-6:]
    JobContext.logger.info("Parsed date is   {}".format(parsed_date))
    l_batch_date = JobContext.batch_date

    l_header_date = datetime.datetime.strptime(parsed_date, p_date_format).strftime(C_BATCH_DATE_FORMAT)

    if(l_header_date==l_batch_date):
        JobContext.logger.info("Validation successful. The header date {} matches with batch date {}.".format(l_header_date, l_batch_date))
        JobContext.logger.info("Removing the header line from DF..  ")
        src_df = src_df.withColumn('header_footer_flg', sf.lit(src_df.value.contains(src_df.first()[0])))
        src_df = src_df.filter(~src_df.header_footer_flg).drop('header_footer_flg')

        JobContext.logger.info("Removed the header line from DF..  ")

    if(l_header_date!=l_batch_date):
        l_file_validation_err_msg += "| The Source File header date : {} does not match with the Input Batch Date: {}".format(l_header_date,l_batch_date)

    JobContext.logger.info("Header validation - Ends ")
    return src_df

###################################
# Method will copy the output from temporary location to target location with desired output file name in s3 bucket.
# p_src_prefix - is the path of the temp folder but will not contain s3 bucket name. For ex.: pr_ubsfi/work/glue_work_files/temp/job_run_id/
# p_target_key - is the target location with desired output file name. For ex. : pr_ubsfi/outgoing/target_folder/target_file_name.
# Bucket name will be taken from env.properties.
####################################
def rename_output_files(p_src_prefix, p_target_key, p_del_temp_file_required = True):
    import boto3
    g_env =JobContext.get_property(C_ENVIRONMENT_GL_PROP_KEY)
    C_BUCKET_NAME = JobContext.config.get(g_env, "s3.bucket")
    client = boto3.client('s3')
    response = client.list_objects(
        Bucket=C_BUCKET_NAME,
        Prefix=p_src_prefix,
    )
    g_count = 0
    JobContext.logger.info("Boto response content - {}".format(response["Contents"]))
    l_multipart_output = len(response["Contents"])
    JobContext.logger.info("Output files count - {}".format(l_multipart_output))
    for file in response["Contents"]:
        g_temp_file = file["Key"]
        JobContext.logger.info("File => {} ".format(file))
        if l_multipart_output>1:
            l_target_key = p_target_key + "_" + str(g_count)
        else:
            l_target_key = p_target_key
        cl_copy = boto3.resource("s3")
        copy_src = {'Bucket': C_BUCKET_NAME,
                    'Key': g_temp_file}
        cl_copy.meta.client.copy(copy_src, C_BUCKET_NAME, l_target_key)
        if p_del_temp_file_required:
            JobContext.logger.info("Deleting temp output files")
            client.delete_object(Bucket=C_BUCKET_NAME,Key=g_temp_file)
        g_count+=1

def remove_character(df, columns_to_consider, char_to_remove):
    for clm in columns_to_consider:
        df = df.withColumn(clm, sf.regexp_replace(sf.col(clm), char_to_remove, ""))
    return df


def isNull_check(clm):
    return sf.col(clm).isNull() | (sf.col(clm) == "")

def xor(clm1, clm2):
    return (sf.col(clm1) & sf.col(clm2)) | (~sf.col(clm1) & ~sf.col(clm2))

def addin_in_list_if_not_exits(li, el):
    if el not in li:
        li.append(el)

def alias_ontology_to_column(src_df,ont_to_fiel_map, explode_on, explode_alias="acc"):

    alias_clsm = [sf.col(k).alias(v) for k,v in ont_to_fiel_map.items()]
    return src_df.select(sf.explode_outer(explode_on).alias(explode_alias)).select(alias_clsm)

###############################
#  In header there is no word like 'date' and in footer there is no 'count' word use this
#  Just date is mentioned in the Header and record count (excluding header and footer) is metioned in the footer
###############################
def read_fixed_len_file_header_footer(p_prop_location, p_date_format="MMDDYYYY", g_header_date_format = '%Y-%m-%d', p_require_batch_date_validation=True):

    JobContext.logger.info("Header/Footer validation - Starts ")
    l_file_validation_err_msg = ""
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
            l_src_df = None
            JobContext.logger.info("File is empty from source".format(p_prop_location))
    except Exception as e:
        JobContext.logger.warn("Fail to read file {}".format(p_prop_location))
        l_src_df = None

    if l_src_df is None:
        return None

    ###############################
    #  Segregating Header-Footer and data records
    ###############################
    l_src_df = l_src_df.withColumn("value", sf.trim(l_src_df.value))
    l_src_df = l_src_df.withColumn("row_nm", sf.monotonically_increasing_id())
    min1=l_src_df.agg({"row_nm": 'min'}).collect()[0][0]
    max1=l_src_df.agg({"row_nm": 'max'}).collect()[0][0]
    hed_foo_df=l_src_df.filter(sf.col("row_nm").isin(min1, max1)).select(sf.col("value"))

    if hed_foo_df.count()==0:
        raise Exception("No header and footer in the file.")

    #header+footer is 2
    if hed_foo_df.count()!=2:
        raise Exception("Header or footer is not available in the file.")

    l_header_date=hed_foo_df.collect()[0].value
    l_footer_count=hed_foo_df.collect()[1].value

    ###############################
    #  Fetching Header date and converting it Batch date format
    ###############################

    if l_header_date is None or len(l_header_date)==0:
        raise Exception(f"Header not found in file, used format is MMDDYYYY")

    if len(l_header_date)==8:
        try:
            if (p_date_format=="MMDDYYYY"):
                head_date = datetime.datetime(int(l_header_date[4:8]),int(l_header_date[0:2]),int(l_header_date[2:4]))
        except ValueError:
            raise Exception("Invalid date in Header, used format is MMDDYYYY")
    else:
        raise Exception("Invalid date in Header")

    val = re.findall(r"^[0-9]+$", l_footer_count)
    if len(val):
        JobContext.logger.info(f"Source Footer Value : {str(l_footer_count)}")
    else:
        raise Exception("No footer count in the file.")

    #when header and footer are correct then extract the data 
    record_df = l_src_df.join(hed_foo_df, "value", how="left_anti").select(sf.col("value"))
    l_record_count = record_df.count()

    if record_df is None:
        return None 
        
    if record_df.count()==0:
        return None

    if int(l_footer_count)==l_record_count:
        JobContext.logger.info("Footer count is matched")

    head_date=head_date.strftime(g_header_date_format)

    l_batch_date=JobContext.batch_date

    ###############################
    #  File level Validations
    ###############################
    if(head_date==l_batch_date):
        JobContext.logger.info(" The Source File header date : {} matches with the input Batch Date: {}".format(l_header_date,l_batch_date))

    if(int(l_footer_count)==l_record_count):
        JobContext.logger.info(" Footer count : {} matches with file record count : {}".format(l_footer_count,l_record_count))

    if(head_date!=l_batch_date and p_require_batch_date_validation == True):
        l_file_validation_err_msg += "| The Source File header date : {} does not match with the Input Batch Date: {}".format(l_header_date,
                                                                                                                            l_batch_date)

    if(int(l_footer_count)!=l_record_count):
        l_file_validation_err_msg += "| Footer count : {} does not match with file record count : {}".format(l_footer_count,
                                                                                                            l_record_count)
    if(l_file_validation_err_msg!=""):
        raise Exception(l_file_validation_err_msg)

    if(record_df.count()==0):
        record_df = None
    JobContext.logger.info("Header/Footer validation - Ends ")

    return record_df

###############################
#  replace the date 1111-11-11 with '01/01/1753'
###############################
def replace_date_cols(dataf, colmns, act_dt, replace_dt):

    for col in colmns:
        dataf=dataf.withColumn(col, sf.when(sf.col(col)==sf.lit(act_dt), sf.lit(replace_dt)).otherwise(sf.col(col)))  
                        
    return dataf

################################################################
#  Util method to skip footer while reading data from input file
################################################################
def read_fixed_len_file_without_header_footer(p_prop_location, p_footer_matcher='%*%'):
    JobContext.logger.info("Footer skipping - Starts ")
    ###############################
    # Reading data from input
    ###############################
    try:
        l_src_df = JobContext.glue_context.spark_session.read.text(JobContext.get_property(p_prop_location))
        # if l_src_df is not empty, cache it to reduce the overhead of action first()
        if l_src_df:
            l_src_df.cache()
        # check if source is empty or not
        if (l_src_df is None) | (l_src_df.first() is None):
            l_src_df = None
            JobContext.logger.info("File is empty from source".format(p_prop_location))
    except Exception as e:
        JobContext.logger.warn("Fail to read file {}".format(p_prop_location))
        l_src_df = None

    if l_src_df is None:
        return None
    ###############################
    #  Fetching header and footer
    ###############################
    l_src_df = l_src_df.withColumn('footer_flg', sf.lit(l_src_df.value.like(p_footer_matcher)))
    ###############################
    #  Segregating Header-Footer and data records
    ###############################
    l_src_df = l_src_df.filter(~l_src_df.footer_flg).drop('footer_flg')

    JobContext.logger.info("Footer removed")

    return l_src_df
