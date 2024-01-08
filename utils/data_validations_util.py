from constant.common_constant import C_OPERATION_NOT_SUPPORTED
from pyspark import StorageLevel
from pyspark.sql import functions as sf
from pyspark.sql.types import *
from constant.common_constant import *
from utils.JobContext import JobContext
from utils.s3_operations_util import save_output_to_s3


###############################
#  This method will validate and return vaild records and it will  store  invaild records
###############################
def validate_rec_and_filter(p_df_to_validate, p_clmn_to_validate_against, p_s3_location, exception_output_clms=None,is_excp_for_report=False,
                            stg_type=StorageLevel.MEMORY_ONLY):
    JobContext.logger.info(
        "Starting Validating Dataframe column against:{},  invaild records output S3 Location: {}".format(
            p_clmn_to_validate_against, p_s3_location))
    p_df_to_validate = validate_rec(p_df_to_validate, p_clmn_to_validate_against,stg_type)
    p_df_to_validate.persist(stg_type)
    l_df_with_valid_rec = p_df_to_validate.filter(p_df_to_validate.exception_desc == '|')
    l_df_with_invalid_rec = p_df_to_validate.filter("exception_desc != '|'")
    if is_excp_for_report:
        l_df_with_valid_rec = p_df_to_validate
        l_excp_type = 'R'
    else:
        l_excp_type = 'E'
    l_df_with_invalid_rec = add_default_value_to_invalid_rec(l_df_with_invalid_rec, JobContext.file_being_processed,
                                                             l_excp_type, 'N')

    # only selecte the give columns
    if exception_output_clms is not None and len(exception_output_clms) != 0:
        l_df_with_invalid_rec = l_df_with_invalid_rec.select(*exception_output_clms)

    #TODO: Use partition and use Glue Catalog also
    #TODO: REmove teh header when
    save_output_to_s3(l_df_with_invalid_rec, "df_with_invalid_rec", p_s3_location, repartition_count=None)
    JobContext.logger.info("Validation Of Dataframe  done")
    return l_df_with_valid_rec.drop("exception_desc")


###############################
#  This method will  iterate over passed map and new column to validate the records
###############################
def validate_rec(p_df_to_validate, p_clmn_to_validator,stg_type):
    JobContext.logger.info("Starting Validating records column to validate against:{}".format(p_clmn_to_validator))
    p_df_to_validate = p_df_to_validate.withColumn("exception_desc", sf.lit('|'))
    count = 0
    for l_clm, l_validator in p_clmn_to_validator.items():
        p_df_to_validate = validate_clm(l_validator, p_df_to_validate, l_clm)
        count= count+1
        #TODO: We need to put optimized values
        if stg_type != StorageLevel.DISK_ONLY:
            if count %5 ==0:
                p_df_to_validate.persist()
        else:
            JobContext.logger.info("Not performing intermediate caching ")
    JobContext.logger.info("Validation Of Dataframe record is done")
    return p_df_to_validate


###############################
#  This method will  check passed parameter  and  take action accordingly
###############################
def validate_clm(p_validator, p_df_to_validate, p_clm):
    JobContext.logger.info("Validating column {} with {}".format(p_clm, p_validator))
    if isinstance(p_validator, list):
        for l_val in p_validator:
            p_df_to_validate = l_val.validate(p_df_to_validate, p_clm)
    elif isinstance(p_validator, Validator):
        p_df_to_validate = p_validator.validate(p_df_to_validate, p_clm)
    else:
        raise Exception('Does not found the supported formator')
    JobContext.logger.info("Validation of column is Done")
    return p_df_to_validate


def add_default_value_to_invalid_rec(p_invalid_df, p_source_table_nm, p_exception_type, p_load_status):
    return p_invalid_df.withColumn('source_table_nm', sf.lit(p_source_table_nm)).withColumn('exception_type', sf.lit(
        p_exception_type)).withColumn('load_status', sf.lit(p_load_status))


###############################
# Base class for all validation type
###############################
class Validator(object):

    def validate(self, p_df_to_validate, p_clm):
        raise Exception(C_OPERATION_NOT_SUPPORTED)


###############################
# Date validator class
###############################
class DateFormatValidator(Validator):
    format = ""

    def __init__(self, p_format):
        self.format = p_format

    def validate(self, p_df_to_validate, p_clm):
        JobContext.logger.info("Validating column {} for with date format {}".format(p_clm, self.format))
        l_df = p_df_to_validate.withColumn("validated_date", sf.to_date(sf.col(p_clm), self.format)).withColumn(
            "exception_desc", sf.when((sf.col(p_clm).isNotNull() & sf.col("validated_date").isNull()),
                                      sf.concat(sf.col('exception_desc'), sf.lit(
                                          "{} is not a validate date as per format {}|".format(p_clm,
                                                                                               self.format)))).otherwise(
                sf.col('exception_desc'))).drop('validated_date')
        JobContext.logger.info("Validated column {} for with date format {}".format(p_clm, self.format))
        return l_df


class NotNullValidator(Validator):

    # using complex name, so that it does not collide with exiting names
    C_EXCEPTION_MSG_CLM_NAME= "exception_msg_notnullvalidator"
    lookup_clm_nme=''
    excep_msg=''
    def __init__(self, p_lookup_clm_nme='', p_excep_msg='', p_list_separator=' '):
        self.lookup_clm_nme = p_lookup_clm_nme
        self.excep_msg= p_excep_msg
        self.list_sep = p_list_separator

    def validate(self, p_df_to_validate, p_clm):
        JobContext.logger.info("Validating column {} for not null".format(p_clm))
        p_df_to_validate = self.add_exception_msg_clm(p_df_to_validate,p_clm, self.lookup_clm_nme,self.excep_msg,self.list_sep)
        l_df = p_df_to_validate.withColumn("exception_desc", sf.when(sf.col(p_clm).isNull(),
                                                                     sf.concat(sf.col('exception_desc'), sf.col(NotNullValidator.C_EXCEPTION_MSG_CLM_NAME))).otherwise(
            sf.col('exception_desc')))
        l_df = l_df.drop(NotNullValidator.C_EXCEPTION_MSG_CLM_NAME)
        JobContext.logger.info("Validated column {} for not null".format(p_clm))
        return l_df

    def add_exception_msg_clm(self, df, clm_under_validation, lookup_clm_nme, excep_msg, p_list_sep):
        msg= "{} is null|".format(clm_under_validation)


        if isinstance(lookup_clm_nme,list):
            cols = [sf.col(x) for x in lookup_clm_nme]
            return df.withColumn(NotNullValidator.C_EXCEPTION_MSG_CLM_NAME,
                             sf.concat(sf.lit(excep_msg),sf.concat_ws(p_list_sep,*cols), sf.lit('|')))

        if lookup_clm_nme:
            return df.withColumn(NotNullValidator.C_EXCEPTION_MSG_CLM_NAME,
                                 sf.concat(sf.lit(excep_msg),
                                           sf.when(sf.col(lookup_clm_nme).isNull(),sf.lit('')).otherwise(sf.col(lookup_clm_nme)),
                                           sf.lit('|')))
        elif excep_msg:
            return df.withColumn(NotNullValidator.C_EXCEPTION_MSG_CLM_NAME, sf.lit(excep_msg))
        else:
            return df.withColumn(NotNullValidator.C_EXCEPTION_MSG_CLM_NAME, sf.lit(msg))


class EqualValidator(Validator):
    validate_against = None
    exp_message = "{clm} is {against}|"

    def __init__(self, p_validate_against):
        self.validate_against = p_validate_against
        l_against = p_validate_against
        if not p_validate_against:
            l_against = 'empty'
        self.exp_message = self.exp_message.format(clm='{}', against=l_against)


    def validate(self, p_df_to_validate, p_clm):
        JobContext.logger.info("Validating column {} against equal value {}".format(p_clm, self.validate_against))
        l_df = p_df_to_validate.withColumn("exception_desc", sf.when(sf.col(p_clm) == self.validate_against,
                                                                     sf.concat(sf.col('exception_desc'), sf.lit(
                                                                         self.exp_message.format(p_clm,
                                                                                                 self.validate_against)))).otherwise(
            sf.col('exception_desc')))
        JobContext.logger.info("Validation of column is done")
        return l_df



class NotEqualValidator(Validator):
    validate_against = None
    exp_message = None
    c_default_exp_message = "{clm} does not have value in {against}|"

    def __init__(self, p_validate_against, exp_message = None):
        self.validate_against = p_validate_against
        #Replacing ',' with '|' as it will be generating CSV with separator as ','Ś

        if exp_message is not None:
            self.exp_message = exp_message
            # {} is present in mtdt_exclude table
        else:
            self.exp_message = NotEqualValidator.c_default_exp_message.format(clm='{}', against=p_validate_against).replace(',','|')

    def validate(self, p_df_to_validate, p_clm):
        JobContext.logger.info("Validating column {} against equal values {}".format(p_clm, self.validate_against))
        l_df = p_df_to_validate.withColumn("exception_desc", sf.when(~((sf.col(p_clm).isin(self.validate_against))
                                                                       & (sf.col(p_clm) == None)),
                                                                     sf.concat(sf.col('exception_desc'), sf.lit(
                                                                         self.exp_message), sf.col(p_clm),sf.lit('|'))).otherwise(
            sf.col('exception_desc')))
        JobContext.logger.info("Validation of column is done")
        return l_df


C_NOT_NULL_WITH_EMPTY_VALIDATOR_LIST = [NotNullValidator(), EqualValidator('')]
C_NOT_NULL_VALIDATOR_LIST = [NotNullValidator()]


def prepare_validation_map(tgt_clms, clms, date_clms=[], src_date_format=None):
    date_validator = [NotNullValidator(), EqualValidator(''),DateFormatValidator(src_date_format)]
    JobContext.logger.info(f"Preparing validation map for columns: {tgt_clms}, it will skip columns: {clms}, passed date columns {date_clms}")
    clm_validation_map = {}
    for clm in tgt_clms:
        if (clm in clms) :
            JobContext.logger.info(f"Not null validator added for column: {clm}")
            clm_validation_map[clm] = C_NOT_NULL_WITH_EMPTY_VALIDATOR_LIST

        if clm in date_clms:
            JobContext.logger.info(f"Date validator added for column: {clm}")
            clm_validation_map[clm] = date_validator
        else:
            JobContext.logger.info(f"skipping column {clm}")
            continue

    return clm_validation_map

