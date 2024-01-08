from constant.common_constant import C_OPERATION_NOT_SUPPORTED
from pyspark.sql import functions as sf
from pyspark.sql.types import *
from constant.common_constant import *
from utils.JobContext import JobContext
from utils.s3_operations_util import save_output_to_s3


###############################
#  This method will validate and return vaild records and it will  store  invaild records
###############################
def _validate_rec_and_filter(p_df_to_validate, p_clmn_to_validate_against, p_s3_location, exception_output_clms=None):
    JobContext.logger.info(
        "Starting Validating Dataframe column against:{},  invaild records output S3 Location: {}".format(
            p_clmn_to_validate_against, p_s3_location))
    p_df_to_validate = validate_rec(p_df_to_validate, p_clmn_to_validate_against)
    p_df_to_validate.cache()
    l_df_with_valid_rec = p_df_to_validate.filter(p_df_to_validate.exception_desc == '|')
    l_df_with_invalid_rec = p_df_to_validate.filter("exception_desc != '|'")
    l_df_with_invalid_rec = add_default_value_to_invalid_rec(l_df_with_invalid_rec, JobContext.file_being_processed,
                                                             'E', 'N')

    # only selecte the give columns
    if exception_output_clms is not None and len(exception_output_clms) != 0:
        l_df_with_invalid_rec = l_df_with_invalid_rec.select(*exception_output_clms)

    #TODO: Use partition and use Glue Catalog also
    #TODO: REmove teh header when
    save_output_to_s3(l_df_with_invalid_rec, "df_with_invalid_rec", p_s3_location)
    JobContext.logger.info("Validation Of Dataframe  done")
    return l_df_with_valid_rec.drop("exception_desc")


###############################
#  This method will  iterate over passed map and new column to validate the records
###############################
def validate_rec(p_df_to_validate, p_clmn_to_validator):
    JobContext.logger.info("Starting Validating records column to validate against:{}".format(p_clmn_to_validator))
    p_df_to_validate = p_df_to_validate.withColumn("exception_desc", sf.lit('|'))
    for l_validator,l_clm in p_clmn_to_validator.items():
        p_df_to_validate = validate_clm(l_validator, p_df_to_validate, l_clm)
    JobContext.logger.info("Validation Of Dataframe record is done")
    return p_df_to_validate


###############################
#  This method will  check passed parameter  and  take action accordingly
###############################
def validate_clm(p_validator, p_df_to_validate, p_clm):
    JobContext.logger.info("Validating column {} with {}".format(p_clm, p_validator))
    p_df_to_validate = p_validator.validate(p_df_to_validate, p_clm)
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
class DateFormatValidator1(Validator):
    format = ""

    def __init__(self, p_format):
        self.format = p_format

    def validate(self, p_df_to_validate, p_clm_list):
        l_df = p_df_to_validate
        for p_clm in p_clm_list:
            JobContext.logger.info("Validating column {} for with date format {}".format(p_clm, self.format))
            l_df = l_df.withColumn("validated_date", sf.to_date(sf.col(p_clm), self.format)).withColumn(
                "exception_desc", sf.when((sf.col(p_clm).isNotNull() & sf.col("validated_date").isNull()),
                                          sf.concat(sf.col('exception_desc'), sf.lit(
                                              "{} is not a validate date as per format {}|".format(p_clm,
                                                                                                   self.format)))).otherwise(
                    sf.col('exception_desc')))
            JobContext.logger.info("Validated column {} for with date format {}".format(p_clm, self.format))
        return l_df


class NotNullValidator1(Validator):

    # using complex name, so that it does not collide with exiting names
    C_EXCEPTION_MSG_CLM_NAME= "exception_msg_notnullvalidator"
    lookup_clm_nme=''
    excep_msg=''
    def __init__(self, p_lookup_clm_nme='', p_excep_msg=''):
        self.lookup_clm_nme = p_lookup_clm_nme
        self.excep_msg= p_excep_msg

    def validate(self, p_df_to_validate, p_clm_list):
        l_df = p_df_to_validate
        for p_clm in p_clm_list:
            JobContext.logger.info("Validating column {} for not null".format(p_clm))
            l_df = self.add_exception_msg_clm(p_df_to_validate,p_clm, self.lookup_clm_nme,self.excep_msg)
            l_df = l_df.withColumn("exception_desc", sf.when(sf.col(p_clm).isNull(),
                                                                         sf.concat(sf.col('exception_desc'), sf.col(NotNullValidator1.C_EXCEPTION_MSG_CLM_NAME))).otherwise(
                sf.col('exception_desc')))
            l_df = l_df.drop(NotNullValidator1.C_EXCEPTION_MSG_CLM_NAME)
            JobContext.logger.info("Validated column {} for not null".format(p_clm))
        return l_df

    def add_exception_msg_clm(self, df, clm_under_validation, lookup_clm_nme, excep_msg):
        msg= "{} is null|".format(clm_under_validation)

        if lookup_clm_nme:
            return df.withColumn(NotNullValidator1.C_EXCEPTION_MSG_CLM_NAME, sf.concat(sf.lit(excep_msg), sf.col(lookup_clm_nme),sf.lit('|')))
        elif excep_msg:
            return df.withColumn(NotNullValidator1.C_EXCEPTION_MSG_CLM_NAME, sf.lit(excep_msg))
        else:
            return df.withColumn(NotNullValidator1.C_EXCEPTION_MSG_CLM_NAME, sf.lit(msg))


class EqualValidator1(Validator):
    validate_against = None
    exp_message = "{clm} is {against}|"

    def __init__(self, p_validate_against):
        self.validate_against = p_validate_against
        l_against = p_validate_against
        if not p_validate_against:
            l_against = 'empty'
        self.exp_message = self.exp_message.format(clm='{}', against=l_against)


    def validate(self, p_df_to_validate, p_clm_list):
        l_df = p_df_to_validate
        for p_clm in p_clm_list:
            JobContext.logger.info("Validating column {} against equal value {}".format(p_clm, self.validate_against))
            l_df = l_df.withColumn("exception_desc", sf.when(sf.col(p_clm) == self.validate_against,
                                                                         sf.concat(sf.col('exception_desc'), sf.lit(
                                                                             self.exp_message.format(p_clm,
                                                                                                     self.validate_against)))).otherwise(
                sf.col('exception_desc')))
            JobContext.logger.info("Validation of column is done")
        return l_df



class NotEqualValidator(Validator):
    validate_against = None
    exp_message = "{clm} does not have value in {against}|"

    def __init__(self, p_validate_against):
        self.validate_against = p_validate_against
        #Replacing ',' with '|' as it will be generating CSV with separator as ','
        self.exp_message = self.exp_message.format(clm='{}', against=p_validate_against).replace(',','|')

    def validate(self, p_df_to_validate, p_clm_list):
        for p_clm in p_clm_list:
            JobContext.logger.info("Validating column {} against equal values {}".format(p_clm, self.validate_against))
            l_df = p_df_to_validate.withColumn("exception_desc", sf.when(~sf.col(p_clm).isin(self.validate_against),
                                                                         sf.concat(sf.col('exception_desc'), sf.lit(
                                                                             self.exp_message.format(p_clm)))).otherwise(
                sf.col('exception_desc')))
            JobContext.logger.info("Validation of column is done")
        return l_df
