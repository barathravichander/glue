import sys
import pyspark.sql.functions as sf

from constant.common_constant import *
from constant.property_constant import *
from awsglue import DynamicFrame
from utils.GlueJobBase import GlueJobBase
from utils.common_util import trim_space, add_audit_columns, prepare_exception_clm_list, \
    parse_fixed_len_data_with_start_len_skip_client_nbr_rcd_typ
from utils.common_derivations_util import *
from utils.data_validations_util import NotNullValidator, validate_rec_and_filter, C_NOT_NULL_WITH_EMPTY_VALIDATOR_LIST
from utils.s3_operations_util import read_from_s3, save_output_to_s3, read_from_s3_catalog_convert_to_df

###############################
#  Implementation class for POSVAL to Fact_Holding Conversion and Fact MV Multiplier
###############################

class PosvalFactMVMultiplier(GlueJobBase):

    C_POSVAL_COL_MAP = {"TAG_OW_ACCT":[1,10],"TAG_SLEEVE_ID":[11,4],"TAG_ACC_N":[15,10],"TAG_PORTFOLIOID":[25,16],
                        "TAG_VALUATIONDATE":[41,10],"TAG_FIID":[51,16],"TAG_ACCOUNTTYPE":[67,2],"TAG_POSITIONID":[69,16],
                        "TAG_ADVISORYELIGCD":[85,1],"TAG_VALCCYCD":[86,3],"TAG_POSTYPECD":[89,6],"TAG_POSITIONBALQTY":[95,31],
                        "TAG_MARKETVALUEAMT":[126,31],"TAG_ACRDINTAMT":[157,31],"TAG_TOTALVALUEAMT":[188,31]}
    C_POSVAL_COLUMN_LIST= ['TAG_OW_ACCT', 'TAG_SLEEVE_ID', 'TAG_ACC_N', 'TAG_PORTFOLIOID', 'TAG_VALUATIONDATE', 'TAG_FIID', 'TAG_ACCOUNTTYPE',
                           'TAG_POSITIONID', 'TAG_ADVISORYELIGCD', 'TAG_VALCCYCD', 'TAG_POSTYPECD', 'TAG_POSITIONBALQTY', 'TAG_MARKETVALUEAMT',
                           'TAG_ACRDINTAMT', 'TAG_TOTALVALUEAMT']
    C_POSVAL_REQ_COLUMN_LIST= ['TAG_OW_ACCT', 'TAG_SLEEVE_ID', 'TAG_ACC_N', 'TAG_VALUATIONDATE', 'TAG_FIID', 'TAG_ACCOUNTTYPE', 'TAG_VALCCYCD',
                               'TAG_POSITIONBALQTY', 'TAG_ADVISORYELIGCD', 'TAG_MARKETVALUEAMT', 'TAG_ACRDINTAMT', 'TAG_POSTYPECD','TAG_POSITIONID']
    C_FACT_HOLDING_CLMS = ['account_id', 'instrument_id', 'sleeve_id', 'account_type_id', 'currency_cd', 'effective_dt', 'qty_val_flg', 'quantity_val','adv_flg', 'firm_id', 'active_flg', 'batch_dt', 'created_program_nm', 'created_user_nm', 'created_ts']

    C_FACT_MV_MULTI_CLMS= ['instrument_id','currency_cd','effective_dt','actual_dt','source_system_id','multiplier_type_cd',
                           'multiplier_val','firm_id','active_flg','batch_dt','created_program_nm','created_user_nm','created_ts']

    C_POS_VAL_DATE_FORMAT="MM/dd/yyyy"
    C_PR_QTY_VAL_FLG_FACT_HOLDING = 'MV'
    C_AI_QTY_VAL_FLG_FACT_HOLDING = 'AI'
    C_PR_FACT_MV_MULTI_MULTIPLIER_TYPE_CD = "PR"
    C_AI_FACT_MV_MULTI_MULTIPLIER_TYPE_CD = "AI"

    C_FACT_HOLDING_FIELD_TO_VALIDATE_AGAINST = {
        "account_id": [NotNullValidator('ow_acct', 'Supplied account reference not found in the system :')],
        "sleeve_id": [NotNullValidator('sor_sleeve_id', 'Supplied sleeve reference not found in the system :')],
        "account_type_id": [NotNullValidator('tag_accounttype', 'Supplied account type reference not found in the system :')],
        "currency_cd": [NotNullValidator('tag_valccycd', 'Supplied currency reference not found in the system :')],
        "instrument_id_position": [NotNullValidator('tag_fiid', 'Supplied instrument reference not found in the system :')],
        "effective_dt": C_NOT_NULL_WITH_EMPTY_VALIDATOR_LIST
    }

    C_FACT_MV_MULTIPLIER_FIELD_TO_VALIDATE_AGAINST = {
        "currency_cd": [NotNullValidator('tag_valccycd', 'Supplied currency reference not found in the system :')],
        "instrument_id_position": [NotNullValidator('tag_fiid', 'Supplied instrument reference not found in the system :')],
        "effective_dt": C_NOT_NULL_WITH_EMPTY_VALIDATOR_LIST
    }
    C_DEFAULT_VALUE_FOR_MULTI = -999999
    C_YEARS_JOB_PARAM= "YEAR"
    C_CREATED_PRM_NM_PREFIX = "CONV_POSVAL_{}"

    def _get_job_parameter_option_names(self):
        l_prop_list = super(PosvalFactMVMultiplier, self)._get_job_parameter_option_names()
        l_addtional_prop_list =[self.C_YEARS_JOB_PARAM]
        l_prop_list = l_prop_list + l_addtional_prop_list
        return l_prop_list


    def _get_logger_name(self):
        return "conv_posval_fact_mv_multiplier";

    def _get_file_type(self):
        return "POSVAL";

    def _execute_job(self):
        self.logger.info("Starting Posval to Fact MV Multiplier Conversion")
        year = self._get_property(self.C_YEARS_JOB_PARAM)
        self.logger.info("Years passed for Posval is {}".format(year))
        posval_loc = self._get_property(C_S3_POSVAL_INPUT_LOCATION_PROP_KEY).replace('*', year)
        loc= "s3://br-uniprudev-us-east-1-ubsfi-glue-dev-store-s3/pr_ubsfi/glue_app/vikas/posval_test/position_target_df/POSVAL/*/".replace('*', year)
        try:
            target_df = JobContext.glue_context.spark_session.read.csv(loc, header=True,sep=",")

            # check if source is empty or not
            if (target_df is None) | (target_df.first() is None):
                self.logger.info("File is empty, so completing read and stage step job")
                return
        except Exception as e:
            JobContext.logger.warn("Fail to read file {}".format(posval_loc))
            return

        position_target_df = target_df.withColumn("valid_pr", sf.col("sum_multiplier_val_pr")==0.0).withColumn("valid_ai", sf.col("sum_multiplier_val_ai")==0.0)
        #  To create PR and AI records we are cross joinning, which will create 2 records for each records
        # If it validate PR then pick multiplier_val_pr, if its vaild AI them pick multiplier_val_ai, else use default negative values

        fact_mv_mult_flag_df = self.spark.createDataFrame([(1,  self.C_PR_FACT_MV_MULTI_MULTIPLIER_TYPE_CD), (1, self.C_AI_FACT_MV_MULTI_MULTIPLIER_TYPE_CD)],["row_num",'multiplier_type_cd'])

        self.logger.info("Selecting first row for Fact Mv Multiplier")
        position_target_df_number_of_partition = position_target_df.rdd.getNumPartitions()
        # After pikcing first record, number of records in partition would be less, so reducing the partition
        new_partition = int(position_target_df_number_of_partition/3)

        # if its very small file, new parition would be 0
        if new_partition==0:
            new_partition =1

        # caching it it will be used for Price and AI analysis, not using s3_utils method as it will add year also in path
        def save_output_to_s3(p_data_frame, p_name_of_dyanmic, p_location, p_format="csv"):

            JobContext.logger.info("Saving data at S3 location {} in format {}".format(p_location, p_format))
            if p_location.endswith("/") == False:
                p_location = p_location +  "/"

            year = self._get_property(self.C_YEARS_JOB_PARAM)
            l_location = p_location + JobContext.file_being_processed +"/"  + year
            import boto3
            s3 = boto3.resource('s3')

            chunked_loc = l_location.split("/",3)
            s3_buket = chunked_loc[2]
            prefix= chunked_loc[3]
            self.logger.info("From location found S3 bucket: {}, prefix: {}".format(s3_buket,prefix))
            bucket = s3.Bucket(s3_buket)
            """
            this will avoid delete unwanted dir Ex:
            Given prefix .../posval
            preset dir
            .../posval
            .../posval1
            .../posval2
            In this case it will delete all dir starts with posval
            """
            if ~prefix.endswith("/"):
                prefix = prefix +  "/"
                self.logger.info("Added backslash in prefix")
            self.logger.info("Delete {} directory from S3 buket {}".format(prefix,s3_buket))
            bucket.objects.filter(Prefix=prefix).delete()
            l_dynamic_frame = DynamicFrame.fromDF(p_data_frame, JobContext.glue_context, p_name_of_dyanmic)
            JobContext.glue_context.write_dynamic_frame.from_options(frame=l_dynamic_frame, connection_type="s3",
                                                                     connection_options={"path": l_location}, format=p_format,
                                                                     format_options={"quoteChar": -1})
            JobContext.logger.info("Saved Dataframe to S3 location")

        self.logger.info("Current number of partition is {}, reducing it to {} ".format(position_target_df_number_of_partition, new_partition))
        fact_mv_mult_df = position_target_df.filter(sf.col("row_num")==1).coalesce(new_partition)
        self.logger.info("Joining fact_mv_multiplier with flag DF")
        fact_mv_mult_df = fact_mv_mult_df.join(sf.broadcast(fact_mv_mult_flag_df), (fact_mv_mult_flag_df["row_num"] == fact_mv_mult_df['row_num']) ,"left_outer" ).select(fact_mv_mult_df['*'], fact_mv_mult_flag_df['multiplier_type_cd'])

        self.logger.info("Joined fact_mv_multiplier with flag DF")

        # Derive quantity value
        self.logger.info("Deriving multiplier_val for fact_mv_multiplier")
        fact_mv_mult_df= fact_mv_mult_df.withColumn("multiplier_val",
                                                sf.when((sf.col("multiplier_type_cd") ==  self.C_PR_FACT_MV_MULTI_MULTIPLIER_TYPE_CD) & sf.col("valid_pr"), sf.col("multiplier_val_pr")). \
                                                when((sf.col("multiplier_type_cd") == self.C_AI_FACT_MV_MULTI_MULTIPLIER_TYPE_CD) & sf.col("valid_ai"), sf.col("multiplier_val_ai")).
                                                otherwise(self.C_DEFAULT_VALUE_FOR_MULTI))

        self.logger.info("Derived quantity_val  for fact_mv_multiplier")
        created_prg_nm =  self.C_CREATED_PRM_NM_PREFIX.format(year)
        src_sys_id = derive_src_sys_id(C_POSVAL_CLIENT_NBR)
        fact_mv_mult_df = fact_mv_mult_df.withColumn("source_system_id", sf.lit(src_sys_id)).withColumn("actual_dt", sf.lit(None))

        fact_mv_mult_df = add_audit_columns(fact_mv_mult_df, self.g_batch_date_clm, created_user_nm = C_CONVERSION_USER_NM,created_progrm_nm = created_prg_nm)

        fact_mv_mult_df = validate_rec_and_filter(fact_mv_mult_df, self.C_FACT_MV_MULTIPLIER_FIELD_TO_VALIDATE_AGAINST,
                                              "s3://br-uniprudev-us-east-1-ubsfi-glue-dev-store-s3/pr_ubsfi/glue_app/vikas/posval_test/fact_mv_mult_df/",
                                              prepare_exception_clm_list(self.C_FACT_MV_MULTI_CLMS),stg_type=StorageLevel.DISK_ONLY).select(*self.C_FACT_MV_MULTI_CLMS)

        save_output_to_s3(fact_mv_mult_df,"fact_mv_mult_df", "s3://br-uniprudev-us-east-1-ubsfi-glue-dev-store-s3/pr_ubsfi/glue_app/vikas/posval_test/fact_mv_mult_df/")


        # Fact Holding Records
        position_target_df_number_of_partition = position_target_df.rdd.getNumPartitions()
        # As source will have less number of records for fact_holdings so reduce partition by /5
        new_partition = int(position_target_df_number_of_partition/5)
        if new_partition==0:
            new_partition =1
        self.logger.info("Current number of partition is {}, reducing it to {} ".format(position_target_df_number_of_partition, new_partition))
        fact_holding_df = position_target_df.drop("qty_val_flg").filter( ~sf.col("valid_pr") | ~sf.col("valid_ai")).coalesce(new_partition)
        # Dummy dataframe to create two record for each records which failed Multiplier calculation
        # it has four records for each possible scenarios
        fact_holding_flag_df = self.spark.createDataFrame([(False,False,  self.C_PR_QTY_VAL_FLG_FACT_HOLDING), (False, False, self.C_AI_QTY_VAL_FLG_FACT_HOLDING),
                                                       (False,True,  self.C_PR_QTY_VAL_FLG_FACT_HOLDING), (True, False, self.C_AI_QTY_VAL_FLG_FACT_HOLDING)], ["valid_pr", "valid_ai", 'qty_val_flg'])

        fact_holding_df = fact_holding_df.join(sf.broadcast(fact_holding_flag_df), (fact_holding_flag_df["valid_pr"] == fact_holding_df['valid_pr']) & (fact_holding_flag_df["valid_ai"] ==fact_holding_df['valid_ai'])).select(fact_holding_df['*'], fact_holding_flag_df['qty_val_flg'])

        # Derive quantity value
        self.logger.info("Deriving quantity_val for fact_holding")
        fact_holding_df= fact_holding_df.withColumn("quantity_val",
                                                sf.when((sf.col("qty_val_flg") == self.C_PR_QTY_VAL_FLG_FACT_HOLDING) & (~sf.col("valid_pr")), sf.col("TAG_MARKETVALUEAMT")). \
                                                when((sf.col("qty_val_flg") == self.C_AI_QTY_VAL_FLG_FACT_HOLDING) & (~sf.col("valid_ai")), sf.col("TAG_ACRDINTAMT")))

        self.logger.info("Derived quantity_val for fact_holding")

        fact_holding_df = add_audit_columns(fact_holding_df, self.g_batch_date_clm, created_user_nm = C_CONVERSION_USER_NM,created_progrm_nm = created_prg_nm)
        fact_holding_df = validate_rec_and_filter(fact_holding_df, self.C_FACT_HOLDING_FIELD_TO_VALIDATE_AGAINST,
                                              "s3://br-uniprudev-us-east-1-ubsfi-glue-dev-store-s3/pr_ubsfi/glue_app/vikas/a_529_inception_Date/fact_holding_df_v",
                                              prepare_exception_clm_list(self.C_FACT_HOLDING_CLMS),stg_type=StorageLevel.DISK_ONLY).select(*self.C_FACT_HOLDING_CLMS)

        save_output_to_s3(fact_holding_df,"fact_holding_df","s3://br-uniprudev-us-east-1-ubsfi-glue-dev-store-s3/pr_ubsfi/glue_app/vikas/posval_test/fact_holding_df/")
def main():
    job = PosvalFactMVMultiplier()
    job.execute()


if __name__ == '__main__':
    main()
