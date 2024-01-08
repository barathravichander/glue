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
        loc= "s3://br-uniprudev-us-east-1-ubsfi-glue-dev-store-s3/pr_ubsfi/glue_app/vikas/posval_test/target_df/POSVAL/*/".replace('*', year)
        try:
            target_df = JobContext.glue_context.spark_session.read.csv(loc, header=True,sep=",")

            # check if source is empty or not
            if (target_df is None) | (target_df.first() is None):
                self.logger.info("File is empty, so completing read and stage step job")
                return
        except Exception as e:
            JobContext.logger.warn("Fail to read file {}".format(posval_loc))
            return



        target_df = target_df.withColumn("TAG_MARKETVALUEAMT",sf.col("TAG_MARKETVALUEAMT").cast(DecimalType(29,9))) \
            .withColumn("position_bal_qty",sf.col("position_bal_qty").cast(DecimalType(29,9))) \
            .withColumn("TAG_ACRDINTAMT",sf.col("TAG_ACRDINTAMT").cast(DecimalType(29,9))).drop("tag_positionbalqty")

        position_target_df = target_df.withColumn("multiplier_val_pr", sf.col("TAG_MARKETVALUEAMT")/sf.col("position_bal_qty")) \
            .withColumn("multiplier_val_ai", sf.col("TAG_ACRDINTAMT")/sf.col("position_bal_qty"))

        plan_window = Window.partitionBy('instrument_id','effective_dt')
        window_order = plan_window.orderBy(sf.asc('position_bal_qty'))

        """
         We will add following column using window function partition based on instrument and effective date and order the records based on TAG_POSITIONBALQTY
          - lag_clm: This will represent previous row's value, for first records it will return Null, we are passing negative_Val to return instead of Null
          
          - delta_clm: This will be difference of lag_clm column and currenct row's value column
          - sum_clm:  summing the delta_clm based on same window but without order, as adding order would give us cumulative sum
          - Now if sum_clm is 0 it means all rows have same values
          
           For all instrument and effective date we will be inserting only one record, so we will add new row row_num to pick first record
        """

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

        position_target_df_number_of_partition = position_target_df.rdd.getNumPartitions()
        position_target_df = position_target_df.repartition(position_target_df_number_of_partition,'instrument_id','effective_dt' )
        position_target_df = self.calculate_multiplier(position_target_df, "multiplier_val_pr", window_order).withColumn("row_num", sf.row_number().over(window_order))

        position_target_df = self.calculate_multiplier(position_target_df, "multiplier_val_ai", window_order)
        save_output_to_s3(position_target_df, "" ,"s3://br-uniprudev-us-east-1-ubsfi-glue-dev-store-s3/pr_ubsfi/glue_app/vikas/posval_test/position_target_df/")

        self.logger.info("Completed Posval to Fact Holding Conversion and Fact MV Multiplier ")


    def calculate_multiplier(self, src_df, clm_nme,window):
        JobContext.logger.info("Starting calculation multiplier for column {}".format(clm_nme))

        # generate column names
        leg_clm_nme = "lag_{}".format(clm_nme)
        sum_clm_nm = "sum_{}".format(clm_nme)
        delta_clm_nme = "delta_{}".format(clm_nme)

        window_sum = Window.partitionBy('instrument_id','effective_dt')

        src_df = src_df.withColumn(leg_clm_nme, sf.lag(clm_nme, default=self.C_DEFAULT_VALUE_FOR_MULTI).over(window)) \
            .withColumn(delta_clm_nme,
                        sf.when(sf.col(leg_clm_nme) == self.C_DEFAULT_VALUE_FOR_MULTI, sf.lit(0))
                        .when(sf.col(leg_clm_nme).isNull() | (sf.col(leg_clm_nme) == ''), sf.lit(self.C_DEFAULT_VALUE_FOR_MULTI))
                        .otherwise(sf.col(leg_clm_nme) - sf.col(clm_nme))) \
            .withColumn(sum_clm_nm, sf.sum(sf.abs(sf.col(delta_clm_nme))).over(window_sum))
        JobContext.logger.info("Completed calculation multiplier for column {}".format(clm_nme))

        return src_df

def main():
    job = PosvalFactMVMultiplier()
    job.execute()


if __name__ == '__main__':
    main()
