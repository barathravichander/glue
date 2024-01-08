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
        try:
            src_df = self.spark.read.text(posval_loc)
            # check if source is empty or not
            if (src_df is None) | (src_df.first() is None):
                self.logger.info("File is empty, so completing read and stage step job")
                return
        except Exception as e:
            JobContext.logger.warn("Fail to read file {}".format(posval_loc))
            return

        src_df = parse_fixed_len_data_with_start_len_skip_client_nbr_rcd_typ(src_df,self.C_POSVAL_COL_MAP)

        src_df = src_df.select(*self.C_POSVAL_REQ_COLUMN_LIST)

        src_df = src_df.withColumn("tag_ow_acct", sf.regexp_replace(sf.col("tag_ow_acct"), "\\s+", ""))

        self.logger.info("Converting quantities to decimal format")
        # create new column to indicate balance records, which will be used for qty_val_flg and instrument_id derivations
        src_df = src_df.withColumn("balance_rec" , sf.when(sf.col("tag_fiid")=='CASH', sf.lit(True)).otherwise(sf.lit(False))) \
            .withColumn("tag_fiid", sf.when(sf.col("balance_rec"), sf.lit(None)).otherwise(sf.col("tag_fiid"))) \
            .withColumn("qty_val_flg", sf.when(sf.col("balance_rec"), sf.lit('B')).otherwise(sf.lit('P')))
        src_df = src_df.withColumn("position_bal_qty",sf.col("tag_positionbalqty").cast(DecimalType(29,9))) \
            .withColumn("tag_marketvalueamt",sf.col("tag_marketvalueamt").cast(DecimalType(29,9))) \
            .withColumn("tag_acrdintamt",sf.col("tag_acrdintamt").cast(DecimalType(29,9))).drop("tag_positionbalqty")
        self.logger.info("Quantities are converted into decimal format")

        # derive account_id
        # add new columns to derive account-id from dtl_account_ref
        src_df = src_df.withColumn("ow_acct", sf.when(sf.col("TAG_OW_ACCT").isNotNull() & (sf.col("TAG_OW_ACCT")!=''), sf.col("TAG_OW_ACCT"))
                                   .otherwise(sf.col("TAG_ACC_N")))
        src_df = src_df.withColumn("acct_ref_type_cd", sf.when(sf.col("TAG_OW_ACCT").isNotNull() & (sf.col("TAG_OW_ACCT")!=''), sf.lit(C_ACCOUNT_REF_TYPE_CD_OW))
                                   .otherwise(sf.lit(C_ACCOUNT_REF_TYPE_CD_UAN)))

        # account_joining_condition = [sf.col('src.ow_acct') == sf.col('acc.sor_account_id')]
        # src_df = derive_account_id(src_df, account_joining_condition, True)
        l_dtl_account_ref_reqd_fields = ["account_id", "account_ref_type_cd", "account_ref_nm", "active_flg", "created_ts"]
        l_dtl_account_ref_df = read_from_s3_catalog_convert_to_df(JobContext.get_property(C_GLUE_S3_DB_NAME_PROP_KEY),
                                                                  JobContext.get_property(C_GLUE_S3_TABLE_DTL_ACCOUNT_REF_PROP_KEY), l_dtl_account_ref_reqd_fields, None)
        l_dtl_account_ref_df = transform_boolean_clm(l_dtl_account_ref_df, "active_flg")

        l_joining_cond = (src_df['ow_acct'] == l_dtl_account_ref_df['account_ref_nm']) & (src_df['acct_ref_type_cd'] == l_dtl_account_ref_df['account_ref_type_cd'])
        l_filter_cond = [sf.lit(C_ACCOUNT_REF_TYPE_CD_OW), sf.lit(C_ACCOUNT_REF_TYPE_CD_UAN)]

        src_df = derive_dtl_account_id(src_df, l_dtl_account_ref_df, l_joining_cond, l_filter_cond, p_broadcast=True)

        # derived dim_currency
        dim_currency_df = read_from_catalog_and_filter_inactive_rec(
            JobContext.get_property( C_GLUE_S3_TABLE_DIM_CURRENCY_PROP_KEY))
        src_df = lookup_dim_currency_currency_cd(src_df, dim_currency_df,"tag_valccycd", "currency_cd")

        # derive sleeve_id
        dim_sleeve_type_df = read_from_catalog_and_filter_inactive_rec( self._get_property(C_GLUE_S3_TABLE_DIM_SLEEVE_PROP_KEY))
        src_df = src_df.withColumn("sor_sleeve_id", sf.concat_ws("-",sf.col("ow_acct"),sf.col("tag_sleeve_id")))
        sleeve_joining_condition = src_df['sor_sleeve_id'] == dim_sleeve_type_df['sor_sleeve_id']
        src_df = derive_sleeve_id_without_filter_logic(src_df, dim_sleeve_type_df, sleeve_joining_condition)

        # if tag_sleeve_id is not supplied set it -1
        src_df = src_df.withColumn("sleeve_id", sf.when(sf.col("tag_sleeve_id").isNull() | (sf.col("tag_sleeve_id")==''), sf.lit(-1) ).otherwise(sf.col("sleeve_id")))

        # Adv_flg derivation
        src_df = src_df.withColumn("adv_flg", sf.when(sf.col("TAG_ADVISORYELIGCD") == 'A', sf.lit('ADV') ).otherwise(sf.lit(None)))
        # derive dim_account type TAG_SLEEVE_ID
        dim_account_type_df = read_from_catalog_and_filter_inactive_rec(self._get_property(C_GLUE_S3_TABLE_DIM_ACCOUNT_TYPE_PROP_KEY))
        sleeve_joining_condition = src_df['tag_accounttype'] == dim_account_type_df['account_type_desc']
        src_df = derive_account_type_id_filter_logic(src_df, dim_account_type_df, sleeve_joining_condition)
        src_df = src_df.withColumn("account_type_id", sf.when(sf.col("account_type_id").isNull(), sf.lit(1)).otherwise(sf.col("account_type_id")))



        # derive Instrument id
        dtl_instrument_df = read_from_s3_catalog_convert_to_df(self.C_DB_NAME, self._get_property(C_GLUE_S3_TABLE_DTL_INSTRUMENT_REF_PROP_KEY))
        dtl_instrument_df= transform_boolean_clm(dtl_instrument_df, "active_flg")
        #instrument_filter_cond = [sf.lit("OT")]

        #for insu
        stg_insu_pos_df = read_from_s3(self._get_property(C_S3_INSU_POSITION_STG_OUTPUT_LOCATION_PROP_KEY))
        src_partition_num = src_df.rdd.getNumPartitions()
        insu_src_df = src_df.where(sf.col("TAG_POSTYPECD") == C_CONV_POS_TYPE_FILTER_VAL)
        if stg_insu_pos_df is not None:
            stg_insu_pos_df = stg_insu_pos_df.select("tag_fiid","insu_ref_nm","tag_positionid").distinct()
            src_df = src_df.where(sf.col("TAG_POSTYPECD") != C_CONV_POS_TYPE_FILTER_VAL)

            insu_src_df = insu_src_df.alias("src").join(sf.broadcast(stg_insu_pos_df).alias("lkp"),(sf.col("src.tag_fiid")==sf.col("lkp.tag_fiid"))
                                                        & (sf.col("src.tag_positionid")==sf.col("lkp.tag_positionid")),"left") \
                .select(insu_src_df["*"],stg_insu_pos_df["insu_ref_nm"])

            insu_filter_cond = [sf.lit(C_INSTRUMENT_REF_TYPE_CD_INSU)]
            insu_joining_cond = insu_src_df['insu_ref_nm'] == dtl_instrument_df['instrument_ref_nm']
            insu_src_df = derive_instrument_id(insu_src_df, dtl_instrument_df, insu_joining_cond, self.g_batch_date_clm,insu_filter_cond)

        instrument_filter_cond = [sf.lit(C_INSTRUMENT_REF_TYPE_CD_OT),sf.lit(C_INSTRUMENT_REF_TYPE_CD_FIID)]
        g_joining_cond = src_df['tag_fiid'] == dtl_instrument_df['instrument_ref_nm']
        src_df = derive_instrument_id(src_df, dtl_instrument_df, g_joining_cond, self.g_batch_date_clm,instrument_filter_cond)

        if stg_insu_pos_df is not None:
            l_src_clm = src_df.columns
            src_df = src_df.select(*l_src_clm).union(insu_src_df.select(*l_src_clm))
            src_df = src_df.coalesce(src_partition_num)

        # using different column for instrument_id ad for balance instrument_id would be null, and we are passing all dataframe for validation, so created new
        # column for instrument_id_position for instrument_id, and set default for balance rec, and we will use instrument_id_position for not null validation
        src_df = src_df.withColumn("instrument_id_position", sf.when(sf.col("balance_rec"), sf.lit("default")).otherwise(sf.col("instrument_id")))


        target_df = src_df.withColumn("effective_dt", sf.to_date(sf.col("tag_valuationdate"), self.C_POS_VAL_DATE_FORMAT) ).withColumn("firm_id", sf.lit(C_FIRM_ID))

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

        # Fact_mv_multiplier as only position records has Instrument information
        self.logger.info("Selecting Position records for Fact MV Multiplier")
        abc = target_df.schema.names
        self.logger.info(f"This is query plan: {abc}")
        target_df.persist(StorageLevel.DISK_ONLY)

        stg_fact_holding_df = target_df.withColumnRenamed('position_bal_qty','quantity_val').select("account_id","instrument_id","sleeve_id","account_type_id","currency_cd","adv_flg","effective_dt","quantity_val")
        save_output_to_s3(stg_fact_holding_df,"stg_fact_holding_df", "s3://br-uniprudev-us-east-1-ubsfi-glue-dev-store-s3/pr_ubsfi/glue_app/vikas/posval_test/stg_fact_holding_df")

        target_df = target_df.filter(~sf.col("balance_rec"))
        save_output_to_s3(target_df,"stg_fact_holding_df", "s3://br-uniprudev-us-east-1-ubsfi-glue-dev-store-s3/pr_ubsfi/glue_app/vikas/posval_test/target_df")



def main():
    job = PosvalFactMVMultiplier()
    job.execute()


if __name__ == '__main__':
    main()
