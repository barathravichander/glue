###############################
# This script wil holds various common derivations
# --------------------------------------------------------------------------
#                       History
# Date            Version No    Developer         Change                                                                                            
# -------           ------       ---------         ------
# 17 Aug 2021       1             Akhil Joe         Added additional filter for source_system_id
# 22/03/2022        2             Uday V            Added fix for dim_account_link lookup to not consider the accounts
#                                                   which are ended at derive_account_id_method.
# 27/04/2022        3             Uday V            Added two new methods, to fix adp issue.
# 09/06/2022        4             Uday V            Added fix to dim_instrument logic.
# 16/06/2022        5             Rakesh Ch         Added a new method  dim_instrument_duplicates_removal to
#                                                   eliminate duplicates loading into dim_instrument.
# 07/04/2022        6             Uday V            Added fix for _create_dim_instrument_stub method, removed stub logic
#                                                   which is unnecessary.
# 07/14/2022        7             Uday V            Added a fix for lookup_mtdt_exclude_account method to exclude
#                                                   invalid accounts.
# 10 AUG 2022       8             Uday V            Added fix to derive_dtl_inst_ref_insert_update_expired method to
#                                                   eliminate invalid adp update.
# 31 OCT 2022       9             Uday V            Added fix to derive_dtl_inst_ref_expired method, now loading missing
#                                                   classification records to updated new securities from expired
#                                                   securities previously to fact_class_entity table.
# 31 OCT 2022      10             Uday V            Added fix to derive_instrument_id method, missing securities fix.

# --------------------------------------------------------------------------
###############################
from pyspark import StorageLevel
from pyspark.sql.types import DoubleType, DateType, DecimalType, StructField, StructType, StringType
from utils.JobContext import JobContext
from utils.common_util import transform_boolean_clm, format_date, add_default_clm, add_audit_columns, \
    prepare_exception_clm_list, lower_case, isNull_check
from utils.data_validations_util import NotNullValidator, validate_rec_and_filter, NotEqualValidator, C_NOT_NULL_WITH_EMPTY_VALIDATOR_LIST
from utils.s3_operations_util import read_from_s3_catalog_convert_to_df, read_from_catalog_and_filter_inactive_rec, read_from_s3,save_output_to_s3
from constant.common_constant import *
from constant.property_constant import *
from pyspark.sql import functions as sf
from pyspark.sql.window import Window

C_DIM_ACC_CLMS=["account_id","sor_account_id","source_system_id","account_nm","currency_cd","tax_status_cd","load_cache_flg","fiscal_year_end","firm_id",
                "active_flg","batch_dt","created_program_nm","created_user_nm","created_ts","modified_program_nm","modified_user_nm","modified_ts"]

C_DIM_ACC_DEFAULT_MAPPING={"source_system_id":"","currency_cd":"","tax_status_cd":"","load_cache_flg":"","fiscal_year_end":"","start_dt":"1753-01-01"
    ,"end_dt":"9999-12-31","firm_id":"1"}

C_DIM_INSTRUMENT_DEFAULT_COL_LIST= {'issue_dt': None, 'maturity_dt': None, 'first_accrual_dt': None, 'first_coupon_dt': None, 'pre_refund_dt': None, 'instrument_desc': None,
                                    'instrument_name_1': None,'instrument_name_2': None, 'constituent_benchmark_id': None, 'constituent_benchmark_start_dt': None,
                                    'constituent_benchmark_end_dt': None,'constituent_benchmark_prct': None, 'qty_factor_val': None, 'interest_rt': None, 'coupon_freq_cd': None,
                                    'pending_payment_days': None, 'opt_call_put_flg': None,'opt_strike_price_amt': None, 'underlying_instrument_id': None, 'year_count_cd': None, 'day_count_cd': None,
                                    'interest_rate_type_cd': None,"instrument_type_cd": "SEC","firm_id": C_FIRM_ID}

C_DTL_INSTRUMENT_REF_DEFAULT_COL_LIST = {"firm_id": C_FIRM_ID,"modified_program": None, "modified_user":None,
                                         "modified_ts":None}

C_DTL_FIELD_TO_VALIDATE_AGAINST = {"instrument_id": C_NOT_NULL_WITH_EMPTY_VALIDATOR_LIST,
                                   "instrument_ref_nm":C_NOT_NULL_WITH_EMPTY_VALIDATOR_LIST}
C_DIM_INST_FIELDS_TO_VALIDATE_AGAINST = {"instrument_id": C_NOT_NULL_WITH_EMPTY_VALIDATOR_LIST}

C_FACT_CLASS_ENTITY_DEFAULT_COL_LIST= {'entity_type_cd':'SECURITY','overriding_entity_type_cd':None,
                                       'allocation_prcnt':None,'overriding_entity_id':None,'firm_id':1}

C_FACT_CLASS_ENTITY_COL_LIST = ['entity_id','class_type_id','entity_type_cd','overriding_entity_type_cd',
                                'overriding_entity_id','effective_from_dt','effective_to_dt','allocation_prcnt',
                                'firm_id','active_flg','batch_dt','created_program_nm','created_user_nm','created_ts']
C_FACT_CLASS_ENTITY_FIELD_TO_VALIDATE_AGAINST = {'entity_id': C_NOT_NULL_WITH_EMPTY_VALIDATOR_LIST,
                                                 'class_type_id': C_NOT_NULL_WITH_EMPTY_VALIDATOR_LIST,
                                                 'entity_type_cd': C_NOT_NULL_WITH_EMPTY_VALIDATOR_LIST}

###############################
# This will accept property_id and derive currency_cd
###############################
def derive_currency_cd():
    JobContext.logger.info("Deriving  currency code ")

    fact_setting_df = read_from_catalog_and_filter_inactive_rec(JobContext.get_property(C_GLUE_S3_TABLE_FACT_SETTING_PROP_KEY))
    dim_prop_val_df = read_from_catalog_and_filter_inactive_rec(JobContext.get_property(C_GLUE_S3_TABLE_DIM_PROPERTY_VALUE_PROP_KEY))
    dim_prop_df = read_from_catalog_and_filter_inactive_rec(JobContext.get_property(C_GLUE_S3_TABLE_DIM_PROPERTY_POP_KEY))
    prop_val_df = fact_setting_df.alias('fs').join(dim_prop_val_df.alias('dpv'), 'property_value_id').join(dim_prop_df.alias('dp'), 'property_id')
    curr_cd_row = prop_val_df.filter(prop_val_df.property_desc == C_BASE_CURRENCY_PROP_DESC).select(sf.col('dpv.property_value_desc').alias('Currency_cd')).first()

    if curr_cd_row is None:
        raise Exception("Corresponding Currency code not found")

    currency_cd =  curr_cd_row.asDict()['Currency_cd']
    JobContext.logger.info("Derived Currency code: {} ".format(currency_cd))
    return currency_cd

###############################
#  This method will add new column for insturment_id, below are it
#     :param p_src_df: source Data frame
#    :param p_dtl_ins_df: Dtl instrument dataframe
#    :param p_joining_cond: This is condition on which these two DF will be joined
#               Ex. [g_fact_set_df['ubs_id'] == g_dtl_instrument_ref_df['instrument_ref_nm']]
#   :param: p_instrument_filter_cond: This list of instrument_type_cd_ref, which need to select
###############################

def derive_instrument_id( p_src_df, p_dtl_ins_df, p_joining_cond, p_batch_date, p_instrument_filter_cond =None,
                         clm_to_use_for_dt_filter =None,derived_instrument_id_alias= 'instrument_id', dim_instrument_df= None,broadcast=True):
    JobContext.logger.info("Deriving  instrument id")
    # picking the latest records as per batch date
    if dim_instrument_df is None:
        JobContext.logger.info("Reading Dim_instrument and filtering it")
        dim_instrument_df = read_from_s3_catalog_convert_to_df(JobContext.get_property(C_GLUE_S3_DB_NAME_PROP_KEY), JobContext.get_property(C_GLUE_S3_TABLE_DIM_INSTRUMENT_PROP_KEY),
                                                               ["instrument_id", "currency_cd","active_flg","created_ts"])
        dim_instrument_df = transform_boolean_clm(dim_instrument_df,"active_flg")
        dim_ins_wind= Window.partitionBy("instrument_id").orderBy("created_ts")
        dim_instrument_df = dim_instrument_df.withColumn("row",sf.row_number().over(dim_ins_wind)) \
            .filter(sf.col("row")==1).filter("active_flg").select("instrument_id","currency_cd")
        JobContext.logger.info("Read Dim_instrument and filtered it")


    if clm_to_use_for_dt_filter is not None:
        start_date_cond = p_dtl_ins_df['start_dt'] <= p_src_df[clm_to_use_for_dt_filter]
        end_date_cond = p_dtl_ins_df['end_dt'] >= p_src_df[clm_to_use_for_dt_filter]
    else:
        start_date_cond = p_dtl_ins_df['start_dt'] <= p_batch_date
        end_date_cond = p_dtl_ins_df['end_dt'] >= p_batch_date

    if p_instrument_filter_cond is not None:
        p_dtl_ins_df = p_dtl_ins_df.filter(p_dtl_ins_df.instrument_ref_type_cd.isin(p_instrument_filter_cond))

    currecy_cd = derive_currency_cd()
    """
    get the currency_Cd from dim_instrument and set preference as True or false, according to default currency.
     Now we will sort based on it which will make matching currency_cd first
    """
    p_dtl_ins_df = p_dtl_ins_df.filter("active_flg").alias("dtl").join(sf.broadcast(dim_instrument_df).alias("dim"), sf.col("dim.instrument_id")==sf.col("dtl.instrument_id"),"left") \
        .select(p_dtl_ins_df['*'], sf.col("dim.currency_cd")) \
        .withColumn("currency_cd_preference", sf.when(sf.col("currency_cd") == currecy_cd,sf.lit(1) ).otherwise(sf.lit(0)))

    # previous we had two windowing functions with difference as start_dt at partition by, now we had only one,
    # added end_dt as ascending to eleminate duplicate records at order by and it is corrected due to the data issue
    # of fix script, code changed as of 2022-03-01.

    # 2022-10-31 misssing securities fix.
    dtl_windo= Window.partitionBy(p_dtl_ins_df['instrument_ref_nm'], p_dtl_ins_df['instrument_ref_type_cd'],p_dtl_ins_df['start_dt']) \
        .orderBy(p_dtl_ins_df['currency_cd_preference'].desc(),p_dtl_ins_df['start_dt'].desc(),p_dtl_ins_df['created_ts'].desc(),p_dtl_ins_df['end_dt'].asc())

    # As of now we are not touching below condition and will correct once issue resolved and not code breakage.
    if clm_to_use_for_dt_filter is not None:
        dtl_irf_df = p_dtl_ins_df.withColumn("row_num", sf.row_number().over(dtl_windo))
    else:
        dtl_irf_df = p_dtl_ins_df.withColumn("row_num", sf.row_number().over(dtl_windo))
    dtl_irf_df = dtl_irf_df.filter("row_num == 1").alias("dtl_irf_df").select('instrument_id', 'start_dt','end_dt' ,'instrument_ref_nm','instrument_ref_type_cd')

    # Joining it with source Data

    if broadcast:
        dtl_irf_df = sf.broadcast(dtl_irf_df)
        JobContext.logger.info("Broadcasting dtl_instrument_ref")
    joined_df = p_src_df.join(dtl_irf_df, ((p_joining_cond) & (start_date_cond) & (end_date_cond)), 'left_outer') \
        .select(p_src_df['*'],dtl_irf_df['instrument_id'].alias(derived_instrument_id_alias))
    JobContext.logger.info("Instrument id is derived")
    return joined_df


def derive_src_sys_id(p_client_nbr,p_desc_required=False):
    JobContext.logger.info("Fetching  source system ID")
    # TODO: This should be globle, but when put it in gloable its failling, need to investigate further
    C_DIM_SRC_SYS_TBL = JobContext.get_property(C_GLUE_S3_TABLE_DIM_SOURCE_SYSTEM_PROP_KEY)
    C_DB_NAME = JobContext.get_property(C_GLUE_S3_DB_NAME_PROP_KEY)
    l_dim_src_sys_dy_frame = read_from_s3_catalog_convert_to_df(C_DB_NAME, C_DIM_SRC_SYS_TBL)
    l_dim_src_sys_dy_frame = l_dim_src_sys_dy_frame.filter(l_dim_src_sys_dy_frame.file_nm == p_client_nbr)
    src_sys_ids = l_dim_src_sys_dy_frame.limit(1).select("source_system_id","source_system_desc").collect()
    if (len(src_sys_ids) == 0):
        raise Exception("Corresponding Source System not found for client nbr number {}".format(p_client_nbr))
    JobContext.logger.info("Source system ID Fetched")
    if p_desc_required:
        return src_sys_ids[0].source_system_id,src_sys_ids[0].source_system_desc
    else:
        return src_sys_ids[0].source_system_id

###############################
# To retrieve Account ID from Dim_account tbl, this method take broadcast as True or false,
# if True is passed, hint broadcast would be added for Dim_account table
# p_joining_condition should have table alias as follows:
# 1. Source(p_vestmark_src_df) - src
# 2. Dim_Account - acc
# Ex. - joining_cond = [sf.col('src.accountname') == sf.col('acc.sor_account_id')]
###############################
def derive_account_id(p_vestmark_src_df, p_joining_cond, broadcast=False, new_account_linking=False):
    l_batch_date_clm = sf.to_date(sf.lit(JobContext.get_property(C_BATCH_DATE_PROP_KEY)))
    JobContext.logger.info(" Deriving account with joining condition {}, broadcast {}".format(p_joining_cond, broadcast))
    g_dtl_account_required_fields = ["account_id", "account_ref_nm","account_ref_type_cd", "active_flg", "start_dt", "created_ts"]
    JobContext.logger.info("Deriving account_id using joining condition {}".format(p_joining_cond))
    l_glue_db_name = JobContext.get_property(C_GLUE_S3_DB_NAME_PROP_KEY)
    l_dtl_account_tbl_nme = JobContext.get_property(C_GLUE_S3_TABLE_DTL_ACCOUNT_REF_PROP_KEY)

    l_dtl_account_df = read_from_s3_catalog_convert_to_df(l_glue_db_name, l_dtl_account_tbl_nme,
                                                          g_dtl_account_required_fields, None)
    l_dtl_account_df = transform_boolean_clm(l_dtl_account_df, "active_flg")
    l_dtl_acc_window_spec = Window.partitionBy(l_dtl_account_df['account_ref_nm']).orderBy(
        l_dtl_account_df['created_ts'].desc())
    l_dtl_account_df = l_dtl_account_df.withColumn("row_num", sf.row_number().over(l_dtl_acc_window_spec)) \
        .where((sf.col("row_num") == 1) & (sf.col("active_flg") == True))
    l_dtl_account_df = l_dtl_account_df.withColumn("sor_account_id",sf.col("account_ref_nm"))
    if broadcast:
        JobContext.logger.info("Broadcast the account table")
        l_dtl_account_df = l_dtl_account_df.hint("broadcast")
    p_vestmark_src_df = p_vestmark_src_df.alias('src').join(l_dtl_account_df.alias('acc'), p_joining_cond, "left") \
        .select(sf.col("acc.account_id"), p_vestmark_src_df['*'])

    # lookup of dim_account_link and join account_id with old_account_id, picking new_account_id
    if new_account_linking:
        lkp_dim_account_link_df = read_from_s3_catalog_convert_to_df(JobContext.get_property(C_GLUE_S3_DB_NAME_PROP_KEY),
                                                                     JobContext.get_property(C_GLUE_S3_TABLE_DIM_ACCOUNT_LINK_PROP_KEY)) \
            .filter(sf.col('link_type_cd') == C_LINK_TYPE_CD_BRNCH)
        lkp_dim_account_link_df = lkp_dim_account_link_df.select('new_account_id', 'old_account_id', 'start_dt').distinct()

        g_windowSpec = Window.partitionBy(lkp_dim_account_link_df['old_account_id']) \
            .orderBy(lkp_dim_account_link_df['start_dt'].desc())
        lkp_dim_account_link_df = lkp_dim_account_link_df.withColumn("row_num", sf.row_number().over(g_windowSpec)).filter("row_num==1")
        lkp_dim_account_link_df = lkp_dim_account_link_df.filter(sf.col('end_dt') >= l_batch_date_clm)

        # Creating an account_id by concatinating from source when account_id is null after joining with dtl_account_ref table due to record inactive or active_flg as false
        p_vestmark_src_df = p_vestmark_src_df.withColumn('sor_account_id_trim', sf.substring('sor_account_id', 2, 8))
        p_vestmark_src_df = p_vestmark_src_df.withColumn('account_id_concat', sf.concat_ws("-", "sor_account_id_trim", sf.lit(C_COMMON_SOURCE_SYSTEM_ID), sf.lit(C_FIRM_ID)))
        p_vestmark_src_df = p_vestmark_src_df.withColumn('account_id', sf.when(sf.col('account_id').isNull(), sf.col('account_id_concat')).otherwise(sf.col("account_id")))

        joining_condition = [sf.col('account_id') == sf.col('old_account_id')]
        p_vestmark_src_df = p_vestmark_src_df.join(lkp_dim_account_link_df, joining_condition, 'left') \
            .select(p_vestmark_src_df['*'], lkp_dim_account_link_df.new_account_id)
        p_vestmark_src_df = p_vestmark_src_df.withColumn('account_id', sf.when(sf.col('new_account_id').isNull(),
                                                                               sf.col('account_id')).otherwise(sf.col("new_account_id")))\
            .drop('account_id_concat','sor_account_id_trim')

        # From source account_id or updated account_id from dim_account_link, checking account_id is present at
        # dim_account otherwise derived account_id will be null. Null accounts will move to dim_account_excp table.
        g_dim_account_required_fields = ["account_id", "account_nm", "active_flg", "created_ts"]
        l_glue_db_name = JobContext.get_property(C_GLUE_S3_DB_NAME_PROP_KEY)
        l_dim_account_tbl_nme = JobContext.get_property(C_GLUE_S3_TABLE_DIM_ACCOUNT_PROP_KEY)

        l_dim_account_df = read_from_s3_catalog_convert_to_df(l_glue_db_name, l_dim_account_tbl_nme,
                                                              g_dim_account_required_fields, None)
        l_dim_account_df = transform_boolean_clm(l_dim_account_df, "active_flg")
        l_dim_acc_window_spec = Window.partitionBy(l_dim_account_df['account_nm']).orderBy(l_dim_account_df['created_ts'].desc())
        l_dim_account_df = l_dim_account_df.withColumn("row_num", sf.row_number().over(l_dim_acc_window_spec)) \
            .where((sf.col("row_num") == 1) & (sf.col("active_flg") == True))
        l_dim_account_df = l_dim_account_df.withColumnRenamed("account_id", "account_id_lkp")
        joining_cond = [sf.col('account_id') == sf.col('account_id_lkp')]
        p_vestmark_src_df = p_vestmark_src_df.alias('src').join(l_dim_account_df.alias('acc'), joining_cond, "left") \
            .select(sf.col("acc.account_id_lkp"), p_vestmark_src_df['*'])
        p_vestmark_src_df = p_vestmark_src_df.withColumnRenamed("account_id", "account_id_src")\
            .withColumnRenamed("account_id_lkp", "account_id")

        # loading account_id with null or empty records into dim_account_excp table
        excp_account_id_null_df = p_vestmark_src_df.filter(isNull_check("account_id"))
        excp_account_id_null_df = excp_account_id_null_df.withColumn("excp_sor_acc_id", sf.split('account_id_src', '-')[0])
        excp_account_id_null_df = excp_account_id_null_df.select('account_id',sf.col("excp_sor_acc_id").alias('sor_account_id'),
                                                                 sf.col('excp_sor_acc_id').alias('account_nm'),'account_id_src')
        excp_account_id_null_df = add_default_clm(excp_account_id_null_df, C_DIM_ACC_DEFAULT_MAPPING)
        excp_account_id_null_df = add_audit_columns(excp_account_id_null_df, l_batch_date_clm, add_modification_clm=True).distinct()
        C_DIM_ACC_FIELDS_TO_VAL={"account_id": [NotNullValidator('account_id_src', 'derived account is not present at dim_account table:')]}
        excp_account_id_null_df = validate_rec_and_filter(excp_account_id_null_df, C_DIM_ACC_FIELDS_TO_VAL,
                                                          JobContext.get_property(C_S3_DIM_ACCOUNT_EXCP_OUTPUT_LOCATION_PROP_KEY),
                                                          prepare_exception_clm_list(C_DIM_ACC_CLMS))

    # Exclude account logic
    p_vestmark_src_df = p_vestmark_src_df.filter(~isNull_check("account_id"))
    p_vestmark_src_df = p_vestmark_src_df.withColumn("exclude_sor_acc_id",sf.split('account_id','-')[0])
    p_vestmark_src_df=lookup_mtdt_exclude_account(p_vestmark_src_df,"exclude_sor_acc_id")

    if "source_system_id" in p_vestmark_src_df.columns:
        keybank_src_sys_id = derive_src_sys_id(C_KEY_TRANS_CLIENT_NUMBER)
        p_vestmark_src_df = p_vestmark_src_df.withColumn("start_sor_account_id",sf.when(sf.col("source_system_id")== keybank_src_sys_id,sf.lit('KEYBANK')).otherwise(sf.col("start_sor_account_id")))
    else:
        p_vestmark_src_df = p_vestmark_src_df

    #Data Validation
    excp_mtdt_df = p_vestmark_src_df.filter((sf.col("start_sor_account_id").isNotNull()) & (sf.col("start_sor_account_id")!='KEYBANK'))
    excp_mtdt_df = excp_mtdt_df.select('account_id',sf.col("exclude_sor_acc_id").alias('sor_account_id'),'start_sor_account_id',sf.col('exclude_sor_acc_id').alias('account_nm'))
    excp_mtdt_df = add_default_clm(excp_mtdt_df,C_DIM_ACC_DEFAULT_MAPPING)
    excp_mtdt_df = add_audit_columns(excp_mtdt_df, l_batch_date_clm, add_modification_clm=True)
    C_DIM_ACC_FIELDS_TO_VALIDATE={"start_sor_account_id": [NotEqualValidator([""], "Provided sor_account_id is present in mtdt_exclude table:")]}
    excp_mtdt_df = validate_rec_and_filter(excp_mtdt_df, C_DIM_ACC_FIELDS_TO_VALIDATE,
                                                  JobContext.get_property(C_S3_DIM_ACCOUNT_EXCP_OUTPUT_LOCATION_PROP_KEY),
                                                  prepare_exception_clm_list(C_DIM_ACC_CLMS))
    p_vestmark_src_df = p_vestmark_src_df.filter((sf.col("start_sor_account_id").isNull()) | (sf.col("start_sor_account_id")=='KEYBANK'))
    p_vestmark_src_df = p_vestmark_src_df.drop("start_sor_account_id")
    JobContext.logger.info("account_id is derived using joining condition {}".format(p_joining_cond))
    return p_vestmark_src_df


###############################
# To retrieve Account ID from dtl_account_ref tbl:
# Parameters: broadcast as True or False. If True is passed (i.e. less data), hint broadcast would be added for dtl_account_ref table.
# p_joining_cond should have joins between src_df and dtl_account_ref_df
# p_filter_cond should have list of account_ref_type_cd
###############################

def derive_dtl_account_id(p_src_df, p_dtl_account_ref_df, p_joining_cond, p_filter_cond, p_broadcast=False, p_derived_account_id_alias='account_id', new_account_linking=False):
    l_batch_date_clm = sf.to_date(sf.lit(JobContext.get_property(C_BATCH_DATE_PROP_KEY)))
    JobContext.logger.info("Deriving account_id with joining condition: {}, filter condition: {}, broadcast: {}".format(p_joining_cond, p_filter_cond, p_broadcast))
    l_dtl_account_ref_df = p_dtl_account_ref_df.filter(p_dtl_account_ref_df.account_ref_type_cd.isin(p_filter_cond))\
        .select("account_id", "account_ref_type_cd", "account_ref_nm", "active_flg", "created_ts")

    l_dtl_acct_ref_window_spec = Window.partitionBy(l_dtl_account_ref_df.account_ref_type_cd, l_dtl_account_ref_df.account_ref_nm)\
        .orderBy(l_dtl_account_ref_df['created_ts'].desc())

    l_dtl_account_ref_df = l_dtl_account_ref_df.withColumn("row_num", sf.row_number().over(l_dtl_acct_ref_window_spec)) \
        .where((sf.col("row_num") == 1) & (sf.col("active_flg") == True))

    if p_broadcast:
        JobContext.logger.info("Broadcast the account table")
        l_dtl_account_ref_df = l_dtl_account_ref_df.hint("broadcast")

    p_src_df = p_src_df.alias('src').join(l_dtl_account_ref_df.alias('acc'), p_joining_cond, "left_outer") \
        .select(p_src_df['*'], sf.col("acc.account_id").alias(p_derived_account_id_alias))

    # lookup of dim_account_link and join account_id with old_account_id, picking new_account_id
    if new_account_linking:
        lkp_dim_account_link_df = read_from_s3_catalog_convert_to_df(JobContext.get_property(C_GLUE_S3_DB_NAME_PROP_KEY),
                                                                     JobContext.get_property(C_GLUE_S3_TABLE_DIM_ACCOUNT_LINK_PROP_KEY)) \
            .filter(sf.col('link_type_cd') == C_LINK_TYPE_CD_BRNCH)
        lkp_dim_account_link_df = lkp_dim_account_link_df.select('new_account_id', 'old_account_id', 'start_dt').distinct()

        g_windowSpec = Window.partitionBy(lkp_dim_account_link_df['old_account_id']) \
            .orderBy(lkp_dim_account_link_df['start_dt'].desc())
        lkp_dim_account_link_df = lkp_dim_account_link_df.withColumn("row_num", sf.row_number().over(g_windowSpec)).filter("row_num==1")

        joining_condition = [sf.col('account_id') == sf.col('old_account_id')]
        p_src_df = p_src_df.join(lkp_dim_account_link_df, joining_condition, 'left') \
            .select(p_src_df['*'], lkp_dim_account_link_df.new_account_id)
        p_src_df = p_src_df.withColumn('account_id', sf.when(sf.col('new_account_id').isNull(),
                                                             sf.col('account_id')).otherwise(sf.col("new_account_id")))

    # Exclude account logic
    p_vestmark_src_df = p_src_df.withColumn("exclude_sor_acc_id",sf.split(p_derived_account_id_alias,'-')[0])
    p_vestmark_src_df=lookup_mtdt_exclude_account(p_vestmark_src_df,"exclude_sor_acc_id")
    excp_mtdt_df = p_vestmark_src_df.filter(sf.col("start_sor_account_id").isNotNull())
    excp_mtdt_df = excp_mtdt_df.select(sf.col(p_derived_account_id_alias).alias('account_id'),sf.col('exclude_sor_acc_id').alias('sor_account_id'),'start_sor_account_id',sf.col('exclude_sor_acc_id').alias('account_nm'))
    C_DIM_ACC_CLMS=["account_id","sor_account_id","source_system_id","account_nm","currency_cd","tax_status_cd","load_cache_flg","fiscal_year_end","firm_id",
                    "active_flg","batch_dt","created_program_nm","created_user_nm","created_ts","modified_program_nm","modified_user_nm","modified_ts"]
    C_DIM_ACC_DEFAULT_MAPPING={"source_system_id":"","currency_cd":"","tax_status_cd":"","load_cache_flg":"","fiscal_year_end":"","start_dt":"1753-01-01"
                                ,"end_dt":"9999-12-31","firm_id":"1"}
    excp_mtdt_df = add_default_clm(excp_mtdt_df,C_DIM_ACC_DEFAULT_MAPPING)
    excp_mtdt_df = add_audit_columns(excp_mtdt_df, l_batch_date_clm, add_modification_clm=True)
    #Data Validation
    C_DIM_ACC_FIELDS_TO_VALIDATE={"start_sor_account_id": [NotEqualValidator([""], "Provided sor_account_id is present in mtdt_exclude table:")]}
    excp_mtdt_df = validate_rec_and_filter(excp_mtdt_df, C_DIM_ACC_FIELDS_TO_VALIDATE,
                                                  JobContext.get_property(C_S3_DIM_ACCOUNT_EXCP_OUTPUT_LOCATION_PROP_KEY),
                                                  prepare_exception_clm_list(C_DIM_ACC_CLMS))
    p_vestmark_src_df = p_vestmark_src_df.filter(sf.col("start_sor_account_id").isNull())
    p_vestmark_src_df = p_vestmark_src_df.drop("start_sor_account_id","exclude_sor_acc_id")
    JobContext.logger.info("derive_dtl_account_id(): Account Id is derived.")
    p_src_df = p_vestmark_src_df
    return p_src_df


###############################
# Lookup dim_source_system
# p_joining_condition should have table alias as follows:
# 1. Source(hpa_df) - src_df
# 2. Dim_Source_system - dim_src_sys_df
# Ex. - joining_cond = (src_df[src_record_type_cd_clm_new_nm] == dim_src_sys_df.file_nm)
###############################

def lookup_dim_source_system_file_nm(src_df, record_type_cd_cl_nm , src_record_type_cd_clm_new_nm,client_nbr_cl_nm=None, source_sysm_id_alias=None):
    JobContext.logger.info("src_record_type_cd_clm_new_nm - {}".format(src_record_type_cd_clm_new_nm))

    dim_src_sys_df = read_from_s3_catalog_convert_to_df(JobContext.get_property(C_GLUE_S3_DB_NAME_PROP_KEY),
                                                        JobContext.get_property( C_GLUE_S3_TABLE_DIM_SOURCE_SYSTEM_PROP_KEY),
                                                        ["file_nm","source_system_nm","source_system_id",
                                                         "source_system_desc","active_flg"])
    dim_src_sys_df = transform_boolean_clm(dim_src_sys_df, "active_flg").filter("active_flg")
    dim_src_sys_df = dim_src_sys_df.filter(sf.col("active_flg"))
    dim_src_sys_df = sf.broadcast(dim_src_sys_df)
    if source_sysm_id_alias is None:
        source_sysm_id_alias ="source_system_id"
    if client_nbr_cl_nm is not None:
        src_df = src_df.join(dim_src_sys_df, (src_df[record_type_cd_cl_nm] == dim_src_sys_df.file_nm) & (
                src_df[client_nbr_cl_nm] == dim_src_sys_df.source_system_nm),
                             "left_outer").select(src_df['*'], dim_src_sys_df.file_nm.alias(src_record_type_cd_clm_new_nm),
                                                  dim_src_sys_df.source_system_id.alias(source_sysm_id_alias),dim_src_sys_df.source_system_desc)
    else:
        src_df = src_df.join(dim_src_sys_df, (src_df[record_type_cd_cl_nm] == dim_src_sys_df.file_nm) ,
                             "left_outer").select(src_df['*'], dim_src_sys_df.file_nm.alias(src_record_type_cd_clm_new_nm),
                                                  dim_src_sys_df.source_system_id.alias(source_sysm_id_alias),dim_src_sys_df.source_system_desc)

    return src_df
###############################
# Lookup dim_currency
# p_joining_condition should have table alias as follows:
# 1. Source - src_df
# 2. dim_currency - dim_currency_df
# Ex. - joining_cond = [dim_currency_df.sor_currency_cd==src_df[source_currency_cd_new_clm_nm]]
###############################

def lookup_dim_currency_sor_currency_cd(src_df,source_currency_cd, source_currency_cd_new_clm_nm, dim_currency_cd_clm_nm="dim_currency_cd_value"):

    dim_currency_df = read_from_s3_catalog_convert_to_df(JobContext.get_property(C_GLUE_S3_DB_NAME_PROP_KEY),
                                                         JobContext.get_property( C_GLUE_S3_TABLE_DIM_CURRENCY_PROP_KEY))
    dim_currency_df = transform_boolean_clm(dim_currency_df, "active_flg").filter("active_flg")

    src_df = src_df.join(sf.broadcast(dim_currency_df), dim_currency_df.sor_currency_cd==src_df[source_currency_cd], "left_outer") \
        .select(src_df['*'], dim_currency_df.sor_currency_cd.alias(source_currency_cd_new_clm_nm),
                dim_currency_df.currency_cd.alias(dim_currency_cd_clm_nm))
    return src_df

###############################
# Lookup dim_currency
# p_joining_condition should have table alias as follows:
# 1. Source - src_df
# 2. dim_currency - dim_currency_df
# Ex. - joining_cond = [dim_currency_df.currency_cd==src_df[source_currency_cd_new_clm_nm]]
###############################

def lookup_dim_currency_currency_cd(src_df, dim_currency_df, source_currency_cd, dim_currency_cd_clm_nm):
    JobContext.logger.info("Performing Lookup on dim_currency, look column is {}".format(dim_currency_cd_clm_nm))

    dim_currency_df = dim_currency_df.select('currency_cd').distinct()
    src_df = src_df.join(sf.broadcast(dim_currency_df), dim_currency_df.currency_cd==src_df[source_currency_cd], "left_outer") \
        .select(src_df['*'], dim_currency_df.currency_cd.alias(dim_currency_cd_clm_nm))
    JobContext.logger.info("Performed Lookup on dim_currency, look column is {}".format(dim_currency_cd_clm_nm))
    return src_df

###############################
# Lookup dim_quantity_factor
# p_joining_condition should have table alias as follows:
# 1. Source - src_df
# 2. dim_quantity - dim_quantity_df
# Ex. - joining_cond = [dim_quantity_factor_df.multiply_price_cd==src_df[source_multiply_price_cd_new_clm_nm]]
###############################

def lookup_dim_quantity_factor_multiply_price_cd(src_df,source_multiply_price_cd,source_multiply_price_cd_new_clm_nm):

    dim_quantity_factor_df =read_from_s3_catalog_convert_to_df(JobContext.get_property(C_GLUE_S3_DB_NAME_PROP_KEY),
                                                               JobContext.get_property((C_GLUE_S3_TABLE_DIM_QUANTITY_FACTOR_PROP_KEY)))

    g_windowSpec= Window.partitionBy(dim_quantity_factor_df['multiply_price_cd']).orderBy(dim_quantity_factor_df['created_ts'].desc())
    dim_quantity_factor_df = dim_quantity_factor_df.withColumn('num',sf.row_number().over(g_windowSpec))
    dim_quantity_factor_df =dim_quantity_factor_df.filter(sf.col('num')==1)
    dim_quantity_factor_df = dim_quantity_factor_df.drop("num")

    src_df = src_df.join(dim_quantity_factor_df,dim_quantity_factor_df["multiply_price_cd"]==src_df[source_multiply_price_cd],
                         "left_outer").select(src_df['*'],dim_quantity_factor_df["multiply_price_cd"].alias(source_multiply_price_cd_new_clm_nm))
    return src_df

def get_property_from_dim_tbl(prop_name, excp_reqd=True):
    JobContext.logger.info("Fetching proptery details..")
    fact_setting_df = read_from_catalog_and_filter_inactive_rec(JobContext.get_property(C_GLUE_S3_TABLE_FACT_SETTING_PROP_KEY))
    dim_prop_val_df = read_from_catalog_and_filter_inactive_rec(JobContext.get_property(C_GLUE_S3_TABLE_DIM_PROPERTY_VALUE_PROP_KEY))
    dim_prop_df = read_from_catalog_and_filter_inactive_rec(JobContext.get_property(C_GLUE_S3_TABLE_DIM_PROPERTY_POP_KEY))
    prop_val_df = fact_setting_df.alias('fs').join(dim_prop_val_df.alias('dpv'), 'property_value_id').join(dim_prop_df.alias('dp'), 'property_id')
    curr_cd_row = prop_val_df.filter(prop_val_df.property_desc ==prop_name ).select(sf.col('dpv.property_value_desc').alias('prop')).first()

    if excp_reqd==True:
        if curr_cd_row is None:
            raise Exception("Corresponding property {} not found".format(prop_name))
    elif curr_cd_row==None:
        JobContext.logger.info("No exception raised, Corresponding property {} not found".format(prop_name))
        return None

    prop_val = curr_cd_row.asDict()['prop']
    JobContext.logger.info("For property name {} found property {}".format(prop_name, prop_val))
    return prop_val

###############################
# Lookup fact_class_entity & dim_class_type
# p_joining_condition should have table alias as follows:
# 1. fact_class_entity_df - fac
# 2. dim_class_type - dim
# Ex. - joining_cond = (class_type_id)
###############################

def derive_entity_id(src_df, child_class_type_cd = ['TIPS_INDICATOR'], entity_type_cd = 'SECURITY'):
    fact_class_entity_df = read_from_catalog_and_filter_inactive_rec(JobContext.get_property(C_S3_FACT_CLASS_ENTITY_TBL_NME))
    fact_class_entity_df = fact_class_entity_df.filter(fact_class_entity_df.entity_type_cd==entity_type_cd)
    fact_class_entity_df = transform_boolean_clm(fact_class_entity_df, "active_flg")
    fact_class_entity_df = fact_class_entity_df.withColumn("row_num",sf.row_number().over(Window.partitionBy(fact_class_entity_df.entity_id,fact_class_entity_df.class_type_id) \
                                                                                    .orderBy(fact_class_entity_df.created_ts.desc(),fact_class_entity_df.batch_dt.desc()))) \
                                                                                    .filter("row_num==1").filter("active_flg")

    dim_class_type_df = read_from_catalog_and_filter_inactive_rec(JobContext.get_property(C_GLUE_S3_TABLE_DIM_CLASS_TYPE))
    dim_class_type_df = dim_class_type_df.filter(dim_class_type_df.child_class_type_cd.isin(child_class_type_cd))

    #check instrument_id in fact_class_entity
    check_instr_in_df = src_df.join(fact_class_entity_df,src_df.instrument_id == fact_class_entity_df.entity_id, how='inner' )\
                                .select(src_df['*'],fact_class_entity_df.entity_id,fact_class_entity_df.class_type_id)
                                
    entity_id_df = check_instr_in_df.alias('fac').join(dim_class_type_df.alias("dim"),"class_type_id", how='inner').select(check_instr_in_df['*'])	
    entity_id_df = entity_id_df.drop('class_type_id')

    if entity_id_df == None:
        return None
    else:
        return entity_id_df


C_DEFAULT_DIM_CLS_TYP_CLMS_FOR_CLASS_TYPE_ID_DER = ['class_type_id', 'child_class_type_cd', 'child_class_val']

C_DEFAULT_DIM_CLASS_TYPE_ALIAS_NAME="dim_class"

#############################
# This will derive class type id using source and provided joining condition
# Note: This will upper case the column child_class_val
# entity_type_cd filter for dim_class-type is set to ['CAPS', 'SECURITY'] as defaults
##########################
def derive_class_type_id_from_dim_cls_typ_and_src(src_df, join_condition,
                                                  clms_to_select_in_dim=C_DEFAULT_DIM_CLS_TYP_CLMS_FOR_CLASS_TYPE_ID_DER,
                                                  dim_class_type_alias_nm=C_DEFAULT_DIM_CLASS_TYPE_ALIAS_NAME,
                                                  p_dim_class_entity_type_cd_filter = C_CAPS_ENTITY_TYPE_CD_LIST):

    JobContext.logger.info("Deriving class type id using dim_class_type and source data")
    dim_class_type_df = read_from_catalog_and_filter_inactive_rec(JobContext.get_property(C_GLUE_S3_TABLE_DIM_CLASS_TYPE))
    dim_class_type_df = dim_class_type_df.filter(sf.col('entity_type_cd').isin(p_dim_class_entity_type_cd_filter)).select(*clms_to_select_in_dim)

    dim_class_type_df = dim_class_type_df.withColumn('child_class_val', sf.upper(sf.col('child_class_val')))
    dim_class_type_df = dim_class_type_df.alias(dim_class_type_alias_nm)

    src_joined_df = src_df.join(dim_class_type_df, join_condition, 'leftouter').select(src_df['*'], dim_class_type_df.class_type_id)

    JobContext.logger.info("Derived class type id using dim_class_type and source data")
    return src_joined_df



C_DEFAULT_DIM_ACCOUNT_GRP_ALIAS_NAME="dim_grp_acc"
C_DEFAULT_DIM_ACC_GRP_CLMS_FOR_ENTITY_DER = ['sor_account_group_id', 'account_group_type_cd', 'account_group_id']

def derive_entity_id_from_acc_grp_id_and_src(src_df, join_condition, derived_clm_alias_name='entity_id',
                                             dim_acc_grp_alias_nm=C_DEFAULT_DIM_ACCOUNT_GRP_ALIAS_NAME,
                                             clms_to_select_in_dim=C_DEFAULT_DIM_ACC_GRP_CLMS_FOR_ENTITY_DER):

    JobContext.logger.info("Deriving Entity id using dim_class_type and source data")
    g_dim_acct_grp_df = read_from_catalog_and_filter_inactive_rec(JobContext.get_property(C_GLUE_S3_TABLE_DIM_ACCOUNT_GROUP_PROP_KEY)).select(*clms_to_select_in_dim)
    g_dim_acct_grp_df = g_dim_acct_grp_df.alias(dim_acc_grp_alias_nm)
    src_df = src_df.join(g_dim_acct_grp_df, join_condition, "leftouter").select(src_df['*'], g_dim_acct_grp_df['account_group_id'].alias(derived_clm_alias_name))

    return src_df

def derive_sleeve_id(p_src_df, p_sleeve_df, joining_condition):
    JobContext.logger.info("Deriving sleeve Id without putting filtering logic on dim_sleeve table with joining condition")
    output_df = p_src_df.join(p_sleeve_df, joining_condition, "left_outer").select(p_src_df['*'], p_sleeve_df['sleeve_id'])
    JobContext.logger.info("Derived sleeve Id without putting filtering logic on dim_sleeve table with joining condition")
    return output_df

def derive_account_type_id_filter_logic(p_src_df, p_account_type_df, joining_condition, dim_account_type_id_clm_name_alias="account_type_id"):
    JobContext.logger.info("Deriving account_type_id without putting filtering logic on dim_account_type table with joining condition")
    output_df = p_src_df.join(sf.broadcast(p_account_type_df), joining_condition, "left_outer").select(p_src_df['*'], p_account_type_df['account_type_id'].alias(dim_account_type_id_clm_name_alias))
    JobContext.logger.info("Derived account_type_id without putting filtering logic on dim_account table with joining condition")
    return output_df

def derive_sleeve_id_without_filter_logic(p_src_df, p_sleeve_df, joining_condition):
    JobContext.logger.info("Deriving sleeve Id without putting filtering logic on dim_sleeve table with joining condition")
    p_sleeve_df = p_sleeve_df.withColumn("row_num",  sf.row_number().over(Window.partitionBy(p_sleeve_df['sor_sleeve_id']).orderBy(p_sleeve_df["created_ts"].desc()))) \
        .filter("row_num==1")
    p_sleeve_df = p_sleeve_df.select(p_sleeve_df['sor_sleeve_id'], p_sleeve_df['sleeve_id'])
    output_df = p_src_df.join(sf.broadcast(p_sleeve_df), joining_condition, "left_outer").select(p_src_df['*'], p_sleeve_df['sleeve_id'])
    JobContext.logger.info("Derived sleeve Id without putting filtering logic on dim_sleeve table with joining condition")
    return output_df

###############################
# Perform lookup on mtdt_exclude_account table
###############################
def lookup_mtdt_exclude_account(src_df,p_src_acc_clm_nm="sor_account_id"):
    JobContext.logger.info("Starting MTDT exclude logic")
    g_mtdt_ex_accnt = read_from_s3_catalog_convert_to_df(JobContext.get_property(C_GLUE_S3_DB_NAME_PROP_KEY),
                                                         JobContext.get_property(C_GLUE_S3_TABLE_MTDT_EXCLUDE_ACCOUNT_PROP_KEY),
                                                         ["start_sor_account_id","end_sor_account_id"])

    g_mtdt_ex_accnt_range = g_mtdt_ex_accnt.where(sf.col("start_sor_account_id") != sf.col("end_sor_account_id"))

    g_mtdt_ex_accnt_brch_cd_df = g_mtdt_ex_accnt_range.withColumn('start_sor_account_id_brch_cd', sf.substring(g_mtdt_ex_accnt_range.start_sor_account_id, 1, 2)) \
        .withColumn('end_sor_account_id_brch_cd', sf.substring(g_mtdt_ex_accnt_range.end_sor_account_id, 1, 2))

    g_mtdt_ex_accnt_brch_cd_match_df = g_mtdt_ex_accnt_brch_cd_df.where(sf.col("start_sor_account_id_brch_cd") == sf.col("end_sor_account_id_brch_cd"))
    g_mtdt_ex_accnt_brch_cd_not_match_df = g_mtdt_ex_accnt_brch_cd_df.where(sf.col("start_sor_account_id_brch_cd") != sf.col("end_sor_account_id_brch_cd"))

    g_mtdt_ex_accnt = g_mtdt_ex_accnt.where(sf.col("start_sor_account_id") == sf.col("end_sor_account_id"))

    # if (g_mtdt_ex_accnt is None) | (g_mtdt_ex_accnt.first() is None):
    #     JobContext.logger.info("MTDT exclude account is empty.")
    #     src_df = src_df.withColumn("start_sor_account_id",sf.lit("-1"))
    #     return src_df

    src_df = src_df.join(sf.broadcast(g_mtdt_ex_accnt),src_df[p_src_acc_clm_nm] == g_mtdt_ex_accnt["start_sor_account_id"],
                         "left_outer").select(src_df["*"], g_mtdt_ex_accnt["start_sor_account_id"].alias("st_acc"))
    src_df = src_df.withColumn('p_src_acc_clm_nm_trim', sf.substring(src_df[p_src_acc_clm_nm], 1, 2))

    p_src_acc_clm_nm = "src."+p_src_acc_clm_nm
    src_df = src_df.alias("src").join(sf.broadcast(g_mtdt_ex_accnt_brch_cd_match_df).alias("lkp"),
                                      (sf.col("src.st_acc").isNull()) &
                                      (sf.col("p_src_acc_clm_nm_trim") == sf.col("start_sor_account_id_brch_cd"))
                                      & (sf.length(p_src_acc_clm_nm) <= 8), "left_outer") \
        .select(src_df["*"], sf.col("lkp.start_sor_account_id").alias("st_acc_1"))

    src_df = src_df.alias("src").join(sf.broadcast(g_mtdt_ex_accnt_brch_cd_not_match_df).alias("lkp"),
                                      (sf.col("src.st_acc").isNull()) & (sf.col("src.st_acc_1").isNull()) &
                                      (sf.col(p_src_acc_clm_nm).between(sf.col("lkp.start_sor_account_id"),
                                                                        sf.col("lkp.end_sor_account_id")))
                                      & (sf.length(p_src_acc_clm_nm) <= 8), "left_outer") \
        .select(src_df["*"], sf.coalesce(sf.col("src.st_acc"), sf.col("src.st_acc_1"), sf.col("lkp.start_sor_account_id")).alias("start_sor_account_id"))
    JobContext.logger.info("MTDT exclude logic completed")

    return src_df.drop("st_acc", "st_acc_1")

###############################
# Join fact_account_Date and dim_account on account_id and then get sor_account_id from dim_account
# join matched records with source records
###############################
#todo - active - inactive
def lookup_uma_account_exclude(src_df):
    JobContext.logger.info("Starting UMA exclude logic")
    l_fact_account_date_df = read_from_catalog_and_filter_inactive_rec(JobContext.get_property(C_GLUE_S3_TABLE_FACT_ACC_DATE_PROP_KEY),
                                                                       ["account_id","date_type_cd","date_type_dt",
                                                                        "program_ind","entity_type_cd","entity_id",
                                                                        "active_flg"])
    l_dm_account = read_from_catalog_and_filter_inactive_rec( JobContext.get_property(C_GLUE_S3_TABLE_DIM_ACCOUNT_PROP_KEY),
                                                              ["sor_account_id","account_id","active_flg"])
    l_fact_account_date_df = l_fact_account_date_df.filter((l_fact_account_date_df["date_type_cd"] == "ADPDT") &
                                                           (sf.col("program_ind") == "UMA"))
    l_fact_account_date_df = format_date(l_fact_account_date_df,'date_type_dt')
    l_fad = l_fact_account_date_df.withColumn("row_num", sf.row_number()
                                              .over(Window.partitionBy(l_fact_account_date_df['account_id'],
                                                                       l_fact_account_date_df['entity_type_cd'],
                                                                       l_fact_account_date_df['entity_id'])
                                                    .orderBy(l_fact_account_date_df['date_type_dt'].desc())))
    l_fad = l_fad.filter(l_fad["row_num"] == 1).select("account_id")
    l_sor_acc_id_value = l_dm_account.join(sf.broadcast(l_fad), l_fad["account_id"] == l_dm_account["account_id"]) \
        .select(l_dm_account["account_id"], l_dm_account["sor_account_id"])
    src_df = src_df.join(sf.broadcast(l_sor_acc_id_value), src_df["sor_account_id"] == l_sor_acc_id_value["sor_account_id"],
                         "left_outer").select(src_df["*"],
                                              l_sor_acc_id_value["sor_account_id"].alias("fad_sor_account_id"))
    JobContext.logger.info("UMA exclude logic completed")
    return src_df

#################################
#  b2b_b2f_data_method
#################################
def get_b2b_b2f_data(src_df):
    JobContext.logger.info("Fetching b2b/b2f data")
    C_COMMON_COL_LIST = [StructField('client_nbr', StringType(), True),StructField('group_slct_id', StringType(), True),
                         StructField('processing_dt', StringType(), True),StructField('branch_cd', StringType(), True),
                         StructField('account_cd', StringType(), True),StructField('currency_cd', StringType(), True),
                         StructField('type_account_cd', StringType(), True),StructField('chck_brch_acct_nbr', StringType(), True),
                         StructField('security_adp_nbr', StringType(), True),StructField('trans_acct_hist_cd', StringType(), True),
                         StructField('transaction_dt', StringType(), True),StructField('debit_credit_cd', StringType(), True)]

    C_B2B_COL_LIST = C_COMMON_COL_LIST + [StructField('trans_control_id', StringType(), True)]
    C_B2F_COL_LIST = C_COMMON_COL_LIST + [StructField('frst_mny_amt', StringType(), True),StructField('int_accrd_bond_amt', StringType(), True),
                                          StructField('cmmsn_amt', StringType(), True),StructField('tax_state_amt', StringType(), True),
                                          StructField('fee_sec_amt', StringType(), True),StructField('pstg_chrg_amt', StringType(), True),
                                          StructField('gnrl_prps_fee_amt', StringType(), True),StructField('currency_trd_cd', StringType(), True),
                                          StructField('cnvrt_trd_crrn_rt', StringType(), True),StructField('trans_hist_seq_nbr', StringType(), True)]

    b2b_df = read_from_s3(JobContext.get_property(C_S3_STG_COMMON_TRAN_INFO_OUTPUT_LOCATION_PROP_KEY))
    b2f_df = read_from_s3(JobContext.get_property(C_S3_STG_COMMISSION_INFO_OUTPUT_LOCATION_PROP_KEY))
    if b2b_df is None:
        b2b_df = JobContext.glue_context.spark_session.createDataFrame(JobContext.glue_context.spark_session.sparkContext.emptyRDD(),schema=StructType(C_B2B_COL_LIST))
    if b2f_df is None:
        b2f_df = JobContext.glue_context.spark_session.createDataFrame(JobContext.glue_context.spark_session.sparkContext.emptyRDD(),schema=StructType(C_B2F_COL_LIST))

    l_b2f_b2b_joining_cond= [(b2f_df.client_nbr==b2b_df.client_nbr)
                             & (b2f_df.processing_dt==b2b_df.processing_dt) &   (b2f_df.branch_cd ==b2b_df.branch_cd)
                             & (b2f_df.account_cd ==b2b_df.account_cd) &   (b2f_df.currency_cd==b2b_df.currency_cd)
                             & (b2f_df.type_account_cd==b2b_df.type_account_cd) & (b2f_df.chck_brch_acct_nbr==b2b_df.chck_brch_acct_nbr)
                             & (b2f_df.security_adp_nbr==b2b_df.security_adp_nbr) & (b2f_df.trans_acct_hist_cd==b2b_df.trans_acct_hist_cd)
                             & (b2f_df.transaction_dt==b2b_df.transaction_dt) & (b2f_df.debit_credit_cd==b2b_df.debit_credit_cd)
                             & (b2f_df.trans_hist_seq_nbr == b2b_df.trans_hist_seq_nbr)]

    JobContext.logger.info("Joining b2b & b2f datasets")
    b2b_b2f_lookup_df= b2f_df.join(b2b_df,l_b2f_b2b_joining_cond) \
        .select(b2b_df.trans_control_id,b2f_df.branch_cd,b2f_df.account_cd,b2f_df.type_account_cd,
                b2f_df.chck_brch_acct_nbr,b2f_df.security_adp_nbr,b2f_df.frst_mny_amt,b2f_df.int_accrd_bond_amt,
                b2f_df.cmmsn_amt,b2f_df.tax_state_amt,b2f_df.fee_sec_amt,b2f_df.pstg_chrg_amt,
                b2f_df.gnrl_prps_fee_amt,b2f_df.currency_trd_cd,b2f_df.currency_cd,b2f_df.cnvrt_trd_crrn_rt,
                b2f_df.trans_hist_seq_nbr,b2f_df.debit_credit_cd,b2f_df.trans_acct_hist_cd).distinct()

    l_joining_cond = [(b2b_b2f_lookup_df.branch_cd==src_df.branch_cd)
                      & (b2b_b2f_lookup_df.trans_acct_hist_cd==src_df.trans_acct_hist_cd)
                      & (b2b_b2f_lookup_df.trans_control_id==src_df.trans_control_id)
                      & (b2b_b2f_lookup_df.account_cd ==src_df.account_cd)
                      & (b2b_b2f_lookup_df.type_account_cd==src_df.type_account_cd)
                      & (b2b_b2f_lookup_df.chck_brch_acct_nbr==src_df.chck_brch_acct_nbr)
                      & (b2b_b2f_lookup_df.security_adp_nbr== src_df.security_adp_nbr)
                      & (b2b_b2f_lookup_df.trans_hist_seq_nbr ==src_df.trans_hist_seq_nbr)
                      & (b2b_b2f_lookup_df.debit_credit_cd==src_df.debit_credit_cd)]

    JobContext.logger.info("Joining b2b_b2f joined dataset with Source data")
    l_src_df = src_df.join(b2b_b2f_lookup_df,l_joining_cond,'left_outer') \
        .select(src_df["*"],b2b_b2f_lookup_df.trans_control_id.alias("b2f_trans_control_id"),
                b2b_b2f_lookup_df.cmmsn_amt.cast(DecimalType(13,2)).alias("b2f_cmmsn_amt"),
                b2b_b2f_lookup_df.tax_state_amt.cast(DecimalType(13,2)).alias("b2f_tax_state_amt"),
                b2b_b2f_lookup_df.fee_sec_amt.cast(DecimalType(13,2)).alias("b2f_fee_sec_amt"),
                b2b_b2f_lookup_df.pstg_chrg_amt.cast(DecimalType(11,2)).alias("b2f_pstg_chrg_amt"),
                b2b_b2f_lookup_df.gnrl_prps_fee_amt.cast(DecimalType(13,2)).alias("b2f_gnrl_prps_fee_amt"),
                b2b_b2f_lookup_df.cnvrt_trd_crrn_rt.cast(DecimalType(15,8)).alias("b2f_cnvrt_trd_crrn_rt"),
                b2b_b2f_lookup_df.int_accrd_bond_amt.cast(DecimalType(17,2)).alias("b2f_int_accrd_bond_amt"),
                b2b_b2f_lookup_df.frst_mny_amt.cast(DecimalType(17,2)).alias("b2f_frst_mny_amt"),
                b2b_b2f_lookup_df.currency_trd_cd.alias("b2f_currency_trd_cd"),
                b2b_b2f_lookup_df.currency_cd.alias("b2f_currency_cd"))

    JobContext.logger.info("Fetched b2b/b2f data")
    return l_src_df

###############################
# Perform lookup on mtdt_exclude_instrument table
###############################
def lookup_mtdt_exclude_instrument(src_df, adp_col):
    JobContext.logger.info("Starting MTDT exclude logic for instrument")
    g_mtdt_ex_instr = read_from_s3_catalog_convert_to_df(JobContext.get_property(C_GLUE_S3_DB_NAME_PROP_KEY)
                                                         ,JobContext.get_property(C_GLUE_S3_TABLE_MTDT_EXCLUDE_INSTR_PROP_KEY),
                                                         ["start_sor_instrument_id","end_sor_instrument_id"])

    g_mtdt_ex_instr_range = g_mtdt_ex_instr.where(sf.col("start_sor_instrument_id") != sf.col("end_sor_instrument_id"))
    g_mtdt_ex_instr = g_mtdt_ex_instr.where(sf.col("start_sor_instrument_id") == sf.col("end_sor_instrument_id"))
    # if (g_mtdt_ex_instr is None) | (g_mtdt_ex_instr.first() is None):
    #     JobContext.logger.info("MTDT exclude instrument is empty.")
    #     src_df = src_df.withColumn("start_sor_instrument_id",sf.lit("-1"))
    #     return src_df

    src_df = src_df.join(sf.broadcast(g_mtdt_ex_instr),src_df[adp_col] == g_mtdt_ex_instr["start_sor_instrument_id"],
                         "left_outer").select(src_df["*"], g_mtdt_ex_instr["start_sor_instrument_id"].alias("st_inst"))

    src_df = src_df.alias("src").join(sf.broadcast(g_mtdt_ex_instr_range).alias("lkp"),(sf.col("src.st_inst").isNull()) &
                                      (src_df[adp_col].between(sf.col("lkp.start_sor_instrument_id"),
                                                               sf.col("lkp.end_sor_instrument_id"))),"left_outer") \
        .select(src_df["*"], sf.coalesce(sf.col("src.st_inst"),sf.col("lkp.start_sor_instrument_id")).alias("start_sor_instrument_id"))
    JobContext.logger.info("MTDT exclude logic for instrument completed")
    return src_df.drop("st_inst")

###############################
# Perform lookup on mtdt_auto_update table
###############################
def lookup_mtdt_exclude_auto_instrument(src_df, adp_col):
    JobContext.logger.info("Starting MTDT exclude logic for auto update")
    #Read mtdt update
    g_mtdt_updt_df = read_from_s3_catalog_convert_to_df(JobContext.get_property(C_GLUE_S3_DB_NAME_PROP_KEY)
                                                        ,JobContext.get_property(C_GLUE_S3_TABLE_MTDT_AUTO_UPDATE_PROP_KEY))
    g_mtdt_updt_df = g_mtdt_updt_df.filter(g_mtdt_updt_df.entity_type_cd=='SECURITY')
    g_mtdt_updt_df = transform_boolean_clm(g_mtdt_updt_df, "active_flg")
    g_window_dim = Window.partitionBy(g_mtdt_updt_df.entity_id).orderBy(g_mtdt_updt_df.created_ts.desc())
    g_mtdt_updt_df = g_mtdt_updt_df.withColumn("latest_rec", sf.row_number().over(g_window_dim) == 1).filter("latest_rec").filter("active_flg")

    #Read Dim Instrument table
    dim_instrument_df = read_from_s3_catalog_convert_to_df(JobContext.get_property(C_GLUE_S3_DB_NAME_PROP_KEY)
                                                           ,JobContext.get_property(C_GLUE_S3_TABLE_DIM_INSTRUMENT_PROP_KEY))
    dim_instrument_df = dim_instrument_df.select("instrument_id","sor_instrument_id").distinct()

    #Check for valid security
    check_secutity_df = g_mtdt_updt_df.join(dim_instrument_df, g_mtdt_updt_df.entity_id == dim_instrument_df.instrument_id, how="inner").select("sor_instrument_id")
    src_df = src_df.join(check_secutity_df,src_df[adp_col] == check_secutity_df.sor_instrument_id, how="left_outer") \
        .select(src_df["*"], check_secutity_df["sor_instrument_id"])
    JobContext.logger.info("MTDT exclude logic for auto update completed")
    return src_df


##########################################################
#  Common util to generate new instrument_id for stub jobs
##########################################################
def mtdt_instrument_id_gen(self, in_df,in_df_security_col_nm='instrument_ref_nm', instrIdColName="instrument_id", created_user_nm = C_CREATED_USER_NM):

        #Read mtdt instrument id gen table and get max instrument_id
        mtdt_instrument_id_ref_gen_df = read_from_s3_catalog_convert_to_df(self.C_DB_NAME,JobContext.get_property(C_GLUE_S3_TABLE_MTDT_INSTR_ID_GEN_PROP_KEY))
        mtdt_instrument_id_ref_gen_df = mtdt_instrument_id_ref_gen_df.withColumnRenamed("instrument_id","instrument_id_mtdt").withColumnRenamed("instrument_ref_nm","instrument_ref_nm_mtdt")
        max_instr_id_df = mtdt_instrument_id_ref_gen_df.select(sf.col('instrument_id_mtdt').cast('bigint'))
        max_instr_id_df = max_instr_id_df.agg({"instrument_id_mtdt":"max"}).withColumnRenamed("max(instrument_id_mtdt)","instrument_id_mtdt").collect()
        max_instr_id = max_instr_id_df[0].instrument_id_mtdt

        #Check for null in max instrument id
        offset=max_instr_id if max_instr_id != None else C_NEW_INSTRUMENT_START_POSITION

        #Check for existance of new instrument_ref_nm in MTDT Instrument gen table
        in_df=in_df if "instrument_id" not in in_df.columns else in_df.drop("instrument_id")

        ref_not_exist_in_mtdt_df = in_df.join(mtdt_instrument_id_ref_gen_df, in_df[in_df_security_col_nm]==mtdt_instrument_id_ref_gen_df['instrument_ref_nm_mtdt'], how='left_outer') \
            .filter(mtdt_instrument_id_ref_gen_df.instrument_id_mtdt.isNull()).select(sf.col(in_df_security_col_nm).alias('instrument_ref_nm')).distinct()
        #Define Schema for dataframe
        df_schema = ref_not_exist_in_mtdt_df.withColumn("instrument_id", sf.lit(1).cast('bigint'))
        #Generate instrument id
        zipped_rdd = ref_not_exist_in_mtdt_df.rdd.zipWithIndex().map(lambda ri: ri[0]+(ri[1]+offset+1,))
        write_to_mtdt_df = self.spark.createDataFrame(zipped_rdd, schema=df_schema.schema)
        write_to_mtdt_df = write_to_mtdt_df.withColumn('active_flg',sf.lit(True))\
                                            .withColumn('created_user_nm',sf.lit(created_user_nm)) \
                                            .withColumn('created_ts', sf.lit(sf.current_timestamp()))\
            .withColumn('created_ts', sf.from_utc_timestamp(sf.col('created_ts'), "EST"))
        write_to_mtdt_df =write_to_mtdt_df.select("instrument_id","instrument_ref_nm","active_flg","created_user_nm","created_ts")
        write_to_mtdt_df.cache()
        full_mtdt_df = write_to_mtdt_df.select("instrument_id","instrument_ref_nm").union(mtdt_instrument_id_ref_gen_df.select("instrument_id_mtdt","instrument_ref_nm_mtdt"))
        save_output_to_s3(write_to_mtdt_df,"write_to_mtdt_df",self._get_property(C_S3_MTDT_INSTR_ID_GEN_OUTPUT_LOCATION_PROP_KEY))
        out_df = in_df.join(full_mtdt_df, in_df[in_df_security_col_nm] == full_mtdt_df['instrument_ref_nm'], how='left_outer')\
                        .select(in_df['*'],full_mtdt_df.instrument_id.alias(instrIdColName))

        return out_df


##############################################################################
#  Common util to generate new corp_action_decl_id for dim corp action declaration
##############################################################################
def mtdt_corp_action_decl_id_gen(self, in_df,in_df_security_col_nm='sor_corp_action_id', instrIdColName="corp_action_decl_id", created_user_nm = C_CREATED_USER_NM):

    #Read mtdt_corp_action_decl_id_gen table and get max corp_action_decl_id
    mtdt_corp_action_decl_id_gen_df = read_from_s3_catalog_convert_to_df(self.C_DB_NAME,JobContext.get_property(C_GLUE_S3_TABLE_MTDT_CORP_ACTN_DECL_ID_GEN_PROP_KEY))
    mtdt_corp_action_decl_id_gen_df = mtdt_corp_action_decl_id_gen_df.withColumnRenamed("corp_action_decl_id","corp_action_decl_id_mtdt") \
        .withColumnRenamed("sor_corp_action_id","sor_corp_action_id_mtdt")
    max_instr_id_df = mtdt_corp_action_decl_id_gen_df.select(sf.col('corp_action_decl_id_mtdt').cast('bigint'))
    max_instr_id_df = max_instr_id_df.agg({"corp_action_decl_id_mtdt":"max"}).withColumnRenamed("max(corp_action_decl_id_mtdt)","corp_action_decl_id_mtdt").collect()
    max_instr_id = max_instr_id_df[0].corp_action_decl_id_mtdt

    #Check for null in max instrument id
    offset=max_instr_id if max_instr_id != None else C_NEW_INSTRUMENT_START_POSITION

    #Check for existance of new sor_corp_action_id in MTDT Instrument gen table
    in_df=in_df if "corp_action_decl_id" not in in_df.columns else in_df.drop("corp_action_decl_id")

    ref_not_exist_in_mtdt_df = in_df.join(mtdt_corp_action_decl_id_gen_df, in_df[in_df_security_col_nm]==mtdt_corp_action_decl_id_gen_df['sor_corp_action_id_mtdt'], how='left_outer') \
        .filter(mtdt_corp_action_decl_id_gen_df.corp_action_decl_id_mtdt.isNull()).select(sf.col(in_df_security_col_nm).alias('sor_corp_action_id')).distinct()
    #Define Schema for dataframe
    df_schema = ref_not_exist_in_mtdt_df.withColumn("corp_action_decl_id", sf.lit(1).cast('bigint'))
    #Generate instrument id
    zipped_rdd = ref_not_exist_in_mtdt_df.rdd.zipWithIndex().map(lambda ri: ri[0]+(ri[1]+offset+1,))
    write_to_mtdt_df = self.spark.createDataFrame(zipped_rdd, schema=df_schema.schema)
    write_to_mtdt_df = write_to_mtdt_df.withColumn('active_flg',sf.lit(True)) \
        .withColumn('created_user_nm',sf.lit(created_user_nm)) \
        .withColumn('created_ts', sf.lit(sf.current_timestamp()))\
        .withColumn('created_ts', sf.from_utc_timestamp(sf.col('created_ts'), "EST"))
    write_to_mtdt_df =write_to_mtdt_df.select("corp_action_decl_id","sor_corp_action_id","active_flg","created_user_nm","created_ts")
    full_mtdt_df = write_to_mtdt_df.select("corp_action_decl_id","sor_corp_action_id") \
        .union(mtdt_corp_action_decl_id_gen_df.select("corp_action_decl_id_mtdt","sor_corp_action_id_mtdt"))
    save_output_to_s3(write_to_mtdt_df,"write_to_mtdt_df",self._get_property(C_S3_MTDT_CORP_ACTN_DECL_ID_GEN_OUTPUT_LOCATION_PROP_KEY))
    out_df = in_df.join(full_mtdt_df, in_df[in_df_security_col_nm] == full_mtdt_df['sor_corp_action_id'], how='left_outer') \
        .select(in_df['*'],full_mtdt_df.corp_action_decl_id.alias(instrIdColName))

    return out_df

def derive_dim_instrument(source_df, dim_instrument_ref_df,src_sor_instrument_id_clm_name, instrument_type_cd, select_clm_list):
    JobContext.logger.info("Joining with dim_instrument")
    instrument_filter_cond = sf.col('instrument_type_cd') == instrument_type_cd
    sor_instrument_join_cond = (sf.col('sor_instrument_id') == sf.col(src_sor_instrument_id_clm_name))
    source_df = source_df.join(dim_instrument_ref_df, sor_instrument_join_cond & instrument_filter_cond, 'left_outer').select(
        select_clm_list)
    JobContext.logger.info("Joined with dim_instrument")
    return source_df

##############################################################################
#  Common util to generate new sleeve_id for dim sleeve
##############################################################################
def mtdt_sleeve_id_gen(self, in_df,in_df_security_col_nm='sleeve_id', instrIdColName="sleeve_gf_id", created_user_nm = C_CREATED_USER_NM):

    #Read mtdt instrument id gen table and get max sleeve_gf_id
    mtdt_sleeve_id_gen_df = read_from_s3_catalog_convert_to_df(JobContext.get_property(C_GLUE_S3_DB_NAME_PROP_KEY),JobContext.get_property(C_GLUE_S3_TABLE_MTDT_SLEVE_ID_GEN_PROP_KEY))
    mtdt_sleeve_id_gen_df = mtdt_sleeve_id_gen_df.withColumnRenamed("sleeve_gf_id","sleeve_gf_id_mtdt").withColumnRenamed("sleeve_id","sleeve_id_mtdt")
    max_instr_id_df = mtdt_sleeve_id_gen_df.select(sf.col('sleeve_gf_id_mtdt').cast('bigint'))
    max_instr_id_df = max_instr_id_df.agg({"sleeve_gf_id_mtdt":"max"}).withColumnRenamed("max(sleeve_gf_id_mtdt)","sleeve_gf_id_mtdt").collect()
    max_instr_id = max_instr_id_df[0].sleeve_gf_id_mtdt

    #Check for null in max instrument id
    offset=max_instr_id if max_instr_id != None else C_NEW_INSTRUMENT_START_POSITION

    #Check for existance of new sleeve_id in MTDT Instrument gen table
    in_df=in_df if "sleeve_gf_id" not in in_df.columns else in_df.drop("sleeve_gf_id")

    ref_not_exist_in_mtdt_df = in_df.join(mtdt_sleeve_id_gen_df, in_df[in_df_security_col_nm]==mtdt_sleeve_id_gen_df['sleeve_id_mtdt'], how='left_outer') \
        .filter(mtdt_sleeve_id_gen_df.sleeve_gf_id_mtdt.isNull()).select(sf.col(in_df_security_col_nm).alias('sleeve_id')).distinct()
    #Define Schema for dataframe
    df_schema = ref_not_exist_in_mtdt_df.withColumn("sleeve_gf_id", sf.lit(1).cast('bigint'))
    #Generate instrument id
    zipped_rdd = ref_not_exist_in_mtdt_df.rdd.zipWithIndex().map(lambda ri: ri[0]+(ri[1]+offset+1,))
    write_to_mtdt_df = JobContext.glue_context.spark_session.createDataFrame(zipped_rdd, schema=df_schema.schema)
    write_to_mtdt_df = write_to_mtdt_df.withColumn('active_flg',sf.lit(True)) \
        .withColumn('instrument_id',sf.lit('')) \
        .withColumn('created_user_nm',sf.lit(created_user_nm)) \
        .withColumn('created_ts', sf.lit(sf.current_timestamp()))\
        .withColumn('created_ts', sf.from_utc_timestamp(sf.col('created_ts'), "EST"))
    write_to_mtdt_df =write_to_mtdt_df.select("sleeve_gf_id","sleeve_id","instrument_id","active_flg","created_user_nm","created_ts")
    write_to_mtdt_df.cache()
    full_mtdt_df = write_to_mtdt_df.select("sleeve_gf_id","sleeve_id").union(mtdt_sleeve_id_gen_df.select("sleeve_gf_id_mtdt","sleeve_id_mtdt"))
    save_output_to_s3(write_to_mtdt_df,"write_to_mtdt_df",JobContext.get_property(C_S3_MTDT_SLEEVE_ID_GEN_OUTPUT_LOCATION_PROP_KEY))
    out_df = in_df.join(full_mtdt_df, in_df[in_df_security_col_nm] == full_mtdt_df['sleeve_id'], how='left_outer') \
        .select(in_df['*'],full_mtdt_df.sleeve_gf_id.alias(instrIdColName))

    return out_df

def derive_account_id_from_dtl_account( src_df, account_df, p_joining_cond, broadcast=True, account_id_alias="account_id"):
    JobContext.logger.info("Deriving account with joining condition {}, broadcast: {}, accoint id alias : {}".format(p_joining_cond, broadcast,account_id_alias))
    if broadcast:
        JobContext.logger.info("Broadcast the account table")
        account_df = account_df.hint("broadcast")

    src_df = src_df.alias('src').join(account_df.alias('acc'), p_joining_cond, "left") \
        .select( src_df['*'],sf.col("acc.account_id").alias(account_id_alias))
    JobContext.logger.info("account_id is derived using joining condition {}".format(p_joining_cond))
    return src_df

def pick_latest_from_dtl_account( account_df):

    JobContext.logger.info("Pick latest account records")
    account_df = transform_boolean_clm(account_df, "active_flg")
    l_dim_acc_window_spec = Window.partitionBy(account_df['account_id'], account_df['account_ref_type_cd']) \
        .orderBy(account_df['created_ts'].desc())
    account_df = account_df.withColumn("row_num", sf.row_number().over(l_dim_acc_window_spec)) \
        .where((sf.col("row_num") == 1) & (sf.col("active_flg") == True))

    return account_df



def pick_latest_from_dim_instrument( dim_instrument_df):

    JobContext.logger.info("Pick latest account records")
    dim_instrument_df = transform_boolean_clm(dim_instrument_df, "active_flg")
    window_spec = Window.partitionBy(dim_instrument_df['sor_instrument_id'], dim_instrument_df['instrument_type_cd']) \
        .orderBy(dim_instrument_df['created_ts'].desc())
    dim_instrument_df = dim_instrument_df.withColumn("row_num", sf.row_number().over(window_spec)) \
        .where((sf.col("row_num") == 1) & (sf.col("active_flg") == True))

    return dim_instrument_df

def pick_latest_from_dtl_instrument(dtl_instrument_df, windows_clms=["instrument_id", "instrument_ref_type_cd"]):

    JobContext.logger.info("Pick latest account records")
    dtl_instrument_df = transform_boolean_clm(dtl_instrument_df, "active_flg")
    window_spec = Window.partitionBy(windows_clms) \
        .orderBy(dtl_instrument_df['created_ts'].desc())
    dtl_instrument_df = dtl_instrument_df.withColumn("row_num", sf.row_number().over(window_spec)) \
        .where((sf.col("row_num") == 1) & (sf.col("active_flg") == True))

    return dtl_instrument_df

#passing job_object is not needed, but mtdt_instrument_id_gen, which recieve self, it doesn't seems correct apporach

#passing job_object is not needed, but mtdt_instrument_id_gen, which recieve self, it doesn't seems correct apporach
def create_stub_for_ben_securities(job_object, source_df, dim_instrument, partition_clms, security_clm, weight_p_clm,
                                   default_clm_map,clms_for_further_steps, build_inst_desc_nam ="src_inst_desc", instrument_name_2 =None,
                                   p_sor_instrument_id_prefix =C_BEN_DIM_INSTRUMENT_SOR_REF_PREFIX, child_class_type_cd=None, child_class_val=None, p_broadcast=True ):

    JobContext.logger.info("Creating stub record for benchmark association using dim_instrument, partition columns {}, security column, build_inst_desc_nam "
                           .format(str(partition_clms),security_clm,  build_inst_desc_nam))

    """
    joining based on instrument_desc, it is important as sor_instrument_id derived from instrument_id, only instrument_desc will have source Blend security information  
    Ex:PBLND10035(45.00% PW_IBOTZ,55.00% PW_EAFEHN)
    
    PBLND10035=> sor_instrument_id
    45.00% PW_IBOTZ,55.00% PW_EAFEHN=> means this security is blend of 45% PW_IBOTZ and 55% of PW_EAFEHN blean has 50.00 % 
    """

    if instrument_name_2 is None:
        instrument_name_2 = security_clm

    # this will make it (BLEND_)(\d+)(\()(.+)(\))
    INSTRUMENT_DESC_REGEX = "({})(\d+)(\()(.+)(\))".format(p_sor_instrument_id_prefix)

    """()=> represent one group 
      
    Regex INSTRUMENT_DESC_REGEX has following 5 groups
    Ex input=>BLEND_111(.4500%  PBLND1136\,.550000%  PBS100) 
     group 1=>BLEND_
     group 2=>111
     group 3=>(
     group 4=>.4500%  PBLND1136\,.550000%  PBS100=> we need this
     group 5=>)"""

    dim_instrument.cache()
    const_df = JobContext.glue_context.spark_session.createDataFrame([(C_FIRM_ID,)],["constid_for_join"])
    source_df = prepare_instrument_desc_by_conat_rec_data(source_df,partition_clms, instrument_name_2, weight_p_clm, build_inst_desc_nam)
    source_df = source_df.crossJoin(sf.broadcast(const_df))
    join_cond = (source_df["constid_for_join"]==dim_instrument["firm_id"]) & ((sf.col(build_inst_desc_nam) == sf.col("instrument_desc")) | \
                ((sf.col(weight_p_clm)==1.00) & (sf.col(security_clm)==sf.col("sor_instrument_id"))))

    if p_broadcast:
        source_df = source_df.join(sf.broadcast(dim_instrument), join_cond, "left") \
                    .select(source_df['*'], dim_instrument.instrument_id.alias("ben_lkp_instrument_id"))
    else:
        source_df = source_df.join(dim_instrument, join_cond, "left") \
            .select(source_df['*'], dim_instrument.instrument_id.alias("ben_lkp_instrument_id"))

    source_df.cache()
    #todo -  Remove plan before merge
    JobContext.logger.info(f"plannn - {source_df._jdf.queryExecution().simpleString()}")
    src_old_instrument_df = source_df.filter(sf.col("ben_lkp_instrument_id").isNotNull()).select(*clms_for_further_steps, 'ben_lkp_instrument_id')
    new_ben_instrument_df = source_df.filter(sf.col("ben_lkp_instrument_id").isNull())

    #From start link conversion data to batch belend instrument record
    new_ben_instrument_df = new_ben_instrument_df.drop("ben_lkp_instrument_id")
    instrument_desc_df = read_from_s3_catalog_convert_to_df(JobContext.get_property(C_GLUE_S3_DB_NAME_PROP_KEY),JobContext.get_property(C_GLUE_S3_TABLE_DIM_INSTRUMENT_PROP_KEY))
    instrument_desc_df1 = instrument_desc_df.filter((sf.col('instrument_type_cd')=='BEN') & (sf.col('constituent_benchmark_id').isNotNull()) & (sf.col('constituent_benchmark_prct')!=1))
    instrument_desc_df2 = instrument_desc_df.filter((sf.col('instrument_type_cd')=='BEN') & (sf.col('constituent_benchmark_id').isNull()) & (sf.col("instrument_name_2").isNotNull()))
    instrument_desc_df2 = instrument_desc_df2.select ( sf.col("instrument_id"),sf.col("instrument_name_2"),sf.col("firm_id")).distinct()
    instrument_desc_df1 = instrument_desc_df1.select ( sf.col("instrument_id"),sf.col("constituent_benchmark_id"),sf.col("constituent_benchmark_prct"),sf.col("firm_id")).distinct()
    instrument_desc_df1 = instrument_desc_df1.alias("src").join(sf.broadcast(instrument_desc_df2).alias("brd"), sf.col("brd.instrument_id")==sf.col("src.constituent_benchmark_id")) \
                            .select(instrument_desc_df1['*'], sf.col("instrument_name_2"))
    instrument_desc_df1 = prepare_instrument_desc_by_conat_rec_data(instrument_desc_df1, ["instrument_id"], "instrument_name_2", "constituent_benchmark_prct", constracted_clm_name="c_instrument_desc")
    instrument_desc_df1 = instrument_desc_df1.select ( sf.col("instrument_id"),sf.col("c_instrument_desc"),sf.col("firm_id")).distinct()
    g_window = Window.partitionBy("c_instrument_desc")
    instrument_desc_df1 = instrument_desc_df1.withColumn("max_instrument_id", sf.max("instrument_id").over(g_window)) \
        .where(sf.col("instrument_id") == sf.col("max_instrument_id"))
    instrument_desc_df1 = instrument_desc_df1.drop("max_instrument_id")
    join_cond1 = (new_ben_instrument_df["constid_for_join"]==instrument_desc_df1["firm_id"]) & (sf.col(build_inst_desc_nam) == sf.col("c_instrument_desc"))
    new_ben_instrument_df = new_ben_instrument_df.join(sf.broadcast(instrument_desc_df1), join_cond1, "left") \
               .select(new_ben_instrument_df['*'], instrument_desc_df1.instrument_id.alias("ben_lkp_instrument_id"))
    src_old_instrument_df1 = new_ben_instrument_df.filter(sf.col("ben_lkp_instrument_id").isNotNull()).select(*clms_for_further_steps, 'ben_lkp_instrument_id')
    new_ben_instrument_df = new_ben_instrument_df.filter(sf.col("ben_lkp_instrument_id").isNull())
    #End link conversion data to batch belend instrument record

    sf_partition_clms = [sf.col(clm).desc() for clm in partition_clms ]
    window_spec = Window.partitionBy(new_ben_instrument_df[build_inst_desc_nam]) \
        .orderBy(sf_partition_clms)
    source_df = source_df.drop("constid_for_join")
    src_new_instrument_df = new_ben_instrument_df.drop("ben_lkp_instrument_id")

    new_ben_instrument_df = new_ben_instrument_df.withColumn("row_num",sf.dense_rank().over(window_spec))
    new_ben_instrument_df = new_ben_instrument_df.filter("row_num==1")
    new_ben_instrument_df.cache()

    new_ben_instrument_df = mtdt_instrument_id_gen(job_object, new_ben_instrument_df,in_df_security_col_nm = build_inst_desc_nam, created_user_nm=C_CREATED_USER_NM)
    new_ben_instrument_df.cache()

    join_cond =[new_ben_instrument_df[build_inst_desc_nam]==src_new_instrument_df[build_inst_desc_nam], new_ben_instrument_df["constituent_benchmark_id"]==src_new_instrument_df["constituent_benchmark_id"],
                new_ben_instrument_df[weight_p_clm]==src_new_instrument_df[weight_p_clm]]
    src_new_instrument_df = src_new_instrument_df.alias("src").join(new_ben_instrument_df.alias("inst"), join_cond,"left_outer") \
        .select(src_new_instrument_df['*'],sf.col("instrument_id").alias("ben_lkp_instrument_id")).select(*clms_for_further_steps, "ben_lkp_instrument_id")
    src_for_brd_rec_df = src_new_instrument_df.union(src_old_instrument_df).union(src_old_instrument_df1)

    src_for_brd_rec_df.cache()
    new_ben_instrument_df = new_ben_instrument_df.withColumn("sor_instrument_id", sf.concat(sf.lit(p_sor_instrument_id_prefix), sf.col("instrument_id")))
    #TODO: Not sure if this is correct
    new_ben_instrument_df = new_ben_instrument_df.withColumn("source_system_id", sf.lit(C_COMMON_SOURCE_SYSTEM_ID))

    currency_Cd = derive_currency_cd()
    new_ben_instrument_df = new_ben_instrument_df.withColumn('currency_cd', sf.lit(currency_Cd))

    new_ben_instrument_df  = add_default_clm(new_ben_instrument_df , default_clm_map)
    new_ben_instrument_df  = add_audit_columns(new_ben_instrument_df,p_batch_date_clm=job_object.g_batch_date_clm,add_modification_clm=True)

    window_mngd_clm = Window.partitionBy(sf.col("instrument_id")).orderBy(sf.col("constituent_benchmark_id"))

    driver_inst = new_ben_instrument_df.withColumn("row_num", sf.row_number().over(window_mngd_clm)).filter("row_num==1")
    driver_inst = driver_inst.withColumn("constituent_benchmark_id", sf.lit(None)) \
        .withColumn("constituent_benchmark_start_dt", sf.lit(None)) \
        .withColumn("constituent_benchmark_end_dt", sf.lit(None)) \
        .withColumn("constituent_benchmark_prct", sf.lit(None))

    new_ben_instrument_df = new_ben_instrument_df.select(*C_DIM_INSTRUMENT_COLUMN_LIST).union(driver_inst.select(*C_DIM_INSTRUMENT_COLUMN_LIST))

    # Removed concatenation in instrument_desc field
    new_ben_instrument_df = new_ben_instrument_df.withColumn("instrument_desc",sf.col('instrument_desc'))
    #creating stub for dtl_instrument_ref
    driver_inst.cache()
    write_to_dtl_instrument_ref = driver_inst.withColumn('instrument_ref_type_cd',sf.lit('SOR_BEN'))\
        .withColumn('instrument_ref_nm',sf.col('sor_instrument_id')) \
        .withColumnRenamed("modified_user_nm","modified_user") \
        .withColumnRenamed("modified_program_nm","modified_program")
    save_output_to_s3(write_to_dtl_instrument_ref.select(*C_DTL_INSTRUMENT_REF_FIELDS),'dtl_instrument_ref',job_object._get_property(C_S3_DTL_INSTRUMENT_REF_OUTPUT_LOCATION_PROP_KEY))
    #saving in dim_instrument
    save_output_to_s3(new_ben_instrument_df, "target_df", job_object._get_property(C_S3_DIM_INSTRUMENT_OUTPUT_LOC_PROP_KEY))

    dim_class_type_df= None
    if (child_class_type_cd is not None) & (child_class_val is not None):
        JobContext.logger.info(f"Passed child_class_type_cd is: {child_class_type_cd} and child_class_val is: {child_class_val}")
        dim_class_type_df = read_from_catalog_and_filter_inactive_rec(JobContext.get_property(C_GLUE_S3_TABLE_DIM_CLASS_TYPE)) \
                                    .filter((sf.upper(sf.col("child_class_type_cd"))==child_class_type_cd )&
                                        (sf.upper(sf.col('child_class_val')) == child_class_val)) \
                                        .select("class_type_id").first()
    else:
        JobContext.logger.info("child_class_type_cd and child_class_val both are required for fac_class_entity records")

    if dim_class_type_df is not None:
        JobContext.logger.info("Preparing Fact_class_entity records for class_type_id {}")
        fact_class_df = new_ben_instrument_df.select(sf.col("instrument_id").alias("entity_id")).distinct()
        class_type_id = dim_class_type_df.asDict()['class_type_id']
        JobContext.logger.info("Class_type_id {}, found for custom blend flg".format(class_type_id))
        C_FACT_CLASS_ENTITY_DEFAULT_CLM_MAPPING = {'entity_type_cd': C_FACT_CLASS_ENTITY_REL_ENTITY_TYPE_CD_BENCHMARK, 'overriding_entity_type_cd': None,
                                                   'overriding_entity_id': None, 'effective_from_dt': C_DEFAULT_START_DATE,
                                                   'effective_to_dt': C_DEFAULT_END_DATE, 'allocation_prcnt': None, 'firm_id': C_FIRM_ID}

        C_FACT_CLASS_ENTITY_COL_LIST = ['entity_id','class_type_id','entity_type_cd','overriding_entity_type_cd',
                                    'overriding_entity_id','effective_from_dt','effective_to_dt','allocation_prcnt',
                                    'firm_id','active_flg','batch_dt','created_program_nm','created_user_nm','created_ts']
        fact_class_df = fact_class_df.withColumn("class_type_id", sf.lit(class_type_id))
        fact_class_df = add_default_clm(fact_class_df,C_FACT_CLASS_ENTITY_DEFAULT_CLM_MAPPING)
        fact_class_df = add_audit_columns(fact_class_df, JobContext.batch_date_clm,created_user_nm=C_CONVERSION_USER_NM)
        save_output_to_s3(fact_class_df.select(C_FACT_CLASS_ENTITY_COL_LIST),"fact_class_entity",JobContext.get_property(C_S3_FACT_CLASS_ENTITY_OUTPUT_LOCATION))
    else:
        JobContext.logger.info(f"Custom blend flag is not present in Dim_class_type with child_class_type_cd is: {child_class_type_cd} "
                               f"and child_class_val is: {child_class_val} , so skipping fact_class_entity records")

    JobContext.logger.info("Created stub record for benchmark association using dim_instrument ")
    return src_for_brd_rec_df

def prepare_instrument_desc_by_conat_rec_data(src_df, partition_clms, security_clm, weight_p_clm, constracted_clm_name="c_instrument_desc"):
    JobContext.logger.info("Preparing instrument_desc for benchmark records, passed security_clm: {}, weight_p_clm: {}".format(security_clm,weight_p_clm))
    # using unqiue name for security_weight_
    percentage_weight_clm = f"percentage_{src_df}"
    JobContext.logger.info(f"Prepared new weight column {percentage_weight_clm }")
    src_df = src_df.withColumn(percentage_weight_clm , (sf.col(weight_p_clm)*100).cast(DecimalType(5,2)))
    src_df = src_df.withColumn("security_weight_!", sf.concat_ws(C_BENCHMARK_INSTURME_DESC_SECURITY_WEIGHT_SEP_CHAR, sf.col(percentage_weight_clm ), sf.col(security_clm)))
    sf_partition_clms = [sf.col(clm) for clm in partition_clms ]
    window = Window.partitionBy(*sf_partition_clms)
    src_df = src_df.withColumn(constracted_clm_name, sf.concat_ws(C_BENCHMARK_INSTURME_DESC_SECURITIES_SEP_CHAR, sf.sort_array(sf.collect_list("security_weight_!").over(window))))
    JobContext.logger.info("Prepared instrument_desc for benchmark records")
    return src_df


##############################################################################
#  Common util to generate new sleeve id for instrument_id of MF/ETF sleeve
##############################################################################
def mtdt_mf_etf_sleeve_id_gen(self, in_df,in_df_security_col_nm='instrument_id', instrIdColName="sleeve_gf_id", created_user_nm = C_CREATED_USER_NM):

    #Read mtdt instrument id gen table and get max sleeve_gf_id
    mtdt_sleeve_id_gen_df = read_from_s3_catalog_convert_to_df(self.C_DB_NAME,JobContext.get_property(C_GLUE_S3_TABLE_MTDT_SLEVE_ID_GEN_PROP_KEY))
    mtdt_sleeve_id_gen_df = mtdt_sleeve_id_gen_df.withColumnRenamed("sleeve_gf_id","sleeve_gf_id_mtdt").withColumnRenamed("instrument_id","instrument_id_mtdt")
    max_instr_id_df = mtdt_sleeve_id_gen_df.select(sf.col('sleeve_gf_id_mtdt').cast('bigint'))
    max_instr_id_df = max_instr_id_df.agg({"sleeve_gf_id_mtdt":"max"}).withColumnRenamed("max(sleeve_gf_id_mtdt)","sleeve_gf_id_mtdt").collect()
    max_instr_id = max_instr_id_df[0].sleeve_gf_id_mtdt

    #Check for null in max instrument id
    offset=max_instr_id if max_instr_id != None else C_NEW_INSTRUMENT_START_POSITION

    #Check for existance of new sleeve_id in MTDT Instrument gen table
    in_df=in_df if "sleeve_gf_id" not in in_df.columns else in_df.drop("sleeve_gf_id")

    ref_not_exist_in_mtdt_df = in_df.join(mtdt_sleeve_id_gen_df, in_df[in_df_security_col_nm]==mtdt_sleeve_id_gen_df['instrument_id_mtdt'], how='left_outer') \
        .filter(mtdt_sleeve_id_gen_df.sleeve_gf_id_mtdt.isNull()).select(sf.col(in_df_security_col_nm).alias('instrument_id')).distinct()
    #Define Schema for dataframe
    df_schema = ref_not_exist_in_mtdt_df.withColumn("sleeve_gf_id", sf.lit(1).cast('bigint'))
    #Generate instrument id
    zipped_rdd = ref_not_exist_in_mtdt_df.rdd.zipWithIndex().map(lambda ri: ri[0]+(ri[1]+offset+1,))
    write_to_mtdt_df = self.spark.createDataFrame(zipped_rdd, schema=df_schema.schema)
    write_to_mtdt_df = write_to_mtdt_df.withColumn('active_flg',sf.lit(True)) \
        .withColumn('sleeve_id',sf.lit('')) \
        .withColumn('created_user_nm',sf.lit(created_user_nm)) \
        .withColumn('created_ts', sf.lit(sf.current_timestamp()))\
        .withColumn('created_ts', sf.from_utc_timestamp(sf.col('created_ts'), "EST"))
    write_to_mtdt_df =write_to_mtdt_df.select("sleeve_gf_id","sleeve_id","instrument_id","active_flg","created_user_nm","created_ts")
    write_to_mtdt_df.cache()
    full_mtdt_df = write_to_mtdt_df.select("sleeve_gf_id","instrument_id").union(mtdt_sleeve_id_gen_df.select("sleeve_gf_id_mtdt","instrument_id_mtdt"))
    save_output_to_s3(write_to_mtdt_df,"write_to_mtdt_df",self._get_property(C_S3_MTDT_SLEEVE_ID_GEN_OUTPUT_LOCATION_PROP_KEY))
    out_df = in_df.join(full_mtdt_df, in_df[in_df_security_col_nm] == full_mtdt_df['instrument_id'], how='left_outer') \
        .select(in_df['*'],full_mtdt_df.sleeve_gf_id.alias(instrIdColName))

    return out_df

##############################################################################
#  Common util to generate Sleeve_id for all transaction jobs
#############################################################################
def derive_transaction_sleeve_id(src_df,p_extra_brdg_acc_rel_cond=None,p_col_filter = "slv_rltn_ind"):
    src_df = src_df.withColumn('derv_account_id',sf.split(src_df.account_id,'-').getItem(0))

    g_src_clms = src_df.columns
    g_src_clms.append('sleeve_id')

    src_default_sleeve_id_df = src_df.filter((~sf.col(p_col_filter).isin(['P','C'])) | (sf.col(p_col_filter).isNull())).withColumn('sleeve_id',sf.lit(C_DEFAULT_SLEEVE_ID_TRANSACTION_VALUE))

    #checking sleeve id in brdg_account_relation i.e sleeve_ind =='P'
    src_sleve_id_derv_C_df = src_df.filter(sf.col(p_col_filter)=='C')
    src_sleve_id_derv_C_df = src_sleve_id_derv_C_df.withColumn("sor_account_id_other", sf.concat(src_sleve_id_derv_C_df["branch_slv_cd"], src_sleve_id_derv_C_df["account_slv_cd"]))
    src_sleve_id_derv_C_df = src_sleve_id_derv_C_df.withColumn('sor_account_id_child', sf.when(sf.col("account_slv_cd").isNull(), sf.col("branch_slv_cd"))
                                                               .when(sf.col("branch_slv_cd").isNull(), sf.col("account_slv_cd")).otherwise(src_sleve_id_derv_C_df["sor_account_id_other"])) \
        .withColumn('join_acc_id',sf.concat_ws('-',sf.col('derv_account_id'),sf.col('sor_account_id_child')))

    dim_sleeve_type_df = read_from_catalog_and_filter_inactive_rec(JobContext.get_property(C_GLUE_S3_TABLE_DIM_SLEEVE_PROP_KEY))
    sleeve_joining_condition = src_sleve_id_derv_C_df['join_acc_id'] == dim_sleeve_type_df['sor_sleeve_id']
    src_sleve_id_derv_C_df = derive_sleeve_id_without_filter_logic(src_sleve_id_derv_C_df, dim_sleeve_type_df, sleeve_joining_condition)
    src_sleve_id_derv_C_df = src_sleve_id_derv_C_df.withColumnRenamed("sleeve_id", "join_sleeve_id")

    #Flow1:generate sleeve id from brdg_account_relation
    brdg_account_relation = read_from_s3_catalog_convert_to_df(JobContext.get_property(C_GLUE_S3_DB_NAME_PROP_KEY), \
                                                               JobContext.get_property(C_GLUE_S3_TABLE_BRDG_ACC_RELATION_PROP_KEY),
                                                               p_fields_to_select=['account_id','entity_id','entity_type_cd','active_flg','created_ts'])
    brdg_account_relation =transform_boolean_clm(brdg_account_relation,'active_flg')
    brdg_account_relation = brdg_account_relation.filter(sf.col('entity_type_cd')=='SLEEVE')

    brdg_account_relation = brdg_account_relation.withColumn('row_num',sf.row_number().over(
        Window.partitionBy(brdg_account_relation['entity_id'], brdg_account_relation['entity_type_cd']).orderBy(
            brdg_account_relation['created_ts'].desc()))).filter("row_num == 1").filter("active_flg").drop('row_num')
    # Adding this column in case fact_account_date condition do not match - Source account id shall be used as account id
    # brdg_account_relation = brdg_account_relation.withColumn("splitted_entity_id",sf.substring_index(sf.col("entity_id"),"-",-2))
    brdg_account_relation = brdg_account_relation.hint("broadcast").cache()
    l_join_cond = ((src_sleve_id_derv_C_df["join_sleeve_id"] == brdg_account_relation.entity_id))
    if p_extra_brdg_acc_rel_cond is not None:
        l_join_cond = ((src_sleve_id_derv_C_df["join_sleeve_id"] == brdg_account_relation.entity_id))\
                      & (p_extra_brdg_acc_rel_cond)
    src_sleve_id_derv_C_df =src_sleve_id_derv_C_df.join(brdg_account_relation,l_join_cond,'left') \
        .select(src_sleve_id_derv_C_df['*'],brdg_account_relation.entity_id.alias('sleeve_id'))

    #Flow2:generate sleeve id from dim_sleeve
    src_sleeve_id_derv_P_df = src_df.filter(sf.col(p_col_filter)=='P')

    #setting sleeve_id as base_sleeve_id if the account_id is swp_account_id
    dim_fund_sleeve_df =read_from_s3_catalog_convert_to_df(JobContext.get_property(C_GLUE_S3_DB_NAME_PROP_KEY),JobContext.get_property(C_GLUE_S3_TABLE_DIM_FUND_SLEEVE_PROP_KEY))
    dim_fund_sleeve_df = transform_boolean_clm(dim_fund_sleeve_df,'active_flg')
    dim_fund_sleeve_df = dim_fund_sleeve_df.withColumn('row_num',sf.row_number().over(
        Window.partitionBy(dim_fund_sleeve_df['sor_sleeve_id'], dim_fund_sleeve_df['instrument_id']).orderBy(
            dim_fund_sleeve_df['created_ts'].desc()))).filter("row_num == 1").filter("active_flg").drop('row_num')
    dim_sleeve_df = read_from_s3_catalog_convert_to_df(JobContext.get_property(C_GLUE_S3_DB_NAME_PROP_KEY), \
                                                       JobContext.get_property(C_GLUE_S3_TABLE_DIM_SLEEVE_PROP_KEY))
    dim_sleeve_df = transform_boolean_clm(dim_sleeve_df,'active_flg')
    dim_sleeve_df = dim_sleeve_df.withColumn('row_num',sf.row_number().over(
        Window.partitionBy(dim_sleeve_df['sor_sleeve_id']).orderBy(dim_sleeve_df['created_ts'].desc()))).filter("row_num == 1").filter("active_flg").drop('row_num')

    dim_fund_sleeve_df=dim_fund_sleeve_df.select('sor_sleeve_id','instrument_id',sf.split(dim_fund_sleeve_df.sor_sleeve_id,'-').getItem(0).alias('dim_fund_account_id'))
    c_dim_fund_join_cond = (src_sleeve_id_derv_P_df.derv_account_id == dim_fund_sleeve_df.dim_fund_account_id) & \
                           (src_sleeve_id_derv_P_df.instrument_id == dim_fund_sleeve_df.instrument_id)
    src_sleeve_id_derv_P_df = src_sleeve_id_derv_P_df.join(dim_fund_sleeve_df,c_dim_fund_join_cond,'left').select(src_sleeve_id_derv_P_df['*'],dim_fund_sleeve_df.sor_sleeve_id)
    src_sleeve_id_derv_P_df = src_sleeve_id_derv_P_df.join(dim_sleeve_df,src_sleeve_id_derv_P_df.sor_sleeve_id==dim_sleeve_df.sor_sleeve_id,'left').select(src_sleeve_id_derv_P_df['*'],'sleeve_id')
    #union of both P and C indicators
    src_df = src_sleeve_id_derv_P_df.select(*g_src_clms).union(src_sleve_id_derv_C_df.select(*g_src_clms))
    #checking for sw parity account
    dim_fund_sleeve_df = dim_fund_sleeve_df.select('dim_fund_account_id').distinct()
    src_df = src_df.join(dim_fund_sleeve_df,src_df.derv_account_id==dim_fund_sleeve_df.dim_fund_account_id,'left').select(src_df['*'],dim_fund_sleeve_df.dim_fund_account_id)

    src_df.cache()
    slv_df = src_df.where(sf.col('sleeve_id').isNull() & (sf.col('slv_rltn_ind')=='P')) \
        .select("account_id",sf.concat_ws("-","derv_account_id",sf.lit(C_BASE_SLEEVE_SWP_ACCOUNT_VALUE)).alias("sor_sleeve_id")) \
        .distinct()
    slv_df.cache()

    if slv_df.first() is not None:
        insert_sleeve_stub(slv_df)

    #sleeve_id derivation
    src_df =src_df.withColumn('sleeve_id',sf.when(sf.col('sleeve_id').isNull(),
                                                  sf.when(sf.col('slv_rltn_ind')=='P'
                                                          ,sf.concat_ws("-","derv_account_id",sf.lit(C_BASE_SLEEVE_SWP_ACCOUNT_VALUE),
                                                                     sf.lit(C_DEFAULT_SLEEVE_ID_TRANSACTION_VALUE_1)))
                                                  .otherwise(sf.lit(C_DEFAULT_SLEEVE_ID_TRANSACTION_VALUE))).otherwise(sf.col('sleeve_id')))
    # default_end_dt_clm = sf.to_date(sf.lit(C_DEFAULT_END_DATE), C_DATE_FORMAT)
    # l_fad_df = read_from_s3_catalog_convert_to_df(JobContext.get_property(C_GLUE_S3_DB_NAME_PROP_KEY), \
    #                                               JobContext.get_property(C_GLUE_S3_TABLE_FACT_ACC_DATE_PROP_KEY),
    #                                               ['account_id','date_type_cd','date_type_dt','program_ind','active_flg','created_ts'])
    # l_fad_df = l_fad_df.filter(sf.col('date_type_cd').isin([ C_DATE_DTPE_CD_ACLDT, C_DATE_DTPE_CD_PRGDT]))
    # l_fad_df = transform_boolean_clm(l_fad_df,'active_flg')
    # l_fad_df = format_date(l_fad_df,'date_type_dt').withColumnRenamed('date_type_dt','start_dt')
    # l_fad_df = l_fad_df.withColumn("type_cd_priority",sf.when(sf.col("date_type_cd")== C_DATE_DTPE_CD_ACLDT,sf.lit(1)).otherwise(sf.lit(2)))
    # # First pick the latest active records
    # latest_Active_Rec_Window = Window.partitionBy('account_id','start_dt').orderBy(sf.col('created_ts').desc(),sf.col('type_cd_priority').asc())
    # l_fad_df = l_fad_df.withColumn('row_num',sf.row_number().over(latest_Active_Rec_Window)).filter(sf.col('row_num')==1)
    # l_fad_df = l_fad_df.drop('row_num').filter(sf.col('active_flg'))
    # # now derive end_dt parition by account_d and order by start_dt, then use lead  -1 to derive end_dt
    # derive_end_dt_Windo = Window.partitionBy('account_id').orderBy(sf.col('start_dt').asc())
    # l_fad_df = l_fad_df.withColumn('end_dt',sf.lead(sf.col('start_dt')).over(derive_end_dt_Windo))
    # l_fad_df = l_fad_df.withColumn('end_dt',sf.when(sf.col('end_dt').isNull(), default_end_dt_clm)
    #                                                  .otherwise(sf.date_add( sf.col('end_dt'),-1)))
    # src_df = src_df.withColumn('effective_dt',sf.to_date(sf.col('effective_dt'), 'yyyy-MM-dd'))
    # l_fad_df = l_fad_df.alias('fad')
    # src_df = src_df.alias('src')
    # # join it with fact_Account_dt
    # date_join_condition = sf.col('effective_dt').between(sf.col('fad.start_dt'),sf.col('fad.end_dt'))
    # l_fad_df = l_fad_df.drop("date_type_dt",'active_flg','created_ts')
    # src_df = src_df.join(sf.broadcast(l_fad_df),(sf.col('src.account_id')==sf.col('fad.account_id')) & date_join_condition,"left_outer") \
    #     .select(src_df['*'],l_fad_df.account_id.alias('fact_account_id'),sf.col('fad.program_ind').alias('fact_program_ind') \
    #             ,sf.col('fad.date_type_cd').alias('date_type_cd'))
    #
    # src_df = src_df.withColumn("sleeve_id",sf.when((sf.col("date_type_cd")==C_DATE_DTPE_CD_PRGDT) & (sf.col('fact_program_ind')==C_FAD_PROGRAM_CD_MAM),
    #                                                sf.col("sleeve_id")).otherwise(sf.lit(C_DEFAULT_SLEEVE_ID_TRANSACTION_VALUE)))
    src_df =src_df.select(*g_src_clms).union(src_default_sleeve_id_df.select(*g_src_clms))

    # Exclude account check
    JobContext.logger.info("Exclude account check - starts")
    src_df = src_df.withColumn("acc_cd_for_exclude_chk",sf.split(sf.col("account_id"),"-").getItem(0))
    src_df = lookup_mtdt_exclude_account(src_df,"acc_cd_for_exclude_chk")
    src_df = src_df.withColumn("account_id",sf.when((sf.col("start_sor_account_id").isNotNull())
                                                    & (sf.col("start_sor_account_id")!=''),sf.lit(None))
                               .otherwise(sf.col("account_id")))
    JobContext.logger.info("Exclude account check - ends")

    src_df = src_df.drop("acc_cd_for_exclude_chk","start_sor_account_id")
    return src_df


def derive_related_entity_id_from_brd_instrument(src_security_cd_df, brd_instrument_rl_dtl=None, join_cond = None,
                                                 relation_type_cd=[C_BRDG_INST_REL_RELATION_TYPE_CD_P_BEN
                                                     ,C_BRDG_INST_REL_RELATION_TYPE_CD_PP_BEN,C_BRDG_INST_REL_RELATION_TYPE_CD_PP_BEN_M], entity_type_cd=C_BRDG_INST_REL_ENTITY_TYPE_CD_BENCHMARK, parition_clms=['instrument_id',"start_dt"]):

    JobContext.logger.info("Derive related entity_id from brd_instrument")

    if brd_instrument_rl_dtl is None:
        brd_instrument_rl_dtl = pick_latest_from_brd_instrument(relation_type_cd, entity_type_cd, parition_clms)



    if join_cond:
        brd_ins_join_cond = join_cond
    else:
        brd_ins_join_cond = [sf.col("src.instrument_id") == sf.col("brd.instrument_id"),sf.col("src.src_start_dt").between( sf.col("brd.start_dt"),sf.col("brd.end_dt"))]
    brd_instrument_rl_dtl = brd_instrument_rl_dtl.alias("brd")
    src_security_cd_df = src_security_cd_df.alias("src").join(sf.broadcast(brd_instrument_rl_dtl), brd_ins_join_cond, "left").select(src_security_cd_df['*'],
                                                                                                                       brd_instrument_rl_dtl.entity_id.alias("related_entity_id"))
    JobContext.logger.info("related entity_id is derived from brd_instrument")
    return src_security_cd_df

def pick_latest_from_brd_instrument(relation_type_cd, entity_type_cd, parition_clms, clm_to_drop=["active_flg","row_num",'created_ts',"ENTITY_TYPE_CD", "RELATION_TYPE_CD"]):
    JobContext.logger.info("Selecting latest records from brdg_instrumenr_relation table, "
                           "filtering dat on relation_type_cd={}, entity_type_cd={}, parition_clm={}"
                           .format(relation_type_cd,entity_type_cd, parition_clms))

    brd_instrument_rl_dtl = read_from_s3_catalog_convert_to_df(JobContext.get_property(C_GLUE_S3_DB_NAME_PROP_KEY),
                                                               JobContext.get_property(C_GLUE_S3_TABLE_BRDG_INSTRU_REL_PROP_KEY)) \
        .select("instrument_id", "ENTITY_TYPE_CD", "RELATION_TYPE_CD", "active_flg", "start_dt", "end_dt", "entity_id",'created_ts')


    brd_instrument_rl_dtl = transform_boolean_clm(brd_instrument_rl_dtl, "active_flg")
    brd_instrument_rl_dtl = brd_instrument_rl_dtl.filter(sf.col("ENTITY_TYPE_CD") == entity_type_cd) \
        .filter(sf.col("RELATION_TYPE_CD").isin(relation_type_cd) )

    brd_instrument_rl_dtl = format_date(brd_instrument_rl_dtl, "start_dt")
    brd_instrument_rl_dtl = format_date(brd_instrument_rl_dtl, "end_dt")
    obj_parition_clms = [sf.col(clm) for clm in parition_clms]
    window = Window.partitionBy(obj_parition_clms).orderBy(sf.col('created_ts').desc())
    brd_instrument_rl_dtl = brd_instrument_rl_dtl.withColumn('row_num', sf.row_number().over(window)) \
        .                               filter(sf.col('row_num') == 1).filter("active_flg").drop(*clm_to_drop)

    return brd_instrument_rl_dtl


def derive_related_entity_id_from_brd_entity(src_entity_df, brd_entity_rl_dtl=None, join_cond = None,
                                             relation_type_cd=C_BRDG_ENTITY_REL_RELATION_TYPE_CD_P_BEN,
                                             related_entity_type_cd=C_BRDG_ENTITY_REL_ENTITY_TYPE_CD_BENCHMARK,
                                             entity_type_cd=C_BRDG_ENTITY_REL_ENTITY_TYPE_CD_FOA, parition_clms=['entity_id',"start_dt"]):
    JobContext.logger.info("Derive related entity_id from brd_instrument")

    if brd_entity_rl_dtl is None:
        brd_entity_rl_dtl = pick_latest_from_brd_entity(related_entity_type_cd,relation_type_cd, entity_type_cd, parition_clms)



    if join_cond:
        brd_ins_join_cond = join_cond
    else:
        brd_ins_join_cond = [sf.col("src.foa_cd") == sf.col("brd.entity_id"), sf.col("src.src_start_dt").between( sf.col("brd.start_dt"),sf.col("brd.end_dt"))]
    brd_entity_rl_dtl = brd_entity_rl_dtl.alias("brd")
    src_entity_df = src_entity_df.alias("src").join(sf.broadcast(brd_entity_rl_dtl), brd_ins_join_cond, "left").select(src_entity_df['*'],
                                                                                                         brd_entity_rl_dtl.related_entity_id)

    JobContext.logger.info("related entity_id is derived from brd_instrument")
    return src_entity_df
#
def pick_latest_from_brd_entity( related_entity_type_cd, relation_type_cd,entity_type_cd, parition_clms):
    JobContext.logger.info("Selecting latest records from BRDG_ENTITY_RELATION table, "
                           "filtering dat on related_entity_type_cd={}, relation_type_cd={}, parition_clm={}"
                           .format(related_entity_type_cd, relation_type_cd, parition_clms))

    brd_entity_rl_dtl = read_from_s3_catalog_convert_to_df(JobContext.get_property(C_GLUE_S3_DB_NAME_PROP_KEY),
                                                           JobContext.get_property(C_GLUE_S3_TABLE_BRDG_ENTITY_RELATION_PROP_KEY))\
                                .select("active_flg","related_entity_type_cd","entity_type_cd","relation_type_cd","start_dt","end_dt","created_ts","entity_id","related_entity_id")


    brd_entity_rl_dtl = transform_boolean_clm(brd_entity_rl_dtl, "active_flg")
    brd_entity_rl_dtl = brd_entity_rl_dtl.filter(sf.col("related_entity_type_cd") == related_entity_type_cd) \
        .filter(sf.col("relation_type_cd") == relation_type_cd).filter(sf.col("entity_type_cd") == entity_type_cd)

    brd_entity_rl_dtl = format_date(brd_entity_rl_dtl,"start_dt")
    brd_entity_rl_dtl = format_date(brd_entity_rl_dtl,"end_dt")

    window = Window. \
        partitionBy(parition_clms). \
        orderBy(sf.col('created_ts').desc())

    brd_entity_rl_dtl = brd_entity_rl_dtl.withColumn('row_num', sf.row_number().over(window)).filter(sf.col('row_num') == 1).filter("active_flg")\
                                        .drop("row_num","active_flg","related_entity_type_cd","entity_type_cd","relation_type_cd","created_ts")
    JobContext.logger.info("Selected latest records from BRDG_ENTITY_RELATION table ")
    return brd_entity_rl_dtl

def pick_latest_from_brd_acc(relation_type_cd, entity_type_cd, parition_clms):
    JobContext.logger.info("Selecting latest records from brdg_account_relation table, "
                           "filtering dat on relation_type_cd={}, entity_type_cd={}, parition_clm={}"
                           .format(relation_type_cd,entity_type_cd, parition_clms))

    brd_acc_rl_dtl = read_from_s3_catalog_convert_to_df(JobContext.get_property(C_GLUE_S3_DB_NAME_PROP_KEY),
                                                        JobContext.get_property(C_GLUE_S3_TABLE_BRDG_ACC_RELATION_PROP_KEY))\
                                .select("active_flg","relation_type_cd","entity_type_cd","start_dt","end_dt","created_ts","entity_id","account_id")
    brd_acc_rl_dtl = transform_boolean_clm(brd_acc_rl_dtl, "active_flg")
    brd_acc_rl_dtl = brd_acc_rl_dtl.filter(sf.col("relation_type_cd") ==relation_type_cd) \
        .filter(sf.col("entity_type_cd") == entity_type_cd)

    brd_acc_rl_dtl = transform_boolean_clm(brd_acc_rl_dtl, "active_flg")
    brd_acc_rl_dtl = format_date(brd_acc_rl_dtl, "start_dt")
    brd_acc_rl_dtl = format_date(brd_acc_rl_dtl, "end_dt")
    obj_partion_clms = [sf.col(clm) for clm in parition_clms]
    window = Window.partitionBy(obj_partion_clms).orderBy(sf.col("start_dt").desc(), sf.col('created_ts').desc())
    brd_acc_rl_dtl = brd_acc_rl_dtl.withColumn('row_num', sf.row_number().over(window)) \
        .filter(sf.col('row_num') == 1).filter("active_flg").drop("active_flg","relation_type_cd","entity_type_cd","created_ts","row_num")

    JobContext.logger.info("Selected latest records from brdg_account_relation table ")
    return brd_acc_rl_dtl

def pick_latest_records_from_dim_foa(dim_foa_df):
    window = Window.partitionBy(sf.col("foa_cd")).orderBy(sf.col('created_ts').desc())
    dim_foa_df = dim_foa_df.withColumn("row_num", sf.row_number().over(window)).filter(sf.col("row_num") == 1)
    return dim_foa_df

C_BRDG_ACC_RELATION_COL_LIST = ['account_id','entity_type_cd','entity_id','start_dt','end_dt','relation_type_cd',
                                'target_alloc_prcnt','manual_override_flg','inc_reason_cd','inc_notes','exc_reason_cd',
                                'exc_notes','firm_id','active_flg','batch_dt','created_program_nm','created_user_nm',
                                'created_ts','modified_program_nm','modified_user_nm','modified_ts']
C_BRDG_ACC_RELATION_DEFAULT_COL_LIST = {"entity_type_cd": C_BRDG_ACC_REL_ENTITY_TYPE_CD_SLEEVE,"end_dt": C_DEFAULT_END_DATE,
                                        "relation_type_cd": None,"target_alloc_prcnt": None,"manual_override_flg": None,
                                        "inc_reason_cd": None,"inc_notes": None,"exc_reason_cd": None,"exc_notes": None,
                                        "firm_id": C_FIRM_ID}

C_DIM_SLEEVE_COL_LIST = ['sleeve_id', 'sor_sleeve_id', 'sleeve_gf_id', 'sleeve_nm', 'sleeve_desc', 'sleeve_type_nm',
                         'primary_model_id', 'money_manager_id', 'additional_models_id','currency_cd',
                         'firm_id', 'active_flg', 'batch_dt', 'created_program_nm', 'created_ts', 'created_user_nm',
                         'modified_program_nm', 'modified_user_nm', 'modified_ts']
C_DIM_SLEEVE_DEFAULT_COL_LIST = {"sleeve_desc": None,"sleeve_type_nm":None,"primary_model_id": None,"money_manager_id": None,
                                 "additional_models_id": None,"firm_id": C_FIRM_ID}
C_DIM_SLEEVE_FIELD_TO_VALIDATE_AGAINST = {"sleeve_id": NotNullValidator(),"sor_sleeve_id": NotNullValidator(),
                                          "sleeve_nm": NotNullValidator(),"sleeve_gf_id": NotNullValidator()}

def insert_sleeve_stub(p_df):
    JobContext.logger.info("Record Insertion process for dim_sleeve for default sleeve table - Starts")
    l_batch_date_clm = sf.to_date(sf.lit(JobContext.get_property(C_BATCH_DATE_PROP_KEY)))
    l_slv_df = read_from_catalog_and_filter_inactive_rec(JobContext.get_property(C_GLUE_S3_TABLE_DIM_SLEEVE_PROP_KEY),
                                                         ["sleeve_id","sor_sleeve_id","active_flg"])
    p_df = p_df.join(sf.broadcast(l_slv_df),p_df["sor_sleeve_id"]==l_slv_df["sor_sleeve_id"],"left_anti") \
        .select(p_df["*"],sf.concat_ws("-","sor_sleeve_id",sf.lit(C_FIRM_ID)).alias("sleeve_id"),p_df["sor_sleeve_id"].alias("sleeve_nm"))
    l_acc_df = p_df.select(sf.col("sleeve_id").alias("entity_id"),"account_id")
    base_currency = derive_currency_cd()
    # Adding default columns
    C_DIM_SLEEVE_DEFAULT_COL_LIST["currency_cd"] = base_currency
    p_df = add_default_clm(p_df, C_DIM_SLEEVE_DEFAULT_COL_LIST)
    # Adding audit columns
    p_df = add_audit_columns(p_df, l_batch_date_clm, add_modification_clm=True)
    # creating df to be loaded in mtdt sleeve gen
    g_dim_slv_df_for_save = p_df.select('sleeve_id','created_user_nm','created_ts').withColumn("active_flg", sf.lit(True))
    #Insert data in to mtdt instrument id gen table
    previous_file_type = JobContext.file_being_processed
    JobContext.file_being_processed = "base_sleeve_trans" 
    mtdt_slv_df = mtdt_sleeve_id_gen(None,g_dim_slv_df_for_save)
    p_df = p_df.join(sf.broadcast(mtdt_slv_df), p_df['sleeve_id'] == mtdt_slv_df['sleeve_id'],'left_outer') \
        .select(p_df['*'], mtdt_slv_df['sleeve_gf_id'])
    # Validating records
    p_df = validate_rec_and_filter(p_df,C_DIM_SLEEVE_FIELD_TO_VALIDATE_AGAINST,
                                   JobContext.get_property(C_S3_DIM_SLEEVE_EXCP_OUTPUT_LOCATION_PROP_KEY),
                                   prepare_exception_clm_list(C_DIM_SLEEVE_COL_LIST))
    # Inserting on S3 Dim sleeve
    p_df = p_df.select(*C_DIM_SLEEVE_COL_LIST)
    save_output_to_s3(p_df, "dim_sleeve", JobContext.get_property(C_S3_DIM_SLEEVE_OUTPUT_LOCATION_PROP_KEY))
    insert_brdg_account_relation_records(l_acc_df)
    JobContext.file_being_processed = previous_file_type
    JobContext.logger.info("Record Insertion process for dim_sleeve for default sleeve table - End")

def insert_brdg_account_relation_records(p_df):
    JobContext.logger.info("Record Insertion process for brdg_account_relation table - Starts")
    l_batch_dt = JobContext.get_property(C_BATCH_DATE_PROP_KEY)
    l_batch_dt_clm = sf.to_date(sf.lit(l_batch_dt))

    # Adding Default column
    C_BRDG_ACC_RELATION_DEFAULT_COL_LIST["start_dt"] = l_batch_dt
    p_df = add_default_clm(p_df, C_BRDG_ACC_RELATION_DEFAULT_COL_LIST)
    # Adding audit column
    p_df = add_audit_columns(p_df,l_batch_dt_clm,add_modification_clm=True)
    # Saving output to s3
    p_df = p_df.select(*C_BRDG_ACC_RELATION_COL_LIST)
    save_output_to_s3(p_df,"brdg_account_relation",JobContext.get_property(C_S3_BRDG_ACC_RELATION_OUTPUT_LOCATION_PROP_KEY))
    JobContext.logger.info("Record Insertion process for brdg_account_relation table - Ends")
    #-------------------FACT_ACCOUNT_DATE Load -------------------#
         
    src_for_fad = p_df.select("account_id","entity_id").distinct()
    #Read FACT_ACCOUNT_DATE table
    C_FAD_COL_LIST = ["account_id","entity_type_cd","entity_id","date_type_cd","date_type_dt","perf_cont_flg","foa_cd","reason_cd","firm_id","program_ind","program_num_id","active_flg","batch_dt",
                      "created_program_nm","created_user_nm","created_ts"]
    C_REQ_COLS = ["account_id","entity_type_cd","entity_id","date_type_cd","active_flg","created_ts"]
    fact_account_date_df = read_from_catalog_and_filter_inactive_rec(JobContext.get_property(C_GLUE_S3_TABLE_FACT_ACC_DATE_PROP_KEY),
                                                                       ["account_id","date_type_cd","date_type_dt",
                                                                        "program_ind","entity_type_cd","entity_id",
                                                                        "active_flg","created_ts"])
    fact_account_date_df = fact_account_date_df.filter((fact_account_date_df.entity_type_cd=='SLEEVE') & (fact_account_date_df.date_type_cd.isin('AOPDT','FOADT','PRGDT')))
    fact_account_date_df = transform_boolean_clm(fact_account_date_df, "active_flg")
    g_window = Window.partitionBy(fact_account_date_df.account_id,fact_account_date_df.entity_id,).orderBy(fact_account_date_df.created_ts.desc())
    fact_account_date_df = fact_account_date_df.withColumn("latest_rec", sf.row_number().over(g_window) == 1).filter("latest_rec").filter("active_flg")

    fad_new_acc_df = src_for_fad.join(fact_account_date_df, ['account_id','entity_id'], how='left_anti')
        
    aopdt_df = fad_new_acc_df.select('account_id','entity_id').withColumn('date_type_cd',sf.lit('AOPDT'))
    foadt_df = fad_new_acc_df.select('account_id','entity_id').withColumn('date_type_cd',sf.lit('FOADT'))
    prgdt_df = fad_new_acc_df.select('account_id','entity_id').withColumn('date_type_cd',sf.lit('PRGDT'))
    C_COLS = ['account_id','entity_id','date_type_cd']
    out_fad_df = aopdt_df.select(C_COLS).union(foadt_df.select(C_COLS)).union(prgdt_df.select(C_COLS))
    out_fad_df = out_fad_df.withColumn('entity_type_cd',sf.lit('SLEEVE'))\
                                .withColumn('date_type_dt',sf.lit(JobContext.batch_date)) \
                                .withColumn('perf_cont_flg',sf.lit('')) \
                                .withColumn('foa_cd',sf.lit('')) \
                                .withColumn('reason_cd',sf.lit('')) \
                                .withColumn('firm_id',sf.lit(C_FIRM_ID)) \
                                .withColumn('program_ind',sf.lit(C_DIM_PRG_PROGRAM_CD_MAM)) \
                                .withColumn('program_num_id',sf.lit(''))
        
    out_fad_df = add_audit_columns(out_fad_df,l_batch_dt_clm,add_modification_clm=False)
    save_output_to_s3(out_fad_df.select(*C_FAD_COL_LIST), 'fact_account_date', JobContext.get_property(C_S3_FACT_ACC_DATE_OUTPUT_LOCATION_PROP_KEY))
        

##########################################################
#  Common util to generate new sor_benchmark_id
##########################################################
def mtdt_acc_id_gen( src_df, sor_acc_lcm='sor_account_id', derived_instru_id_clm="sor_benchmark_id", created_user_nm = C_CREATED_USER_NM, existing_clm_ind="existing"):

    JobContext.logger.info("Generating MTDT_ACC_BEN_ID records")

    #Read mtdt instrument id gen table and get max instrument_id
    mtd_acc_id_ref_gen_df = read_from_s3_catalog_convert_to_df(JobContext.get_property(C_GLUE_S3_DB_NAME_PROP_KEY),JobContext.get_property(C_GLUE_S3_TABLE_MTDT_ACC_BEN_ID_GEN))
    mtd_acc_id_ref_gen_df = mtd_acc_id_ref_gen_df.select(sf.col('sor_benchmark_id').cast('bigint'),"related_sor_ref")
    mtdt_acc_id_df = mtd_acc_id_ref_gen_df.agg({"sor_benchmark_id":"max"}).withColumnRenamed("max(sor_benchmark_id)","sor_benchmark_id").collect()
    max_instr_id = mtdt_acc_id_df[0].sor_benchmark_id

    #Check for null in mtdt_sor_benchmark_id
    offset=max_instr_id if max_instr_id != None else C_MTDT_ACC_BEN_ID_GEN_START_POS


    src_df_with_mtdt_id = src_df.alias("src").join(sf.broadcast(mtd_acc_id_ref_gen_df).alias("mtdt"), sf.col(f"src.{sor_acc_lcm}") == sf.col("mtdt.related_sor_ref"), how='left_outer') \
        .select(src_df['*'], sf.col("mtdt.sor_benchmark_id").alias(derived_instru_id_clm))
    src_df_mtdt_null = src_df_with_mtdt_id.filter(sf.col(derived_instru_id_clm).isNull()).withColumn(existing_clm_ind, sf.lit(False)).drop(derived_instru_id_clm)

    # we will only join newly created records
    src_df_mtdt_not_null = src_df_with_mtdt_id.filter(sf.col(derived_instru_id_clm).isNotNull()).withColumn(existing_clm_ind, sf.lit(True))

    ref_not_exist_in_mtdt_df = src_df_mtdt_null.select(sf.col(sor_acc_lcm).alias('related_sor_ref')).distinct()

    #Define Schema for dataframe
    df_schema = ref_not_exist_in_mtdt_df.withColumn("sor_benchmark_id", sf.lit(1).cast('bigint'))
    #Generate id
    zipped_rdd = ref_not_exist_in_mtdt_df.rdd.zipWithIndex().map(lambda ri: ri[0]+(ri[1]+offset+1,))
    write_to_mtdt_df = JobContext.glue_context.spark_session.createDataFrame(zipped_rdd, schema=df_schema.schema)

    write_to_mtdt_df = write_to_mtdt_df.withColumn('active_flg',sf.lit(C_BOOLEAN_DEFAULT_VALUE)) \
        .withColumn('created_user_nm',sf.lit(created_user_nm)) \
        .withColumn('created_ts', sf.lit(sf.current_timestamp()))\
        .withColumn('created_ts', sf.from_utc_timestamp(sf.col('created_ts'), "EST"))

    write_to_mtdt_df =write_to_mtdt_df.select("sor_benchmark_id","related_sor_ref","active_flg","created_user_nm","created_ts")
    write_to_mtdt_df.cache()
    save_output_to_s3(write_to_mtdt_df,"write_to_mtdt_df",JobContext.get_property(C_S3_MTDT_ACC_BEN_ID_GEN_OUTPUT_LOCATION_PROP_KEY))

    out_df = src_df_mtdt_null.alias("src").join(sf.broadcast(write_to_mtdt_df.alias("mtdt")), sf.col(f"src.{sor_acc_lcm}") == sf.col("mtdt.related_sor_ref"), how='left_outer') \
        .select(src_df_mtdt_null['*'], sf.col("mtdt.sor_benchmark_id").alias(derived_instru_id_clm))

    clms = out_df.columns
    out_df = out_df.select(*clms).union(src_df_mtdt_not_null.select(*clms))
    JobContext.logger.info("MTDT_ACC_BEN_ID record generated")
    return out_df



# Dim instrument related constants

def create_dim_instrument_stub(self, l_full_src_instrument_df,g_source_sys_id, g_base_currency):

    C_DIM_INSTRUMENT_DEFAULT_COL_LIST = {'issue_dt': None, 'start_dt': C_DEFAULT_START_DATE,
                                     'end_dt': C_DEFAULT_END_DATE, 'first_accrual_dt': None, 'first_coupon_dt': None,
                                     'instrument_desc': None, 'instrument_name_1': None, 'instrument_name_2': None,
                                     'instrument_type_cd': "SEC", 'constituent_benchmark_id': None,
                                     'constituent_benchmark_start_dt': None, 'constituent_benchmark_end_dt': None,
                                     'constituent_benchmark_prct': None, 'qty_factor_val': None, 'interest_rt': None,
                                     'coupon_freq_cd': None, 'pending_payment_days': None, 'opt_strike_price_amt': None,
                                     'underlying_instrument_id': None, 'year_count_cd': None, 'day_count_cd': None,
                                     'interest_rate_type_cd': None, 'firm_id': C_FIRM_ID, 'MODIFIED_PROGRAM_NM': None,
                                     'MODIFIED_USER_NM': None, 'modified_ts': None,"MATURITY_DT":None,"pre_refund_dt":None,
                                         "OPT_CALL_PUT_FLG":None}
    C_DIM_FIELD_TO_VALIDATE_AGAINST = {"instrument_id": C_NOT_NULL_WITH_EMPTY_VALIDATOR_LIST}
    new_dim_instrument_df = l_full_src_instrument_df.filter("is_new_rec")
    C_DIM_INSTRUMENT_DEFAULT_COL_LIST["source_system_id"] = g_source_sys_id
    C_DIM_INSTRUMENT_DEFAULT_COL_LIST["currency_cd"] = g_base_currency
    new_dim_instrument_df = add_default_clm(new_dim_instrument_df, C_DIM_INSTRUMENT_DEFAULT_COL_LIST)
    new_dim_instrument_df = add_audit_columns(new_dim_instrument_df, JobContext.batch_date_clm)

    new_dim_instrument_df = new_dim_instrument_df.select(*C_DIM_INSTRUMENT_COLUMN_LIST)
    new_dim_instrument_df = validate_rec_and_filter(new_dim_instrument_df, C_DIM_FIELD_TO_VALIDATE_AGAINST,
                                                    JobContext.get_property(C_S3_DIM_INSTRUMENT_EXCP_OUTPUT_LOC_PROP_KEY),
                                                    prepare_exception_clm_list(C_DIM_INSTRUMENT_COLUMN_LIST))
    new_dim_instrument_df = new_dim_instrument_df.select(*C_DIM_INSTRUMENT_COLUMN_LIST)
    save_output_to_s3(new_dim_instrument_df, "inst_src_df", JobContext.get_property(C_S3_DIM_INSTRUMENT_OUTPUT_LOC_PROP_KEY), repartition_count=None)

def create_dtl_instrument_ref_stub(job_obj, p_src_df, ref_type_to_clm_name, src_systm_id, p_dtl_inst_lkp_df=None,
                                   parent_ref_type=C_INSTRUMENT_REF_TYPE_CD_OT, implement_one_to_one_ref_map=True):
    C_DTL_FIELD_TO_VALIDATE_AGAINST = {"instrument_id": C_NOT_NULL_WITH_EMPTY_VALIDATOR_LIST, "instrument_ref_nm":C_NOT_NULL_WITH_EMPTY_VALIDATOR_LIST}

    C_DTL_INSTRUMENT_REF_DEFAULT_COL_LIST = {"firm_id": C_FIRM_ID,"modified_program": None, "modified_user":None,
                                             "modified_ts":None}

    JobContext.logger.info("Creating dtl_instrument_ref stub, for record with various security ref_type")
    job_obj.g_base_currency = derive_currency_cd()
    all_ref_types = []
    all_clm_names=[]
    # We will take OrderedDict, dynamically create stack expression.
    # Un-pivoting all security ref names
    count = len(ref_type_to_clm_name)
    all_ref = ""
    index = 0
    priorty_index = 1
    condition_statement = None
    for k,v in ref_type_to_clm_name.items():
        all_ref += f" '{k}', {v}"
        if index<count-1:
            all_ref = all_ref+","
            index= index+1

        condition_statement = condition_statement.when(sf.col("instrument_ref_type_cd") == k, priorty_index) \
            if condition_statement is not None  else sf.when(sf.col("instrument_ref_type_cd") == k, priorty_index)
        priorty_index= priorty_index+1
        all_ref_types.append(k)
        all_clm_names.append(v)

    adp_sec= ref_type_to_clm_name.get(parent_ref_type)
    l_stack_exp =f"stack({count},{all_ref} ) as (instrument_ref_type_cd,instrument_ref_nm)"
    JobContext.logger.info(f"Prepared stack exp: {l_stack_exp}")
    JobContext.logger.info(f"Prepared instrument sequence is: {condition_statement}")
    JobContext.logger.info(f"Passed reference types are: {all_ref_types}")

    if p_dtl_inst_lkp_df is None:
        JobContext.logger.info("Dtl_instrument is not passed so reading it")
        p_dtl_inst_lkp_df = read_from_s3_catalog_convert_to_df(job_obj.C_DB_NAME, job_obj._get_property(C_GLUE_S3_TABLE_DTL_INSTRUMENT_REF_PROP_KEY),
                                                               ["instrument_id","start_dt","end_dt","instrument_ref_type_cd",
                                                                "instrument_ref_nm","active_flg","created_ts"])
        p_dtl_inst_lkp_df = transform_boolean_clm(p_dtl_inst_lkp_df, "active_flg")
        p_dtl_inst_lkp_df = p_dtl_inst_lkp_df.where(sf.col("instrument_ref_type_cd").isin(*all_ref_types))

    # filtering lookup to get latest record based on ref nm and ref type code
    g_window = Window.partitionBy(p_dtl_inst_lkp_df.instrument_ref_nm,
                                  p_dtl_inst_lkp_df.instrument_ref_type_cd).orderBy(p_dtl_inst_lkp_df.end_dt.desc(),
                                                                                    p_dtl_inst_lkp_df.created_ts.desc())
    p_dtl_inst_lkp_df = p_dtl_inst_lkp_df.withColumn("num",sf.row_number().over(g_window))
    p_dtl_inst_lkp_df = p_dtl_inst_lkp_df.where((job_obj.g_batch_date_clm.between(sf.col("start_dt"), sf.col("end_dt")))
                                                & (sf.col("num")==1))
    p_dtl_inst_lkp_df = p_dtl_inst_lkp_df.drop("num")
    p_dtl_inst_lkp_df.cache()
    JobContext.logger.info("Prepared Column list" + str(all_clm_names))
    JobContext.logger.info(f"Master security column {adp_sec}")
    l_inst_src_df = p_src_df.select(*all_clm_names)
    """"
    Below step will pick security reference based on priorities, means sequence of columns that's why we need OrderedDict for ref_type_to_clm_name.
    
    """
    l_inst_src_df = l_inst_src_df.withColumn("sec_ref_nm", get_condition_for_non_empty_value(all_clm_names)).distinct()
    l_inst_src_df = l_inst_src_df.withColumn("rec_num",sf.row_number().over(Window.orderBy(adp_sec)))
    l_inst_src_df.cache()

    l_org_inst_src_df = l_inst_src_df

    """
     Logic:
        sample exmaple:
        ADP00121|cusip1|symbol1|||OTC1|DE1|SC1|44|2000.1234|423345.1448|A|N|US|2018-05-01|2018-05-01|AZ|2018-05-03|102||2018-05-03|m|E||||
        ADP00125|cusip05|symbo51|||OTC5|DE5|SC5|44|2000.1234|423345.1448|A|N|US|2018-05-01|2018-05-01|AZ|2018-05-03|102||2018-05-03|m|E||||
        
        1 - We will put number to all records, As we will pivot the data, to join back we will need track of record number.
        
        
        ADP00121|cusip1|symbol1|||OTC1|DE1|SC1|44|2000.1234|423345.1448|A|N|US|2018-05-01|2018-05-01|AZ|2018-05-03|102||2018-05-03|m|E||||1
        ADP00125|cusip05|symbo51|||OTC5|DE5|SC5|44|2000.1234|423345.1448|A|N|US|2018-05-01|2018-05-01|AZ|2018-05-03|102||2018-05-03|m|E||||2
        
        2 - Now we will pivot the record 
        instrument_ref_nm|instrument_ref_type
        ADP00121|OT.....
        cusip1|CU.....
        DE1|SC1|44|DESC.....
        ADP00125|OT"""
    l_inst_src_df = l_inst_src_df.selectExpr("rec_num", "sec_ref_nm",adp_sec, l_stack_exp)

    number_of_partition = l_inst_src_df.rdd.getNumPartitions()
    #TODO: value 4 in parition, We may have to take is as parameter or we can try with  len(ref_type_to_clm_name),
    # for correct number we need 2-3 real time files
    l_inst_src_df = l_inst_src_df.repartition(number_of_partition*4,"instrument_ref_nm","instrument_ref_type_cd")
    job_obj.logger.info(f"Instrument source - Number of partition from {number_of_partition * 4} to {number_of_partition}")


    l_inst_src_df = l_inst_src_df.where((sf.col("instrument_ref_nm").isNotNull()) & (sf.col("instrument_ref_nm")!="")).distinct()

    #Joining source with lookup to find existing instruments

    l_inst_df = p_dtl_inst_lkp_df
    l_inst_src_df = l_inst_src_df.join(l_inst_df,(l_inst_src_df["instrument_ref_nm"]== l_inst_df["instrument_ref_nm"])
                                       & (l_inst_src_df["instrument_ref_type_cd"]== l_inst_df["instrument_ref_type_cd"]),"left") \
        .select(l_inst_src_df["*"],l_inst_df["instrument_id"],l_inst_df["end_dt"].alias("end_dt_lkp"),l_inst_df["start_dt"].alias("start_dt_lkp")
                ,l_inst_df["instrument_id"].alias("original_instrument_id"))

    #TODO: This can be drived using ref_type_to_clm_name
    l_inst_src_df = l_inst_src_df.withColumn("ref_type_order",condition_statement)
    l_win = Window.partitionBy("rec_num").orderBy("ref_type_order")
    l_inst_src_df = l_inst_src_df.withColumn("selected_inst_id",sf.collect_list("instrument_id").over(l_win).getItem(0)) \
        .withColumn("instrument_id",sf.when(sf.col("instrument_id").isNotNull(),sf.col("selected_inst_id"))).drop("selected_inst_id")



    # populating instrument id to all ref names for existing securities.
    # For example: If adp is available in lookup and Cusip is newly coming for same security, then populating
    # instrument_id to the cusip record.
    l_inst_src_df.cache()

    l_inst_id_df = l_inst_src_df.where(sf.col("instrument_id").isNotNull() & (~sf.col("instrument_ref_type_cd").
                                                                              isin(C_INSTRUMENT_REF_TYPE_CD_PRO_I,
                                                                                   C_INSTRUMENT_REF_TYPE_CD_DESC,
                                                                                   C_INSTRUMENT_REF_TYPE_CD_PW_SEC))) \
        .select(sf.col("instrument_id").alias("lkp_instrument_id"),sf.col("rec_num").alias("lkp_rec_num")).distinct()
    l_inst_src_df = l_inst_src_df.join(sf.broadcast(l_inst_id_df),l_inst_src_df["rec_num"]==l_inst_id_df["lkp_rec_num"],"left") \
        .select(l_inst_src_df["*"],l_inst_id_df["lkp_instrument_id"])
    l_inst_src_df = l_inst_src_df.drop("instrument_id").withColumnRenamed("lkp_instrument_id","instrument_id")

    # new instrument_id generation
    l_new_inst_id_df = l_inst_src_df.where(sf.col("instrument_id").isNull()).select("rec_num").distinct()
    get_instrument_id_df = l_org_inst_src_df.join(sf.broadcast(l_new_inst_id_df), l_new_inst_id_df["rec_num"]==l_org_inst_src_df["rec_num"]) \
        .select(sf.col("sec_ref_nm").alias("instrument_ref_nm")).distinct()

    # Insert data in to mtdt instrument id gen table
    get_instrument_id_df = mtdt_instrument_id_gen(job_obj, get_instrument_id_df, created_user_nm=job_obj._get_file_type())
    get_instrument_id_df = get_instrument_id_df.join(l_org_inst_src_df,get_instrument_id_df["instrument_ref_nm"]==l_org_inst_src_df["sec_ref_nm"]) \
        .select(get_instrument_id_df["*"],l_org_inst_src_df["rec_num"],l_org_inst_src_df[adp_sec])
    get_instrument_id_df = get_instrument_id_df.toDF(*["instrument_ref_nm", "instrument_id", "rec_num", adp_sec])
    get_instrument_id_df.cache()

    l_inst_src_df = l_inst_src_df.alias("main").join(get_instrument_id_df.alias("new"),sf.col("main.rec_num")==sf.col("new.rec_num"),"left") \
        .select(l_inst_src_df["rec_num"],l_inst_src_df["instrument_ref_type_cd"],l_inst_src_df["end_dt_lkp"],l_inst_src_df["instrument_ref_nm"],
                sf.coalesce(l_inst_src_df["instrument_id"],get_instrument_id_df["instrument_id"]).alias("instrument_id"),
                get_instrument_id_df["instrument_id"].isNotNull().alias("is_new_rec"), "sec_ref_nm",
                l_inst_src_df["start_dt_lkp"], l_inst_src_df["original_instrument_id"],l_inst_src_df[adp_sec])
    l_inst_src_df.cache()
    l_full_src_instrument_df = l_inst_src_df.select("instrument_id", sf.col("sec_ref_nm").alias("instrument_ref_nm"),
                                                    "is_new_rec",adp_sec).distinct()

    g_window = Window.partitionBy(p_dtl_inst_lkp_df.instrument_id,
                                  p_dtl_inst_lkp_df.instrument_ref_type_cd).orderBy(p_dtl_inst_lkp_df.end_dt.desc(),
                                                                                    p_dtl_inst_lkp_df.created_ts.desc())
    p_dtl_inst_lkp_df = p_dtl_inst_lkp_df.withColumn("num",sf.row_number().over(g_window))
    p_dtl_inst_lkp_df = p_dtl_inst_lkp_df.where((job_obj.g_batch_date_clm.between(sf.col("start_dt"), sf.col("end_dt")))
                                                & (sf.col("num")==1))
    p_dtl_inst_lkp_df = p_dtl_inst_lkp_df.drop("num")

    # Re-joining with lookup for changing/expired security values.
    l_inst_src_df = l_inst_src_df.join(p_dtl_inst_lkp_df,(l_inst_src_df["instrument_id"]==p_dtl_inst_lkp_df["instrument_id"])
                                       & (l_inst_src_df["instrument_ref_type_cd"]==p_dtl_inst_lkp_df["instrument_ref_type_cd"])
                                       & (l_inst_src_df["instrument_ref_nm"]!=p_dtl_inst_lkp_df["instrument_ref_nm"]),"left") \
        .select(l_inst_src_df["*"],p_dtl_inst_lkp_df["start_dt"].alias("old_start_dt"),p_dtl_inst_lkp_df["end_dt"].alias("old_end_dt"),
                p_dtl_inst_lkp_df["instrument_ref_nm"].alias("old_instrument_ref_nm"))

    """
     Now if ref_type_cd is OT and instrument_ref_nm is not same as old_instrument_ref_nm, it means ADP change(OT has top priority so if OT record
     instrument_ref_nm will be adp secu only) old_instrument_ref_nm.isNull will check if previously
     adp_sec was null or not if its was null its not adp_change
    """
    l_inst_src_df = l_inst_src_df.withColumn("adp_chang", sf.when((sf.col("instrument_ref_type_cd")==parent_ref_type)
                                                                  & ~(sf.col("instrument_ref_nm").eqNullSafe(sf.col("old_instrument_ref_nm")))
                                                                  &sf.col("old_instrument_ref_nm").isNotNull(), sf.lit(True))
                                             .otherwise(sf.lit(False)))
    adp_chang_df = l_inst_src_df.filter(sf.col("adp_chang") & sf.lit(implement_one_to_one_ref_map)).select("rec_num")


    """this will track that previously, ADP was null now source has aDP value
    so we have to create new dim_instrument with this adp as currently it would be null
    end_dt_lkp: this is null means OT reference is not present for this security, means previously it was null
    instrument_ref_nm: means this time is is passed
    adp_chang: ~adp_chang means this is not adp change record, in that case also  end_dt_lkp will be null
    is_new_rec: ~is_new_rec this will exclude new instrument
    """
    l_inst_src_df = l_inst_src_df.withColumn("link_adp_dim_instrument", sf.when((sf.col("instrument_ref_type_cd")==parent_ref_type)
                                                                                &sf.col("end_dt_lkp").isNull() & sf.col("instrument_ref_nm").isNotNull()
                                                                                & ~sf.col("adp_chang") & ~sf.col("is_new_rec"), sf.lit(True))
                                             .otherwise(sf.lit(False)))
    link_adp_dim_instrument_df = l_inst_src_df.filter(sf.col("link_adp_dim_instrument"))



    l_inst_src_df = l_inst_src_df.alias("main").join(sf.broadcast(adp_chang_df).alias("adp"), sf.col("main.rec_num")==sf.col("adp.rec_num"),"left") \
        .select(l_inst_src_df['*'],sf.col("adp.rec_num").isNotNull().alias("adp_adp_chang"))
    # now take adp_change and pick all security reference records
    adp_change_src_df = l_inst_src_df.filter(sf.col("adp_adp_chang") ) \
        .withColumnRenamed("instrument_id","old_instrument_id")
    adp_change_src_inst_df = adp_change_src_df.select(sf.col("sec_ref_nm").alias("instrument_ref_nm")).distinct()

    # Insert data in to mtdt instrument id gen table
    adp_change_src_inst_df = mtdt_instrument_id_gen(job_obj, adp_change_src_inst_df, created_user_nm=job_obj._get_file_type())
    adp_change_src_df = adp_change_src_df.alias("ref").join(sf.broadcast(adp_change_src_inst_df).alias("id"),
                                                            sf.col("id.instrument_ref_nm")==sf.col("ref.sec_ref_nm")) \
        .select(adp_change_src_df['*'], sf.col("id.instrument_id"))

    new_adp_change_src_df = adp_change_src_df.withColumn("start_dt", sf.when(sf.col("instrument_ref_type_cd")==C_INSTRUMENT_REF_TYPE_CD_OT,
                                                                             sf.lit(C_DEFAULT_START_DATE)).otherwise(job_obj.g_batch_date_clm)) \
        .withColumn("end_dt",sf.to_date(sf.lit(C_DEFAULT_END_DATE)))

    l_full_src_instrument_df = l_full_src_instrument_df.union(new_adp_change_src_df.select("instrument_id",
                                                                                           sf.col("sec_ref_nm").alias("instrument_ref_nm"),
                                                                                           sf.lit(True).alias("is_new_rec"),adp_sec).distinct()) \
        .union(link_adp_dim_instrument_df.select("instrument_id",
                                                 sf.col("sec_ref_nm").alias("instrument_ref_nm"),
                                                 sf.lit(True).alias("is_new_rec"),adp_sec)) \
        .toDF("instrument_id","instrument_ref_nm","is_new_rec",adp_sec)

    # now pick only new records or reference type is changed(except adp)
    l_inst_src_df = l_inst_src_df.filter((~sf.col("adp_adp_chang")) & (~sf.col("instrument_ref_nm").eqNullSafe(sf.col("old_instrument_ref_nm")))
                                         & sf.col("instrument_ref_nm").isNotNull())

    # start_dt, end_dt derivation
    l_inst_src_df = l_inst_src_df.withColumn("start_dt",sf.when((sf.col("old_end_dt").isNotNull()) & (sf.col("old_end_dt") > job_obj.batch_date),
                                                                sf.to_date(sf.lit(job_obj.batch_date)))
                                             .when((sf.col("old_end_dt").isNotNull()) & (sf.col("old_end_dt") < job_obj.batch_date),
                                                   sf.date_add(sf.to_date("old_end_dt"),1))
                                             .when(sf.col("end_dt_lkp").isNull(),sf.to_date(sf.lit(C_DEFAULT_START_DATE)))
                                             .when(sf.col("end_dt_lkp") < job_obj.batch_date, sf.date_add(sf.to_date("end_dt_lkp"), 1))
                                             .otherwise(sf.lit(None))) \
        .withColumn("end_dt",sf.to_date(sf.lit(C_DEFAULT_END_DATE)))
    l_inst_src_df = l_inst_src_df.where(sf.col("start_dt").isNotNull())
    l_inst_src_df.cache()
    # deriving, validating update records
    df_old = l_inst_src_df.where((sf.col("old_instrument_ref_nm").isNotNull()) & (sf.col("old_end_dt").isNotNull())
                                 & (sf.col("old_end_dt") > job_obj.batch_date)) \
        .select("instrument_id", sf.col("old_instrument_ref_nm").alias("instrument_ref_nm"),"instrument_ref_type_cd",
                sf.col("old_start_dt").alias("start_dt"), sf.date_add(sf.to_date(sf.lit(job_obj.batch_date)), -1).alias("end_dt"))

    C_DTL_INSTRUMENT_REF_DEFAULT_COL_LIST["source_system_id"] = src_systm_id
    df_old = add_default_clm(df_old,C_DTL_INSTRUMENT_REF_DEFAULT_COL_LIST)
    df_old = add_audit_columns(df_old, job_obj.g_batch_date_clm)

    #TODO: We may have to colease here, once we have client file we can check and apply
    df_old = df_old.select(*C_DTL_INSTRUMENT_REF_FIELDS)
    df_old = validate_rec_and_filter(df_old, C_DTL_FIELD_TO_VALIDATE_AGAINST,
                                     job_obj._get_property(C_S3_DTL_INSTRUMENT_REF_EXCP_LOCATION_PROP_KEY),
                                     prepare_exception_clm_list(C_DTL_INSTRUMENT_REF_FIELDS))

    # adding default and audit column
    l_inst_src_df = add_default_clm(l_inst_src_df,C_DTL_INSTRUMENT_REF_DEFAULT_COL_LIST)
    l_inst_src_df = add_audit_columns(l_inst_src_df, job_obj.g_batch_date_clm)


    new_adp_change_src_df = add_default_clm(new_adp_change_src_df,C_DTL_INSTRUMENT_REF_DEFAULT_COL_LIST)
    new_adp_change_src_df = add_audit_columns(new_adp_change_src_df, job_obj.g_batch_date_clm)
    #TODO: We may have to colease here, once we have client fiC_DTL_FIELD_TO_VALIDATE_AGAINSTle we can check and apply
    l_inst_src_df = l_inst_src_df.select(*C_DTL_INSTRUMENT_REF_FIELDS).union(new_adp_change_src_df.select(*C_DTL_INSTRUMENT_REF_FIELDS))

    #Validating new records
    l_inst_src_df = validate_rec_and_filter(l_inst_src_df, C_DTL_FIELD_TO_VALIDATE_AGAINST,
                                            job_obj._get_property(C_S3_DTL_INSTRUMENT_REF_EXCP_LOCATION_PROP_KEY),
                                            prepare_exception_clm_list(C_DTL_INSTRUMENT_REF_FIELDS))

    #Saving output to s3
    l_inst_src_df = l_inst_src_df.select(*C_DTL_INSTRUMENT_REF_FIELDS)
    df_old = df_old.select(*C_DTL_INSTRUMENT_REF_FIELDS)
    save_output_to_s3(df_old,"inst_src_df", job_obj._get_property(C_S3_DTL_INSTRUMENT_REF_OUTPUT_LOCATION_PROP_KEY))
    save_output_to_s3(l_inst_src_df,"update_record_df", job_obj._get_property(C_S3_DTL_INSTRUMENT_REF_OUTPUT_LOCATION_PROP_KEY))

    return l_full_src_instrument_df

def get_condition_for_non_empty_value(p_col_list):

    l_non_empty_val = None
    for clm in p_col_list:
        if l_non_empty_val is None:
            l_non_empty_val = sf.when((sf.col(clm).isNotNull()) & (sf.col(clm) != ""),sf.col(clm))
        else:
            l_non_empty_val = l_non_empty_val.when((sf.col(clm).isNotNull()) & (sf.col(clm) != ""),sf.col(clm))

    l_non_empty_val.otherwise(None)

    return l_non_empty_val

#################################################################
#This method gets default instrument id (of 999999998)
#sor_ins_id: pass this to get instrument id
##################################################################
def get_default_instrument_id(sor_ins_id):
    JobContext.logger.info("Get default instrument id")

    # picking the latest records as per batch date
    JobContext.logger.info("Reading Dim_instrument and filtering it")
    dim_instrument_df = read_from_s3_catalog_convert_to_df(JobContext.get_property(C_GLUE_S3_DB_NAME_PROP_KEY), JobContext.get_property(C_GLUE_S3_TABLE_DIM_INSTRUMENT_PROP_KEY),
                                                            ["instrument_id", "sor_instrument_id","active_flg"])

    dim_instrument_df = transform_boolean_clm(dim_instrument_df,"active_flg")

    dim_instrument_df = dim_instrument_df.filter((sf.col("sor_instrument_id")==sor_ins_id) & (sf.col("active_flg")==True))
    ins_id = dim_instrument_df.limit(1).collect()[0].instrument_id

    JobContext.logger.info("Read default Dim_instrument id {}".format(ins_id))

    return ins_id

#################################################################
#if effective date is less than cpos batch date, return as true...else false
#if it gets missed to reach the cpos_date_calculation, return false
#True - invalid, False - Valid records
#################################################################
def cpos_effective_date_validation(src_df):
    if not adjust_back_dated_trans():
        JobContext.logger.info("Adjusting backdates transactions skipped")
        return src_df.withColumn("back_dated_check_clm", sf.lit(False))
    JobContext.logger.info("Adjusting backdates transactions")

    mtdt_batch_dt_df = read_from_s3_catalog_convert_to_df(JobContext.get_property(C_GLUE_S3_DB_NAME_PROP_KEY),JobContext.get_property(C_GLUE_S3_TABLE_MTDT_BATCH_DATE_PROP_KEY))
    
    cpos_batch_date_dict = mtdt_batch_dt_df.filter(sf.col("batch_source_cd")==C_CPOS_CONV_DT).first()

    #This could happen in QA env, when we dont have fresh unload, putting it on safe side
    if cpos_batch_date_dict is None:
        JobContext.logger.info("CPOS batch date not found, skipping date adjustment")
        return src_df.withColumn("back_dated_check_clm", sf.lit(False))
    cpos_batch_date = cpos_batch_date_dict.asDict()['batch_dt']
    JobContext.logger.info(f"CPOS batch date : {cpos_batch_date}")

    const_df = JobContext.glue_context.spark_session.createDataFrame([(cpos_batch_date,)],["cpos_batch_date"])
    
    src_df = src_df.withColumn("back_dated_check_clm", sf.col("effective_dt")<cpos_batch_date)
    JobContext.logger.info("Adjusted backdates transactions")
    return src_df
    
#################################################################
#This is available in common.custom_processor
#Not reusing it due to cyclic dependency issue, copied the same here
#################################################################
def adjust_back_dated_trans(src_df=None):
    JobContext.logger.info("Executing back dated transaction adjustment logic")
    # then check for configured property
    """"
    TRAN_BACK_DT_B4_CONV_FLG - Y/N	
    Y - Current behavior. Posts on the actual date of the transaction
    N - UBS behavior. Posts on "CONV_DT+1BUSINESS DAY"

    """
    prop_val = get_property_from_dim_tbl(C_BACK_DATED_TRANS_PROPERTY_DESC, False)

    # check for condition
    if lower_case(prop_val) in C_DIM_PROPERTY_VALUE_DESC_N:
       JobContext.logger.info("Executed back dated transaction adjustment logic")
       return True
    else:
       JobContext.logger.info("Skipping back dated transaction adjustment logic")
       return False


def create_stub_for_ben_securities_temp_fix(job_object, source_df, dim_instrument, partition_clms, security_clm, weight_p_clm,
                                   default_clm_map,clms_for_further_steps, build_inst_desc_nam ="src_inst_desc", instrument_name_2 =None,
                                   p_sor_instrument_id_prefix =C_BEN_DIM_INSTRUMENT_SOR_REF_PREFIX, child_class_type_cd=None, child_class_val=None, p_broadcast=True ):

    JobContext.logger.info("Creating stub record for benchmark association using dim_instrument, partition columns {}, security column, build_inst_desc_nam "
                           .format(str(partition_clms),security_clm,  build_inst_desc_nam))

    """
    joining based on instrument_desc, it is important as sor_instrument_id derived from instrument_id, only instrument_desc will have source Blend security information  
    Ex:PBLND10035(45.00% PW_IBOTZ,55.00% PW_EAFEHN)
    
    PBLND10035=> sor_instrument_id
    45.00% PW_IBOTZ,55.00% PW_EAFEHN=> means this security is blend of 45% PW_IBOTZ and 55% of PW_EAFEHN blean has 50.00 % 
    """

    if instrument_name_2 is None:
        instrument_name_2 = security_clm

    # this will make it (BLEND_)(\d+)(\()(.+)(\))
    INSTRUMENT_DESC_REGEX = "({})(\d+)(\()(.+)(\))".format(p_sor_instrument_id_prefix)

    """()=> represent one group 
      
    Regex INSTRUMENT_DESC_REGEX has following 5 groups
    Ex input=>BLEND_111(.4500%  PBLND1136\,.550000%  PBS100) 
     group 1=>BLEND_
     group 2=>111
     group 3=>(
     group 4=>.4500%  PBLND1136\,.550000%  PBS100=> we need this
     group 5=>)"""

    dim_instrument.cache()
    const_df = JobContext.glue_context.spark_session.createDataFrame([(C_FIRM_ID,)],["constid_for_join"])
    source_df = prepare_instrument_desc_by_conat_rec_data(source_df,partition_clms, instrument_name_2, weight_p_clm, build_inst_desc_nam)
    source_df = source_df.crossJoin(sf.broadcast(const_df))
    
    source_df_common = source_df
    
    # 1st join
    join_cond = (source_df_common["constid_for_join"]==dim_instrument["firm_id"]) & (sf.col(build_inst_desc_nam) == sf.col("instrument_desc"))
    
    source_df1 = source_df_common.join(dim_instrument, join_cond, "inner") \
                .select(source_df_common['*'], dim_instrument.instrument_id.alias("ben_lkp_instrument_id"))

    # 2nd Join
    join_cond = (source_df_common["constid_for_join"]==dim_instrument["firm_id"]) & ((sf.col(weight_p_clm)==1.00) & (sf.col(security_clm)==sf.col("sor_instrument_id")))
    
    source_df2 = source_df_common.join(dim_instrument, join_cond, "inner") \
                .select(source_df_common['*'], dim_instrument.instrument_id.alias("ben_lkp_instrument_id"))
    
    # 3rd Join
    join_cond = (source_df_common["constid_for_join"]==dim_instrument["firm_id"]) & ((sf.col(build_inst_desc_nam) == sf.col("instrument_desc")) & ((sf.col(weight_p_clm)==1.00) & (sf.col(security_clm)==sf.col("sor_instrument_id"))))
    
    source_df3 = source_df_common.join(dim_instrument, join_cond, "inner") \
                .select(source_df_common['*'], dim_instrument.instrument_id.alias("ben_lkp_instrument_id"))
    
    source_df = source_df1.union(source_df2).exceptAll(source_df3)

    # 4th Join
    source_df4 = source_df.drop("ben_lkp_instrument_id")
    source_df5 = source_df_common.exceptAll(source_df4)
    source_df5 = source_df5.withColumn("ben_lkp_instrument_id",sf.lit(None))

    source_df = source_df5.union(source_df)

    source_df.cache()
    #todo -  Remove plan before merge
    JobContext.logger.info(f"plannn - {source_df._jdf.queryExecution().simpleString()}")
    src_old_instrument_df = source_df.filter(sf.col("ben_lkp_instrument_id").isNotNull()).select(*clms_for_further_steps, 'ben_lkp_instrument_id')
    new_ben_instrument_df = source_df.filter(sf.col("ben_lkp_instrument_id").isNull())

    #From start link conversion data to batch belend instrument record
    new_ben_instrument_df = new_ben_instrument_df.drop("ben_lkp_instrument_id")
    instrument_desc_df = read_from_s3_catalog_convert_to_df(JobContext.get_property(C_GLUE_S3_DB_NAME_PROP_KEY),JobContext.get_property(C_GLUE_S3_TABLE_DIM_INSTRUMENT_PROP_KEY))
    instrument_desc_df1 = instrument_desc_df.filter((sf.col('instrument_type_cd')=='BEN') & (sf.col('constituent_benchmark_id').isNotNull()) & (sf.col('constituent_benchmark_prct')!=1))
    instrument_desc_df2 = instrument_desc_df.filter((sf.col('instrument_type_cd')=='BEN') & (sf.col('constituent_benchmark_id').isNull()) & (sf.col("instrument_name_2").isNotNull()))
    instrument_desc_df2 = instrument_desc_df2.select ( sf.col("instrument_id"),sf.col("instrument_name_2"),sf.col("firm_id")).distinct()
    instrument_desc_df1 = instrument_desc_df1.select ( sf.col("instrument_id"),sf.col("constituent_benchmark_id"),sf.col("constituent_benchmark_prct"),sf.col("firm_id")).distinct()
    instrument_desc_df1 = instrument_desc_df1.alias("src").join(sf.broadcast(instrument_desc_df2).alias("brd"), sf.col("brd.instrument_id")==sf.col("src.constituent_benchmark_id")) \
                            .select(instrument_desc_df1['*'], sf.col("instrument_name_2"))
    instrument_desc_df1 = prepare_instrument_desc_by_conat_rec_data(instrument_desc_df1, ["instrument_id"], "instrument_name_2", "constituent_benchmark_prct", constracted_clm_name="c_instrument_desc")
    instrument_desc_df1 = instrument_desc_df1.select ( sf.col("instrument_id"),sf.col("c_instrument_desc"),sf.col("firm_id")).distinct()
    g_window = Window.partitionBy("c_instrument_desc")
    instrument_desc_df1 = instrument_desc_df1.withColumn("max_instrument_id", sf.max("instrument_id").over(g_window)) \
        .where(sf.col("instrument_id") == sf.col("max_instrument_id"))
    instrument_desc_df1 = instrument_desc_df1.drop("max_instrument_id")
    join_cond1 = (new_ben_instrument_df["constid_for_join"]==instrument_desc_df1["firm_id"]) & (sf.col(build_inst_desc_nam) == sf.col("c_instrument_desc"))
    new_ben_instrument_df = new_ben_instrument_df.join(sf.broadcast(instrument_desc_df1), join_cond1, "left") \
               .select(new_ben_instrument_df['*'], instrument_desc_df1.instrument_id.alias("ben_lkp_instrument_id"))
    src_old_instrument_df1 = new_ben_instrument_df.filter(sf.col("ben_lkp_instrument_id").isNotNull()).select(*clms_for_further_steps, 'ben_lkp_instrument_id')
    new_ben_instrument_df = new_ben_instrument_df.filter(sf.col("ben_lkp_instrument_id").isNull())
    #End link conversion data to batch belend instrument record

    sf_partition_clms = [sf.col(clm).desc() for clm in partition_clms ]
    window_spec = Window.partitionBy(new_ben_instrument_df[build_inst_desc_nam]) \
        .orderBy(sf_partition_clms)
    source_df = source_df.drop("constid_for_join")
    src_new_instrument_df = new_ben_instrument_df.drop("ben_lkp_instrument_id")

    new_ben_instrument_df = new_ben_instrument_df.withColumn("row_num",sf.dense_rank().over(window_spec))
    new_ben_instrument_df = new_ben_instrument_df.filter("row_num==1")
    new_ben_instrument_df.cache()

    new_ben_instrument_df = mtdt_instrument_id_gen(job_object, new_ben_instrument_df,in_df_security_col_nm = build_inst_desc_nam, created_user_nm=C_CREATED_USER_NM)
    new_ben_instrument_df.cache()

    join_cond =[new_ben_instrument_df[build_inst_desc_nam]==src_new_instrument_df[build_inst_desc_nam], new_ben_instrument_df["constituent_benchmark_id"]==src_new_instrument_df["constituent_benchmark_id"],
                new_ben_instrument_df[weight_p_clm]==src_new_instrument_df[weight_p_clm]]
    src_new_instrument_df = src_new_instrument_df.alias("src").join(new_ben_instrument_df.alias("inst"), join_cond,"left_outer") \
        .select(src_new_instrument_df['*'],sf.col("instrument_id").alias("ben_lkp_instrument_id")).select(*clms_for_further_steps, "ben_lkp_instrument_id")
    src_for_brd_rec_df = src_new_instrument_df.union(src_old_instrument_df).union(src_old_instrument_df1)

    src_for_brd_rec_df.cache()
    new_ben_instrument_df = new_ben_instrument_df.withColumn("sor_instrument_id", sf.concat(sf.lit(p_sor_instrument_id_prefix), sf.col("instrument_id")))
    #TODO: Not sure if this is correct
    new_ben_instrument_df = new_ben_instrument_df.withColumn("source_system_id", sf.lit(C_COMMON_SOURCE_SYSTEM_ID))

    currency_Cd = derive_currency_cd()
    new_ben_instrument_df = new_ben_instrument_df.withColumn('currency_cd', sf.lit(currency_Cd))

    new_ben_instrument_df  = add_default_clm(new_ben_instrument_df , default_clm_map)
    new_ben_instrument_df  = add_audit_columns(new_ben_instrument_df,p_batch_date_clm=job_object.g_batch_date_clm,add_modification_clm=True)

    window_mngd_clm = Window.partitionBy(sf.col("instrument_id")).orderBy(sf.col("constituent_benchmark_id"))

    driver_inst = new_ben_instrument_df.withColumn("row_num", sf.row_number().over(window_mngd_clm)).filter("row_num==1")
    driver_inst = driver_inst.withColumn("constituent_benchmark_id", sf.lit(None)) \
        .withColumn("constituent_benchmark_start_dt", sf.lit(None)) \
        .withColumn("constituent_benchmark_end_dt", sf.lit(None)) \
        .withColumn("constituent_benchmark_prct", sf.lit(None))

    new_ben_instrument_df = new_ben_instrument_df.select(*C_DIM_INSTRUMENT_COLUMN_LIST).union(driver_inst.select(*C_DIM_INSTRUMENT_COLUMN_LIST))

    # Removed concatenation in instrument_desc field
    new_ben_instrument_df = new_ben_instrument_df.withColumn("instrument_desc",sf.col('instrument_desc'))
    #creating stub for dtl_instrument_ref
    driver_inst.cache()
    write_to_dtl_instrument_ref = driver_inst.withColumn('instrument_ref_type_cd',sf.lit('SOR_BEN'))\
        .withColumn('instrument_ref_nm',sf.col('sor_instrument_id')) \
        .withColumnRenamed("modified_user_nm","modified_user") \
        .withColumnRenamed("modified_program_nm","modified_program")
    save_output_to_s3(write_to_dtl_instrument_ref.select(*C_DTL_INSTRUMENT_REF_FIELDS),'dtl_instrument_ref',job_object._get_property(C_S3_DTL_INSTRUMENT_REF_OUTPUT_LOCATION_PROP_KEY))
    #saving in dim_instrument
    save_output_to_s3(new_ben_instrument_df, "target_df", job_object._get_property(C_S3_DIM_INSTRUMENT_OUTPUT_LOC_PROP_KEY))

    dim_class_type_df= None
    if (child_class_type_cd is not None) & (child_class_val is not None):
        JobContext.logger.info(f"Passed child_class_type_cd is: {child_class_type_cd} and child_class_val is: {child_class_val}")
        dim_class_type_df = read_from_catalog_and_filter_inactive_rec(JobContext.get_property(C_GLUE_S3_TABLE_DIM_CLASS_TYPE)) \
                                    .filter((sf.upper(sf.col("child_class_type_cd"))==child_class_type_cd )&
                                        (sf.upper(sf.col('child_class_val')) == child_class_val)) \
                                        .select("class_type_id").first()
    else:
        JobContext.logger.info("child_class_type_cd and child_class_val both are required for fac_class_entity records")

    if dim_class_type_df is not None:
        JobContext.logger.info("Preparing Fact_class_entity records for class_type_id {}")
        fact_class_df = new_ben_instrument_df.select(sf.col("instrument_id").alias("entity_id")).distinct()
        class_type_id = dim_class_type_df.asDict()['class_type_id']
        JobContext.logger.info("Class_type_id {}, found for custom blend flg".format(class_type_id))
        C_FACT_CLASS_ENTITY_DEFAULT_CLM_MAPPING = {'entity_type_cd': C_FACT_CLASS_ENTITY_REL_ENTITY_TYPE_CD_BENCHMARK, 'overriding_entity_type_cd': None,
                                                   'overriding_entity_id': None, 'effective_from_dt': C_DEFAULT_START_DATE,
                                                   'effective_to_dt': C_DEFAULT_END_DATE, 'allocation_prcnt': None, 'firm_id': C_FIRM_ID}

        C_FACT_CLASS_ENTITY_COL_LIST = ['entity_id','class_type_id','entity_type_cd','overriding_entity_type_cd',
                                    'overriding_entity_id','effective_from_dt','effective_to_dt','allocation_prcnt',
                                    'firm_id','active_flg','batch_dt','created_program_nm','created_user_nm','created_ts']
        fact_class_df = fact_class_df.withColumn("class_type_id", sf.lit(class_type_id))
        fact_class_df = add_default_clm(fact_class_df,C_FACT_CLASS_ENTITY_DEFAULT_CLM_MAPPING)
        fact_class_df = add_audit_columns(fact_class_df, JobContext.batch_date_clm,created_user_nm=C_CONVERSION_USER_NM)
        save_output_to_s3(fact_class_df.select(C_FACT_CLASS_ENTITY_COL_LIST),"fact_class_entity",JobContext.get_property(C_S3_FACT_CLASS_ENTITY_OUTPUT_LOCATION))
    else:
        JobContext.logger.info(f"Custom blend flag is not present in Dim_class_type with child_class_type_cd is: {child_class_type_cd} "
                               f"and child_class_val is: {child_class_val} , so skipping fact_class_entity records")

    JobContext.logger.info("Created stub record for benchmark association using dim_instrument ")
    return src_for_brd_rec_df

def derive_sec_usage_cd_instrument_id(p_src_df,joining_clm):
    JobContext.logger.info("Deriving sec_usage_cd new instrument_id")
    # Lookups
    g_fact_cls_entity_df = read_from_s3_catalog_convert_to_df(JobContext.get_property(C_GLUE_S3_DB_NAME_PROP_KEY),
                                                              JobContext.get_property(C_S3_FACT_CLASS_ENTITY_TBL_NME),
                                                              ["entity_id","class_type_id","effective_from_dt",
                                                               "effective_to_dt","active_flg","created_ts"])

    g_dim_cls_typ_df = read_from_catalog_and_filter_inactive_rec(JobContext.get_property(C_GLUE_S3_TABLE_DIM_CLASS_TYPE),
                                                                 ["class_type_id","child_class_type_cd","child_class_val",
                                                                  "active_flg","created_ts"])
    g_dim_cls_typ_df = g_dim_cls_typ_df.filter(sf.col('child_class_type_cd') == C_FCE_CLASS_TYPE_CD_SEC_USAGE)

    # joining lookups and fetching child_class_type_cd for existing
    g_fact_cls_entity_df = g_fact_cls_entity_df.join(g_dim_cls_typ_df, ["class_type_id"], "inner") \
        .select(g_fact_cls_entity_df["*"], g_dim_cls_typ_df["child_class_type_cd"], g_dim_cls_typ_df['child_class_val'])

    dtl_instrument_df = read_from_catalog_and_filter_inactive_rec(JobContext.get_property(C_GLUE_S3_TABLE_DTL_INSTRUMENT_REF_PROP_KEY))
    dtl_instrument_df = dtl_instrument_df.filter(sf.col("instrument_ref_type_cd") == "CU")

    # Here taking records of instrument_ref_nm whose count is greater than 1 only.
    dtl_instrument_count_df = dtl_instrument_df.groupBy("instrument_ref_nm").count()
    dtl_instrument_count_df = dtl_instrument_count_df.filter(sf.col('count') > 1)
    dtl_instrument_df = dtl_instrument_df.select('instrument_id', 'instrument_ref_nm', 'start_dt', 'end_dt')

    # joining two data frames
    dtl_instrument_df = dtl_instrument_df.join(dtl_instrument_count_df, ["instrument_ref_nm"], "inner") \
        .select(dtl_instrument_df["*"])

    g_fact_cls_entity_df = g_fact_cls_entity_df.join(dtl_instrument_df, g_fact_cls_entity_df.entity_id == dtl_instrument_df.instrument_id,"inner") \
        .select(g_fact_cls_entity_df["*"], dtl_instrument_df["*"]) \
        .orderBy('instrument_ref_nm', 'child_class_val')

    g_windowSpec = Window.partitionBy(g_fact_cls_entity_df['instrument_ref_nm']).orderBy(g_fact_cls_entity_df['child_class_val'].asc())
    g_fact_cls_entity_df = g_fact_cls_entity_df.withColumn("row_num", sf.row_number().over(g_windowSpec)).filter("row_num==1")

    joining_cond = p_src_df[joining_clm] == g_fact_cls_entity_df['instrument_ref_nm']
    p_src_df = p_src_df.join(g_fact_cls_entity_df, joining_cond, "left") \
        .select(p_src_df["*"], g_fact_cls_entity_df['instrument_id'].alias('cu_instrument_id'))
    return p_src_df

def exclude_invalid_accounts(p_src_df, clm_to_validate, EXCLUDE_FLG=False):
    JobContext.logger.info("Deriving new method to eliminate invalid records")
    if EXCLUDE_FLG:
        p_src_df = p_src_df.withColumn('acc_len', sf.length(p_src_df[clm_to_validate]))
        p_src_df = p_src_df.withColumn('exclude_acc_type', sf.substring(p_src_df[clm_to_validate], 1, 1))
        p_src_df = p_src_df.withColumn('invalid_acc_validator', sf.when((sf.col('acc_len') == 8)
                                                                        & (sf.col('exclude_acc_type').isin(C_EXCLUDE_ACCOUNT_TYPE)), sf.lit('true'))
                                       .otherwise(sf.lit(None)))
    else:
        p_src_df = p_src_df.withColumn('invalid_acc_validator', sf.lit(True))
    return p_src_df

def derive_dtl_inst_ref_expired(self, p_src_adp_df):
    JobContext.logger.info("Deriving new method to derive new instrument_id for expired records of OT")
    l_batch_date_clm = sf.to_date(sf.lit(JobContext.get_property(C_BATCH_DATE_PROP_KEY)))
    dtl_instrument_adp_update_df = read_from_catalog_and_filter_inactive_rec(JobContext.get_property(C_GLUE_S3_TABLE_DTL_INSTRUMENT_REF_PROP_KEY))
    dtl_instrument_adp_update_df = dtl_instrument_adp_update_df.filter(dtl_instrument_adp_update_df.instrument_ref_type_cd == 'OT')
    g_window = Window.partitionBy(dtl_instrument_adp_update_df.instrument_ref_nm,
                                  dtl_instrument_adp_update_df.instrument_ref_type_cd) \
        .orderBy(dtl_instrument_adp_update_df.start_dt.desc(), dtl_instrument_adp_update_df.created_ts.desc())

    dtl_instrument_adp_update_df = dtl_instrument_adp_update_df.withColumn("row_num", sf.row_number().over(g_window))
    dtl_instrument_adp_update_df = dtl_instrument_adp_update_df.filter("row_num == 1")
    join_cond = [(p_src_adp_df['instrument_ref_nm'] == dtl_instrument_adp_update_df['instrument_ref_nm'])
                 & (p_src_adp_df['instrument_ref_type_cd'] == dtl_instrument_adp_update_df['instrument_ref_type_cd'])
                 & (sf.col("end_dt") < l_batch_date_clm)]
    p_src_adp_df = p_src_adp_df.join(dtl_instrument_adp_update_df, join_cond, how='left_outer') \
        .select(p_src_adp_df['*'], dtl_instrument_adp_update_df.instrument_id, dtl_instrument_adp_update_df.end_dt)
    p_src_adp_df = p_src_adp_df.withColumn("new_instrument_record", sf.col('end_dt') < l_batch_date_clm).dropDuplicates(['instrument_id'])
    p_src_adp_df = p_src_adp_df.filter("new_instrument_record").drop('instrument_id', 'end_dt')

    g_mtdt_instrument_id_gen_df = p_src_adp_df.select(p_src_adp_df.instrument_ref_nm)
    #Insert data in to mtdt instrument id gen table
    self.C_CREATED_PROFRAM_NM=self._get_file_type()
    new_instrument_id_gen_df = mtdt_instrument_id_gen(self, g_mtdt_instrument_id_gen_df, created_user_nm=self.C_CREATED_PROFRAM_NM)

    ###############################
    # Method to read mtdt_instrument_id_gen from Redshift
    ###############################
    g_mtdt_instrument_id_gen_df = read_from_s3_catalog_convert_to_df(self.C_DB_NAME,JobContext.get_property(C_GLUE_S3_TABLE_MTDT_INSTR_ID_GEN_PROP_KEY))

    self.logger.info("Joining New Instrument Id Data with mtdt_instrument_id_gen for instrument_id")
    g_new_inst_id_df = p_src_adp_df.join(g_mtdt_instrument_id_gen_df,
                                         p_src_adp_df.instrument_ref_nm == g_mtdt_instrument_id_gen_df.instrument_ref_nm,
                                         "left_outer").select(p_src_adp_df['*'],
                                                              g_mtdt_instrument_id_gen_df.instrument_id.alias("mtdt_instrument_id")).drop(
        "source_system_id").withColumn("instrument_id", sf.col("mtdt_instrument_id"))
    g_new_inst_id_df = g_new_inst_id_df.withColumn('start_dt', sf.lit(l_batch_date_clm)).withColumn('end_dt', sf.lit(C_DEFAULT_END_DATE))

    # dim_instrument logic for new instrument_id's for expired adp's
    source_system_id = derive_src_sys_id(C_SEC_OTHER_CLIENT_NUMBER)
    currency_cd_da_frame = derive_currency_cd()

    self.logger.info("Loading new Instrument_id, for dim_instrument")
    g_dim_instrument = g_new_inst_id_df.withColumn("source_system_id", sf.lit(source_system_id).cast("int")) \
        .withColumn("currency_cd", sf.lit(currency_cd_da_frame))
    g_dim_instrument = add_default_clm(g_dim_instrument, C_DIM_INSTRUMENT_DEFAULT_COL_LIST)
    g_dim_instrument = add_audit_columns(g_dim_instrument, l_batch_date_clm, add_modification_clm=True)
    g_dim_instrument = g_dim_instrument.withColumn('sor_instrument_id', sf.col('instrument_ref_nm')).select(*C_DIM_INSTRUMENT_COLUMN_LIST)
    save_output_to_s3(g_dim_instrument, "dim_instrument", self._get_property(C_S3_DIM_INSTRUMENT_OUTPUT_LOC_PROP_KEY))
    self.logger.info("Loaded new Instrument_id, for dim_instrument")

    # dtl_instrument_ref logic for new instrument_id's for expired adp's
    g_source_sys_id = derive_src_sys_id(C_SEC_OTHER_CLIENT_NUMBER)
    C_DTL_INSTRUMENT_REF_DEFAULT_COL_LIST["source_system_id"] = g_source_sys_id
    g_new_inst_id_df = add_default_clm(g_new_inst_id_df, C_DTL_INSTRUMENT_REF_DEFAULT_COL_LIST)
    g_new_inst_id_df = add_audit_columns(g_new_inst_id_df, l_batch_date_clm)

    g_new_inst_id_df = g_new_inst_id_df.select(*C_DTL_INSTRUMENT_REF_FIELDS)
    save_output_to_s3(g_new_inst_id_df, "update_record_df", self._get_property(C_S3_DTL_INSTRUMENT_REF_OUTPUT_LOCATION_PROP_KEY))
    self.logger.info("Joining completed between New Instrument Id Data and mtdt_instrument_id_gen for instrument_id")

    self.logger.info("loading updated instruments from expired to fact_class_entity table")
    # Below method is used to load new securities, which are updated to new from expired securities, it will load to
    # fact_class_entity (re-continuing the expired securities from dtl_inst_ref).
    fact_class_entity_for_expire(self, g_new_inst_id_df)

def derive_dtl_inst_ref_insert_update_expired(self, src_adp_df):
    JobContext.logger.info("Deriving new method to derive instrument_id based on OT and FIID")
    l_batch_date_clm = sf.to_date(sf.lit(JobContext.get_property(C_BATCH_DATE_PROP_KEY)))

    # scenario - 1: In this will check for both adp and fiid match and will use for update and expire scenario's.
    dtl_inst_ref_adp_insert_df = read_from_catalog_and_filter_inactive_rec(JobContext.get_property(C_GLUE_S3_TABLE_DTL_INSTRUMENT_REF_PROP_KEY))
    dtl_inst_ref_adp_insert_df = dtl_inst_ref_adp_insert_df.filter(dtl_inst_ref_adp_insert_df.instrument_ref_type_cd.isin(['OT', 'FIID']))
    g_window = Window.partitionBy(dtl_inst_ref_adp_insert_df.instrument_ref_nm,
                                  dtl_inst_ref_adp_insert_df.start_dt) \
        .orderBy(dtl_inst_ref_adp_insert_df.end_dt.desc(), dtl_inst_ref_adp_insert_df.created_ts.desc())

    dtl_inst_ref_adp_insert_df = dtl_inst_ref_adp_insert_df.withColumn("row_num", sf.row_number().over(g_window))
    dtl_inst_ref_adp_insert_df = dtl_inst_ref_adp_insert_df.filter("row_num == 1")
    dtl_inst_ref_adp_insert_df = dtl_inst_ref_adp_insert_df.withColumn('instrument_ref_nm',sf.translate(sf.col('instrument_ref_nm'),'\,',','))
    src_adp_df = src_adp_df.withColumn('processing_dt', sf.lit(l_batch_date_clm))

    join_cond_adp = [(src_adp_df['instrument_ref_nm_ot'] == dtl_inst_ref_adp_insert_df['instrument_ref_nm'])
                     & (src_adp_df['instrument_ref_type_cd_ot'] == dtl_inst_ref_adp_insert_df['instrument_ref_type_cd'])
                     & (sf.col("src.processing_dt").between(sf.col("lkp.start_dt"), sf.col("lkp.end_dt")))]

    src_adp_df = src_adp_df.alias("src").join(dtl_inst_ref_adp_insert_df.alias("lkp"), join_cond_adp, how='left_outer') \
        .select(src_adp_df['*'], dtl_inst_ref_adp_insert_df.instrument_id)

    src_adp_update_df = src_adp_df.filter(sf.col('instrument_id').isNotNull())

    src_ffid_df = src_adp_df.filter(sf.col('instrument_id').isNull()).drop('instrument_id')

    join_cond_fiid = [(src_ffid_df['instrument_ref_nm_fiid'] == dtl_inst_ref_adp_insert_df['instrument_ref_nm'])
                      & (src_ffid_df['instrument_ref_type_cd_fiid'] == dtl_inst_ref_adp_insert_df['instrument_ref_type_cd'])]
    src_ffid_df = src_ffid_df.join(dtl_inst_ref_adp_insert_df, join_cond_fiid, how='left_outer') \
        .select(src_ffid_df['*'], dtl_inst_ref_adp_insert_df.instrument_id)

    src_ffid_update_df = src_ffid_df.filter(sf.col('instrument_id').isNotNull())

    src_adp_ffid_matched = src_adp_update_df.union(src_ffid_update_df)

    src_adp_ffid_matched = src_adp_ffid_matched.withColumn('instrument_ref_nm', sf.when(~isNull_check('instrument_ref_nm_ot'), sf.col('instrument_ref_nm_ot'))
                                                           .otherwise(sf.col('instrument_ref_nm_fiid'))) \
        .withColumn('instrument_ref_type_cd', sf.when(~isNull_check('instrument_ref_type_cd_ot'), sf.col('instrument_ref_type_cd_ot'))
                    .otherwise(sf.col('instrument_ref_type_cd_fiid')))
    src_adp_expired_df = src_adp_ffid_matched

    # scenario - 2: Both Adp update and expire logic.
    dtl_inst_ref_ot_df = read_from_catalog_and_filter_inactive_rec(JobContext.get_property(C_GLUE_S3_TABLE_DTL_INSTRUMENT_REF_PROP_KEY))
    dtl_inst_ref_ot_df = dtl_inst_ref_ot_df.filter(dtl_inst_ref_ot_df.instrument_ref_type_cd == 'OT')
    dtl_inst_ref_adp_expired_df = dtl_inst_ref_ot_df

    g_window_ot = Window.partitionBy(dtl_inst_ref_ot_df.instrument_id, dtl_inst_ref_ot_df.start_dt) \
        .orderBy(dtl_inst_ref_ot_df.start_dt.desc(), dtl_inst_ref_ot_df['end_dt'].asc(), dtl_inst_ref_ot_df.created_ts.desc())
    dtl_inst_ref_ot_df = dtl_inst_ref_ot_df.withColumn("latest_rec", sf.row_number().over(g_window_ot) == 1).filter("latest_rec").filter("active_flg")
    dtl_inst_ref_ot_df = dtl_inst_ref_ot_df.withColumn('instrument_ref_nm',sf.translate(sf.col('instrument_ref_nm'),'\,',','))
    dtl_inst_ref_ot_df.cache()

    # Re-joining with lookup for changing/expired security values.
    src_adp_ffid_matched = src_adp_ffid_matched.filter(src_adp_ffid_matched.instrument_ref_type_cd == 'OT')
    src_adp_ffid_matched = src_adp_ffid_matched.join(dtl_inst_ref_ot_df, (src_adp_ffid_matched["instrument_id"] == dtl_inst_ref_ot_df["instrument_id"])
                                                     & (src_adp_ffid_matched["instrument_ref_type_cd"] == dtl_inst_ref_ot_df["instrument_ref_type_cd"])
                                                     & (src_adp_ffid_matched["instrument_ref_nm"] != dtl_inst_ref_ot_df["instrument_ref_nm"]), "left") \
        .select(src_adp_ffid_matched["*"],dtl_inst_ref_ot_df["start_dt"].alias("old_start_dt"),dtl_inst_ref_ot_df["end_dt"].alias("old_end_dt"),
                dtl_inst_ref_ot_df["instrument_ref_nm"].alias("old_instrument_ref_nm"))

    src_adp_ffid_matched = src_adp_ffid_matched.withColumn("start_dt", sf.when((sf.col("old_end_dt").isNotNull()) & (sf.col("old_end_dt") > l_batch_date_clm),
                                                                               sf.to_date(sf.lit(l_batch_date_clm)))
                                                           .otherwise(sf.lit(None))) \
        .withColumn("end_dt", sf.to_date(sf.lit(C_DEFAULT_END_DATE)))
    src_adp_ffid_matched = src_adp_ffid_matched.where(sf.col("start_dt").isNotNull())
    src_adp_ffid_matched.cache()
    # deriving, validating update records
    df_old = src_adp_ffid_matched.where((sf.col("old_instrument_ref_nm").isNotNull()) & (sf.col("old_end_dt").isNotNull())
                                        & (sf.col("old_end_dt") > l_batch_date_clm)) \
        .select("instrument_id",sf.col("old_instrument_ref_nm").alias("instrument_ref_nm"),"instrument_ref_type_cd",
                sf.col("old_start_dt").alias("start_dt"), sf.date_add(sf.to_date(sf.lit(l_batch_date_clm)), -1).alias("end_dt"))
    g_source_sys_id = derive_src_sys_id(C_SEC_OTHER_CLIENT_NUMBER)
    C_DTL_INSTRUMENT_REF_DEFAULT_COL_LIST["source_system_id"] = g_source_sys_id
    df_old = add_default_clm(df_old, C_DTL_INSTRUMENT_REF_DEFAULT_COL_LIST)
    df_old = add_audit_columns(df_old, l_batch_date_clm)

    df_old = df_old.select(*C_DTL_INSTRUMENT_REF_FIELDS)
    df_old = validate_rec_and_filter(df_old, C_DTL_FIELD_TO_VALIDATE_AGAINST,
                                     self._get_property(C_S3_DTL_INSTRUMENT_REF_EXCP_LOCATION_PROP_KEY),
                                     prepare_exception_clm_list(C_DTL_INSTRUMENT_REF_FIELDS))

    # adding default and audit column
    src_adp_ffid_matched = add_default_clm(src_adp_ffid_matched, C_DTL_INSTRUMENT_REF_DEFAULT_COL_LIST)
    src_adp_ffid_matched = add_audit_columns(src_adp_ffid_matched, l_batch_date_clm)

    src_adp_ffid_matched = src_adp_ffid_matched.select(*C_DTL_INSTRUMENT_REF_FIELDS)

    # Validating new records
    src_adp_ffid_matched = validate_rec_and_filter(src_adp_ffid_matched, C_DTL_FIELD_TO_VALIDATE_AGAINST,
                                                   self._get_property(C_S3_DTL_INSTRUMENT_REF_EXCP_LOCATION_PROP_KEY),
                                                   prepare_exception_clm_list(C_DTL_INSTRUMENT_REF_FIELDS))

    # Saving output to s3
    src_adp_ffid_matched = src_adp_ffid_matched.select(*C_DTL_INSTRUMENT_REF_FIELDS)
    df_old = df_old.select(*C_DTL_INSTRUMENT_REF_FIELDS)
    save_output_to_s3(df_old, "source_data_inst", self._get_property(C_S3_DTL_INSTRUMENT_REF_OUTPUT_LOCATION_PROP_KEY))
    save_output_to_s3(src_adp_ffid_matched, "update_record_df", self._get_property(C_S3_DTL_INSTRUMENT_REF_OUTPUT_LOCATION_PROP_KEY))

    # Below logic is used to re-continue the adp which is ended or expired.
    g_window_u2 = Window.partitionBy(dtl_inst_ref_adp_expired_df.instrument_id, dtl_inst_ref_adp_expired_df.instrument_ref_type_cd) \
        .orderBy(dtl_inst_ref_adp_expired_df.start_dt.desc(), dtl_inst_ref_adp_expired_df.created_ts.desc())
    dtl_inst_ref_adp_expired_df = dtl_inst_ref_adp_expired_df.withColumn("latest_rec", sf.row_number().over(g_window_u2) == 1) \
        .filter("latest_rec").filter("active_flg")
    dtl_inst_ref_adp_expired_df.cache()
    dtl_inst_ref_adp_expired_df = dtl_inst_ref_adp_expired_df.filter(dtl_inst_ref_adp_expired_df.instrument_ref_type_cd=='OT')
    src_adp_expired_df = src_adp_expired_df.filter(src_adp_expired_df.instrument_ref_type_cd == 'OT')

    src_adp_expired_df = src_adp_expired_df.join(dtl_inst_ref_adp_expired_df,(src_adp_expired_df["instrument_id"] == dtl_inst_ref_adp_expired_df["instrument_id"])
                                                 & (src_adp_expired_df["instrument_ref_type_cd"] == dtl_inst_ref_adp_expired_df["instrument_ref_type_cd"])
                                                 & (src_adp_expired_df["instrument_ref_nm"] == dtl_inst_ref_adp_expired_df["instrument_ref_nm"])
                                                 & (dtl_inst_ref_adp_expired_df.end_dt < l_batch_date_clm), "left") \
        .select(src_adp_expired_df["*"], dtl_inst_ref_adp_expired_df["start_dt"].alias("old_start_dt"), dtl_inst_ref_adp_expired_df["end_dt"].alias("old_end_dt"))

    src_adp_expired_df = src_adp_expired_df.withColumn("start_dt", sf.when((sf.col("old_end_dt").isNotNull()) & (sf.col("old_end_dt") < l_batch_date_clm),
                                                                           sf.date_add(sf.to_date("old_end_dt"), 1))
                                                       .otherwise(sf.lit(None))) \
        .withColumn("end_dt", sf.to_date(sf.lit(C_DEFAULT_END_DATE)))

    src_adp_expired_df = src_adp_expired_df.where(sf.col("start_dt").isNotNull())
    src_adp_expired_df.cache()

    # adding default and audit column
    C_DTL_INSTRUMENT_REF_DEFAULT_COL_LIST["source_system_id"] = g_source_sys_id
    src_adp_expired_df = add_default_clm(src_adp_expired_df, C_DTL_INSTRUMENT_REF_DEFAULT_COL_LIST)
    src_adp_expired_df = add_audit_columns(src_adp_expired_df, l_batch_date_clm)

    src_adp_expired_df = src_adp_expired_df.select(*C_DTL_INSTRUMENT_REF_FIELDS)

    # Validating new records
    src_adp_expired_df = validate_rec_and_filter(src_adp_expired_df, C_DTL_FIELD_TO_VALIDATE_AGAINST,
                                                 self._get_property(C_S3_DTL_INSTRUMENT_REF_EXCP_LOCATION_PROP_KEY),
                                                 prepare_exception_clm_list(C_DTL_INSTRUMENT_REF_FIELDS))

    # Saving output to s3
    src_adp_expired_df = src_adp_expired_df.select(*C_DTL_INSTRUMENT_REF_FIELDS)
    save_output_to_s3(src_adp_expired_df, "source_data_same_adp_inst", self._get_property(C_S3_DTL_INSTRUMENT_REF_OUTPUT_LOCATION_PROP_KEY))
    JobContext.logger.info("Derived new method to derive instrument_id based on OT and FIID")

# Method to remove duplicates loading into dim_instrument

def dim_instrument_duplicates_removal(self, dim_instrument_output_df):
        df1 = dim_instrument_output_df.select("instrument_id", "start_dt").distinct()
        window_df1 = Window.partitionBy(df1.instrument_id).orderBy(df1.start_dt.desc())
        df1 = df1.withColumn("num",sf.row_number().over(window_df1))
        df1 = df1.where(sf.col("num")==1).drop("num")

        rlf_lkp_df = read_from_s3_catalog_convert_to_df(self.C_DB_NAME,
                                                        self._get_property(C_GLUE_S3_TABLE_DIM_INSTRUMENT_PROP_KEY)).drop("start_dt")

        if (rlf_lkp_df.count() == 0) or (dim_instrument_output_df.count() == 0):
            dim_dup_rem_df = dim_instrument_output_df
        else:
            drop_list1 = ["batch_dt","created_program_nm","created_user_nm","created_ts","modified_program_nm",
                          "modified_user_nm","modified_ts"]
            drop_list2 = ["batch_dt", "created_program_nm", "created_user_nm", "created_ts", "modified_program",
                         "modified_user", "modified_ts"]
            dim_instrument_output_df1 = dim_instrument_output_df
            dim_instrument_output_df2 = dim_instrument_output_df1.drop("start_dt")
            dim_instrument_output_df2 = (dim_instrument_output_df2.drop(*drop_list1)).subtract(rlf_lkp_df.drop(*drop_list2))

            for i in drop_list1:
                dim_instrument_output_df2 = dim_instrument_output_df2.withColumn(i, sf.lit(dim_instrument_output_df.first()[i]))
            dim_dup_rem_df = dim_instrument_output_df2.join(df1, ["instrument_id"], "inner")
        return dim_dup_rem_df

# Below method is used to load new securities, which are updated to new from expired securities, it will load to
# fact_class_entity (re-continuing the expired securities from dtl_inst_ref).
def fact_class_entity_for_expire(self, p_src_df):
    l_batch_date_clm = sf.to_date(sf.lit(JobContext.get_property(C_BATCH_DATE_PROP_KEY)))
    C_FACT_CLASS_ENTITY_S3_LOCATION_NEW_REC_INVAILD = JobContext.get_property(C_S3_FACT_CLASS_ENTITY_EXCP_OUTPUT_LOCATION)
    p_src_df = p_src_df.select("instrument_id")

    # Read fact_class_entity lookup table
    fact_class_entity_df = read_from_catalog_and_filter_inactive_rec(JobContext.get_property(C_S3_FACT_CLASS_ENTITY_TBL_NME))
    # Read dim_class_type lookup table
    dim_class_type_df = read_from_catalog_and_filter_inactive_rec(self._get_property(C_GLUE_S3_TABLE_DIM_CLASS_TYPE))
    l_dim_class_type_df = dim_class_type_df
    dim_class_type_df = dim_class_type_df.filter(dim_class_type_df.child_class_type_cd == C_DIM_CLASS_SEC_TYPE)

    fact_class_entity_df = fact_class_entity_df.join(dim_class_type_df, fact_class_entity_df.class_type_id == dim_class_type_df.class_type_id, how='left')\
        .select(fact_class_entity_df['*'], dim_class_type_df.child_class_type_cd).filter(sf.col("child_class_type_cd").isNotNull())
    fact_class_entity_df = fact_class_entity_df.select('entity_id').distinct()

    p_src_df = p_src_df.join(fact_class_entity_df, p_src_df.instrument_id == fact_class_entity_df.entity_id, how='left')\
        .select(p_src_df['*'], fact_class_entity_df.entity_id)
    p_src_df = p_src_df.filter(sf.col("entity_id").isNull()).drop("entity_id")
    p_src_df = p_src_df.withColumnRenamed('instrument_id', 'entity_id')

    l_dim_class_type_df = l_dim_class_type_df.select('class_type_id')\
        .where((((sf.col('child_class_type_cd')=='SEC_TYPE') & (sf.col('child_class_val')=='Equity') & (sf.col('entity_type_cd')=='SECURITY')) |
                ((sf.col('child_class_type_cd')=='SEC_CLASS') & (sf.col('child_class_val')=='Other') & (sf.col('entity_type_cd')=='SECURITY')) |
                ((sf.col('child_class_type_cd')=='SEC_ASSET_CLASS') & (sf.col('child_class_val')=='Other') & (sf.col('entity_type_cd')=='SECURITY')) |
                ((sf.col('child_class_type_cd')=='SEC_ASSET_CATEGORY') & (sf.col('child_class_val')=='Other') & (sf.col('entity_type_cd')=='SECURITY')) |
                ((sf.col('child_class_type_cd')=='SEC_MKT_GROUP') & (sf.col('child_class_val')=='Other') & (sf.col('entity_type_cd')=='SECURITY'))) &
               (sf.col('active_flg')))

    p_src_df = p_src_df.crossJoin(sf.broadcast(l_dim_class_type_df))

    p_fact_clss_df = p_src_df.withColumn('effective_from_dt', sf.to_date(sf.lit(C_DEFAULT_START_DATE)))\
        .withColumn('effective_to_dt', sf.to_date(sf.lit(C_DEFAULT_END_DATE)))
    ###############################
    # Adding Default column
    ###############################
    p_fact_clss_df = add_default_clm(p_fact_clss_df, C_FACT_CLASS_ENTITY_DEFAULT_COL_LIST)
    ###############################
    # Adding Audit column
    ###############################
    p_fact_clss_df = add_audit_columns(p_fact_clss_df, l_batch_date_clm, add_modification_clm=False)
    p_fact_clss_df = p_fact_clss_df.select(C_FACT_CLASS_ENTITY_COL_LIST)

    ###############################
    # Validating records
    ###############################
    p_input_df = validate_rec_and_filter(p_fact_clss_df, C_FACT_CLASS_ENTITY_FIELD_TO_VALIDATE_AGAINST,
                                         C_FACT_CLASS_ENTITY_S3_LOCATION_NEW_REC_INVAILD)
    ###############################
    # Saving output to s3
    ###############################
    save_output_to_s3(p_fact_clss_df, "fact_class_entity", JobContext.get_property(C_S3_FACT_CLASS_ENTITY_OUTPUT_LOCATION))
    JobContext.logger.info("Record insertion for Fact class entity - Ends")
