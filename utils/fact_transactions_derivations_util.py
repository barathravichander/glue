###############################
# This script has various tranfromation needed in fact_transaction
# Ex. cf_Val, fact_holding, fact_capital_flow, etc
###############################
# ---------------------------------------------------------------------------------------------
#                       History
# Date         Version No      Change                                                                     Developer
# -------       ------          ------                                                                     ---------
# 07/07/2021     0.1             WMAUCETG-14520:TIPS security delivery valuation difference                Haribabu.P
#                                p_quantity_val * p_amort_factor_val * (p_price_val * p_qty_factor_val) +
#                                (p_quantity_val*p_accured_int_rt)
# 15-Nov-2021    0.2             Amt flg is null in fact capital flow fee table                            Ajay.K
# 17-Nov-2021    0.3             WMPRPARLEL-265: ADV ROR report sleeve billing breaks AS/FCH               Ajay.K
# 10-Dec-2021    0.4             Added parallel_create_fact_capital_flw method for CI-CO logic swap        Akhil Joe
# 12-Aug-2022    0.5             Added adv_flg changes to create_fact_capital_flw method                   Uday V
# 10-OCT-2022    0.6             Added changes for __derive_advisory_fee_flg method, from now deriving
#                                ADV flg for only accounts and not deriving for combination of             Uday V
#                                accounts & securities.
# ------------------------------------------------------------------------------------------------
from common.custom_processor import MultiCurrencyCustomProcessor, AdjustBackDatedTransCustomProcessor, CancelTransCustomProcessor
from pyspark import StorageLevel
from pyspark.sql.types import DoubleType, DateType, BooleanType
from utils.JobContext import JobContext
from utils.common_derivations_util import get_property_from_dim_tbl, derive_account_type_id_filter_logic, derive_currency_cd, mtdt_sleeve_id_gen, \
    insert_sleeve_stub, pick_latest_from_dtl_instrument
from utils.common_util import transform_boolean_clm, format_date, add_default_clm, add_audit_columns, prepare_exception_clm_list, isNull_check
from utils.data_validations_util import validate_rec_and_filter, NotNullValidator
from utils.s3_operations_util import read_from_s3_catalog_convert_to_df, save_output_to_s3, \
    read_from_catalog_and_filter_inactive_rec
from constant.common_constant import *
from constant.property_constant import *
from pyspark.sql import functions as sf
from pyspark.sql.window import Window
from pyspark.sql.types import *
from decimal import Decimal

C_TODAY_FACT_TRANSACTION_TBL_CLM = ["transaction_type_id","transaction_type_desc","account_id","instrument_id","account_type_id",
                                    "transaction_behavior_id","behavior_desc","source_system_id","source_system_desc","corp_action_decl_id","sleeve_id","trade_dt",
                                    "settle_dt","effective_dt","trade_currency_cd","settle_currency_cd","sor_transaction_ref","account_capital_flow_flg",
                                    "position_capital_flow_flg","capital_flow_algo_id","fee_flg","cf_val","fee_val","spot_exchange_rt","trailer_txt","quantity_val",
                                    "price_val","principal_val","commission_val","misc_expense_val","net_amt_val","amortization_factor_val","accrued_interest_val",
                                    "batch_cd","entry_cd","sec_fee_val","postage_val","other_fee","credit_debit_flg","external_transaction_flg","tax_withheld_amt",
                                    "foreign_tax_withheld_amt","transaction_status_ind","firm_id","active_flg","batch_dt","created_program_nm","created_user_nm",
                                    "created_ts"]

C_REQUIRED_CLM_FOR_FACT_DERIVATION =['transaction_type_id',"account_id",'instrument_id',"effective_dt","currency_cd","account_type_id",
                                     "sleeve_id",'quantity_val', 'net_amt_val', 'cf_val','fee_Val','account_capital_flow_flg', 'position_capital_flow_flg','settle_dt','fee_flg','commission_val','transaction_status_ind']

C_BALANCE_FACT_HOLDING_TRANS_TYPE_ID_EXCLUDE_LIST = [29014,9013,29003,29011,29007,29009,29013,29004,9014,29008,0]

C_FACT_HOLDING_DEFAULT_MAPPING = { "firm_id": C_FIRM_ID, "class_type_id": None}
C_FACT_CAPITAL_FLOW_FEE_DEFAULT_MAPPING = { "firm_id": C_FIRM_ID, "class_type_id": None, "program_ind": None}
# Fact_holding Column list
C_FACT_HOLDING_TBL_CLM = ["account_id","instrument_id","sleeve_id","account_type_id","currency_cd","effective_dt","qty_val_flg",
                          "quantity_val","adv_flg","firm_id","active_flg","batch_dt","created_program_nm","created_user_nm","created_ts"]

# Fact capital flow fee
C_FACT_CAPITAL_FLOW_FEE_TBL_CLM = ["account_id","instrument_id","sleeve_id","currency_cd","account_type_id" ,'class_type_id',"effective_dt","amt_flg",
                                   "amt_val","adv_flg","program_ind","firm_id","active_flg","batch_dt","created_program_nm",
                                   "created_user_nm","created_ts"]

C_TRAN_BHV_ID_FOR_WRK_REPROCESS_CORP = [3800, 3900]
C_TRN_BHV_IDS_COR_ACT_NULL_POS_CF_VAL = [3200, 3210, 3800, 3900]
C_TRN_BHV_IDS_COR_ACT_NULL_NEG_CF_VAL = [3300, 3310]

C_TRN_BHV_IDS_COR_ACT_NOTNULL_PAY_EX_EFF_DT = [3200, 3210]
C_TRN_BHV_IDS_COR_ACT_NOTNULL_EX_PAY_EFF_DT = [3800, 3900]
C_TRN_BHV_IDS_COR_ACT_NOTNULL_EX_PAY_EFF_DT_MIN_1 = [3300, 3310]

C_REQ_REPORT_TYPE_ID_LIST = [10,20,30,40,50,60,70]
C_REQ_TRN_GRP_ID_LIST = [5016,5027,5028]
C_CHILD_CLASS_VALUES_FOR_CAPITAL_FLOW_EXTRA_AMOUT=[C_DIM_CLASS_TYPE_CHILD_CLASS_VAL_COMMITMENT,C_DIM_CLASS_TYPE_CHILD_CLASS_VAL_DISTRIBUTION]

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
C_DIM_SLEEVE_DEFAULT_COL_LIST = {"sor_sleeve_id":C_BASE_SLEEVE_SWP_ACCOUNT_VALUE,"sleeve_nm":C_BASE_SLEEVE_SWP_ACCOUNT_VALUE,
                                 "sleeve_desc": None,"sleeve_type_nm":None,"primary_model_id": None,"money_manager_id": None,
                                 "additional_models_id": None,"firm_id": C_FIRM_ID}
C_DIM_SLEEVE_FIELD_TO_VALIDATE_AGAINST = {"sleeve_id": NotNullValidator(),"sor_sleeve_id": NotNullValidator(),
                                          "sleeve_nm": NotNullValidator(),"sleeve_gf_id": NotNullValidator()}
###############################
# This section is related to Fact holding
###############################

###############################
#  This method will create fact holding records and save them to S3
###############################
#TODO: Rexamine with big files
def create_fact_holding(fact_trans_new_rec_df):
    JobContext.logger.info("Started creating  Fact holding..")

    # Filer the fact transaction records based on transaction_type_id and net_amt_Val and then groupby them to sum the net_amt_val
    JobContext.logger.info("Creating balance fact holding records")
    # removing null and zero value records as this is not going to add any values
    fact_holder_bal_df = fact_trans_new_rec_df.filter(~fact_trans_new_rec_df.transaction_type_id.isin(C_BALANCE_FACT_HOLDING_TRANS_TYPE_ID_EXCLUDE_LIST)) \
        .filter(fact_trans_new_rec_df.net_amt_val.isNotNull()&(fact_trans_new_rec_df.net_amt_val!=0))
    fact_holder_bal_df = fact_holder_bal_df.withColumn("adv_flg",sf.when(sf.col("pace_account_flg"),sf.lit(None))
                                                       .otherwise(sf.col("adv_flg")))
    fact_holder_bal_df = __check_default_sleeve_insert_brdg_acc_rel(fact_holder_bal_df)
    fact_holder_bal_df = fact_holder_bal_df.groupby("account_id", "effective_dt", "currency_cd", "account_type_id", "sleeve_id",'adv_flg').sum('net_amt_val') \
        .withColumnRenamed('sum(net_amt_val)', 'quantity_val').withColumn('qty_val_flg', sf.lit('B')) \
        .withColumn('instrument_id', sf.lit(None))

    JobContext.logger.info("Created balance fact holding records")

    # Filer the fact transaction records based on instrument_id and quantity_val and then groupby them to sum the quantity_val
    JobContext.logger.info("Creating position fact holding records")
    # removing null and zero value records as this is not going to add any values
    fact_holder_pos_df = fact_trans_new_rec_df.filter(fact_trans_new_rec_df.instrument_id.isNotNull()) \
        .filter(fact_trans_new_rec_df.quantity_val.isNotNull()&(fact_trans_new_rec_df.quantity_val!=0))
    fact_holder_pos_df = fact_holder_pos_df.groupby("account_id", "instrument_id", "effective_dt", "currency_cd",  "account_type_id", "sleeve_id", 'adv_flg').sum('quantity_val') \
        .withColumnRenamed('sum(quantity_val)', 'quantity_val').withColumn('qty_val_flg', sf.lit('P'))
    JobContext.logger.info("Created position fact holding records")

    fact_holder_selected_clm = ["account_id", "instrument_id", "effective_dt", "currency_cd", "account_type_id", "sleeve_id", 'quantity_val', 'qty_val_flg','adv_flg']
    JobContext.logger.info("Taking union of both fact holding records")
    fact_holder_df = fact_holder_pos_df.select(*fact_holder_selected_clm).union(  fact_holder_bal_df.select(*fact_holder_selected_clm))
    fact_holder_df = fact_holder_df.filter(fact_holder_df.quantity_val!=0)
    JobContext.logger.info("Union of both fact holding records done")

    fact_holder_df = add_default_clm(fact_holder_df, C_FACT_HOLDING_DEFAULT_MAPPING)
    fact_holder_df.persist()
    fact_holder_df = add_audit_columns(fact_holder_df, JobContext.batch_date_clm)
    save_output_to_s3(fact_holder_df.select(C_FACT_HOLDING_TBL_CLM), "fact_holder_df",   JobContext.get_property(C_S3_FACT_HOLDING_OUTPUT_LOCATION),repartition_count=None)
    JobContext.logger.info("Saved fact_holding ...")

def __check_default_sleeve_insert_brdg_acc_rel(p_df):
    JobContext.logger.info("Checking if sleeve is available in dim_fund_sleeve table")
    l_default_sleeve_id = C_BASE_SLEEVE_SWP_ACCOUNT_VALUE+'-'+str(C_FIRM_ID)

    l_dfs_df =read_from_catalog_and_filter_inactive_rec(JobContext.get_property(C_GLUE_S3_TABLE_DIM_FUND_SLEEVE_PROP_KEY),
                                                        ["sor_sleeve_id","active_flg"])
    l_dfs_df=l_dfs_df.withColumn("sleeve_id",sf.concat(l_dfs_df.sor_sleeve_id,sf.lit('-'+str(C_FIRM_ID)))).distinct()
    p_df = p_df.join(sf.broadcast(l_dfs_df),p_df["sleeve_id"] == l_dfs_df["sleeve_id"],'left') \
        .select(p_df["*"],l_dfs_df["sor_sleeve_id"],sf.split(p_df["account_id"],"-").getItem(0).alias("sor_acc"))
    p_df = p_df.withColumn("sleeve_id",sf.when(sf.col("sor_sleeve_id").isNotNull(),
                                               sf.concat_ws("-","sor_acc",sf.lit(l_default_sleeve_id)))
                           .otherwise(sf.col("sleeve_id")))
    p_df.cache()
    slv_df = p_df.where(sf.col("sor_sleeve_id").isNotNull()) \
        .select("account_id",sf.concat_ws("-","sor_acc",sf.lit(C_BASE_SLEEVE_SWP_ACCOUNT_VALUE)).alias("sor_sleeve_id")).distinct()
    if slv_df.first() is not None:
        insert_sleeve_stub(slv_df)

    p_df = p_df.drop("sor_sleeve_id","sor_acc")
    return p_df


###############################
# This section is related to Fact capital flow
###############################
###############################
#  This method will create fact capital flow records and save them to S3
###############################
def create_fact_capital_flw(fact_trans_new_rec_df, fact_capital_extra_amount_rec = None):
    JobContext.logger.info("Staring Fact capital flow")
    # adding records based on cf_Val, add two new column so that, we can group the record in single operation and then select the not null value as target
    JobContext.logger.info("Creating fact capital flow records using  account_capital_flow_flg")
    fact_cap_flow_cf_Val_df = fact_trans_new_rec_df.withColumn('cf_val_positive', sf.when(sf.col('cf_val') >= 0, sf.col('cf_val')).otherwise(None)) \
        .withColumn('cf_val_negative', sf.when(sf.col('cf_val') < 0, sf.col('cf_val')).otherwise(None)) \
        .withColumn('sign', sf.col('cf_val') < 0)


    # filter based on account_capital_flow_flg and pick only not null cf_Val records
    fact_cap_flow_cf_Val_df.persist()
    fact_cap_flow_capital_flow_flg_df = fact_cap_flow_cf_Val_df.filter('account_capital_flow_flg').filter(fact_trans_new_rec_df.cf_val.isNotNull())
    fact_cap_flow_capital_flow_flg_df = fact_cap_flow_capital_flow_flg_df.withColumn("adv_flg",sf.when(sf.col("pace_account_flg"),sf.lit(None))
                                                                                     .otherwise(sf.col("adv_flg")))
    fact_cap_flow_capital_flow_flg_df = fact_cap_flow_capital_flow_flg_df.groupby("account_id", "effective_dt","currency_cd", "account_type_id", "sleeve_id", 'sign','adv_flg') \
        .sum('cf_val_positive', 'cf_val_negative') \
        .withColumnRenamed('sum(cf_val_positive)', 'cf_val_positive') \
        .withColumnRenamed('sum(cf_val_negative)', 'cf_val_negative') \
        .withColumn('instrument_id', sf.lit(None))
    fact_cap_flow_capital_flow_flg_df.persist()
    JobContext.logger.info("Created fact capital flow records using  account_capital_flow_flg")

    # filter based on account_capital_flow_flg and pick only not null cf_Val records
    JobContext.logger.info("Creating fact capital flow records using  position_capital_flow_flg")
    fact_cap_flow_pos_capital_flow_flg_df = fact_cap_flow_cf_Val_df.filter( fact_trans_new_rec_df.cf_val.isNotNull()) \
        .filter('position_capital_flow_flg'). \
        filter(fact_trans_new_rec_df.instrument_id.isNotNull())
    fact_pace_df = fact_cap_flow_pos_capital_flow_flg_df.where(sf.col("pace_account_flg"))
    fact_cap_flow_pos_capital_flow_flg_df = fact_cap_flow_pos_capital_flow_flg_df.groupby("account_id", "effective_dt",  "currency_cd", "account_type_id", "sleeve_id", 'sign', 'instrument_id','adv_flg') \
        .sum('cf_val_positive', 'cf_val_negative').withColumnRenamed('sum(cf_val_positive)','cf_val_positive') \
        .withColumnRenamed('sum(cf_val_negative)', 'cf_val_negative')

    fact_pace_df = fact_pace_df.groupby("account_id", "effective_dt",  "currency_cd", "account_type_id", "sleeve_id", 'sign', 'instrument_id','adv_flg') \
        .sum('cf_val_positive', 'cf_val_negative') \
        .withColumn('amt_val', sf.when(sf.col('sum(cf_val_positive)').isNotNull(), sf.col('sum(cf_val_positive)'))
                    .otherwise(sf.col('sum(cf_val_negative)'))) \
        .withColumn('amt_flg', sf.when(sf.col('sign'), 'TCO').otherwise('TCI')).withColumn("instrument_id",sf.lit(None))

    fact_cap_flow_pos_capital_flow_flg_df.persist()
    JobContext.logger.info("Created fact capital flow records using  position_capital_flow_flg")

    JobContext.logger.info("Taking union of  fact capital flow records created using  position_capital_flow_flg and account_capital_flow_flg")
    cf_agg_df_clm_list = ["account_id", "effective_dt", "currency_cd", "account_type_id", "sleeve_id","instrument_id", 'sign', "cf_val_positive", "cf_val_negative",'adv_flg']
    fact_capital_flw_all = fact_cap_flow_pos_capital_flow_flg_df.select(*cf_agg_df_clm_list).union(fact_cap_flow_capital_flow_flg_df.select(*cf_agg_df_clm_list))
    fact_capital_flw_all = fact_capital_flw_all.withColumn('amt_val', sf.when(sf.col('cf_val_positive').isNotNull(), sf.col('cf_val_positive'))
                                                           .otherwise(sf.col('cf_val_negative'))) \
        .withColumn('amt_flg', sf.when(sf.col('sign'), 'CO').otherwise('CI')) \
        .drop('cf_val_positive','cf_val_negative', 'sign')
    fct_col_list = fact_capital_flw_all.columns
    fact_capital_flw_all = fact_capital_flw_all.select(*fct_col_list).union(fact_pace_df.select(*fct_col_list))
    fact_capital_flw_all.persist()
    JobContext.logger.info("Union of  fact capital flow records done")
    # Add record based on fee_flg
    # To club two group by we are following approch
    fact_trans_new_rec_df = fact_trans_new_rec_df.withColumn("commission_val", sf.col("commission_val").cast(C_DEFAULT_DECIMAL_TYPE)) \
        .withColumn('commission_val',sf.when(sf.col("commission_val")!=0, sf.col("commission_val")).otherwise(sf.lit(None)))
    fact_trans_new_rec_df = fact_trans_new_rec_df.withColumn("commission_val",sf.when(sf.col('transaction_status_ind') == 'N', -1*sf.col('commission_val')).otherwise(sf.col('commission_val')))
    fact_cap_flow_fee_flg_df_w_cmv = fact_trans_new_rec_df.filter('fee_flg').groupby("account_id", "effective_dt", "currency_cd", "account_type_id", "sleeve_id", 'adv_flg').sum('fee_val',"commission_val")
    cf_agg_df_clm_list = ["account_id", "effective_dt", "currency_cd", "account_type_id", "sleeve_id",  "instrument_id", 'amt_flg', 'amt_Val','adv_flg']


    fact_cap_flow_fee_flg_df = fact_cap_flow_fee_flg_df_w_cmv.withColumnRenamed('sum(fee_val)', 'amt_val').withColumn( 'amt_flg', sf.lit(C_FACT_CAPITAL_FLOW_FEE_AMT_FLG_F)) \
        .withColumn('instrument_id', sf.lit(None)) \
        .select("account_id", "effective_dt", "currency_cd","account_type_id", "sleeve_id", "instrument_id", "amt_val", "amt_flg", "adv_flg")
    JobContext.logger.info("Calculating commission val for position and account level")
    fact_cap_flow_fee_cv_flg_df = fact_trans_new_rec_df.filter(sf.col("commission_val").isNotNull()) \
        .groupby("account_id", "effective_dt", "currency_cd", "account_type_id", "sleeve_id",'adv_flg').sum("commission_val").withColumnRenamed('sum(commission_val)', 'amt_val') \
        .withColumn( 'amt_flg', sf.lit(C_FACT_CAPITAL_FLOW_FEE_AMT_FLG_F)) \
        .withColumn('instrument_id', sf.lit(None)) \
        .select("account_id", "effective_dt", "currency_cd","account_type_id", "sleeve_id", "instrument_id", "amt_val", "amt_flg", "adv_flg")

    fact_cap_flow_fee_flg_cm_instr_df = fact_trans_new_rec_df.filter(sf.col("instrument_id").isNotNull()) \
        .filter(sf.col("commission_val").isNotNull()) \
        .groupby("account_id", "effective_dt", "currency_cd", "account_type_id", "sleeve_id",
                 'adv_flg',"instrument_id").sum("commission_val").withColumnRenamed('sum(commission_val)', 'amt_val') \
        .withColumn('amt_flg', sf.lit(C_FACT_CAPITAL_FLOW_FEE_AMT_FLG_F))
    fact_cap_flow_fee_cv_flg_df = fact_cap_flow_fee_cv_flg_df.select(*cf_agg_df_clm_list) \
        .union(fact_cap_flow_fee_flg_cm_instr_df.select(*cf_agg_df_clm_list))
    fact_cap_flow_fee_cv_flg_df = add_default_clm(fact_cap_flow_fee_cv_flg_df, C_FACT_CAPITAL_FLOW_FEE_DEFAULT_MAPPING)
    fact_cap_flow_fee_cv_flg_df = add_audit_columns(fact_cap_flow_fee_cv_flg_df, JobContext.batch_date_clm,
                                                    created_progrm_nm=C_FACT_CAPITAL_FLOW_FEE_COMMISION_FLG_PRG_NM)

    # [25-Jun-20] Added cap_flow_fee_flg for all accounts_id's and instrument_id's
    fact_cap_flow_fee_flg_instr_df = fact_trans_new_rec_df.filter('fee_flg') \
        .filter(sf.col("instrument_id").isNotNull())

    dfs_df = read_from_catalog_and_filter_inactive_rec(JobContext.get_property(C_GLUE_S3_TABLE_DIM_FUND_SLEEVE_PROP_KEY),
                                                       ["sor_sleeve_id","active_flg"])
    dfs_df = dfs_df.withColumn("dfs_slv_id",sf.concat_ws("-","sor_sleeve_id",sf.lit(C_FIRM_ID)))
    l_src = fact_cap_flow_fee_flg_instr_df.alias("src")
    dfs = sf.broadcast(dfs_df).alias("lkp")
    fact_cap_flow_fee_flg_instr_df = l_src.join(dfs,sf.col("src.sleeve_id") == sf.col("lkp.dfs_slv_id"),"left") \
        .select(fact_cap_flow_fee_flg_instr_df["*"],dfs_df["dfs_slv_id"])
    fee_flg_mf_etf_df = fact_cap_flow_fee_flg_instr_df.filter(sf.col("dfs_slv_id").isNotNull())
    fact_cap_flow_fee_flg_instr_df = fact_cap_flow_fee_flg_instr_df.filter(sf.col("dfs_slv_id").isNull()) \
        .groupby("account_id", "instrument_id", "effective_dt", "currency_cd", "account_type_id", "sleeve_id", 'adv_flg').sum('fee_val')
    fact_cap_flow_fee_flg_instr_df = fact_cap_flow_fee_flg_instr_df.withColumnRenamed('sum(fee_val)', 'amt_val').withColumn( 'amt_flg', sf.lit(C_FACT_CAPITAL_FLOW_FEE_AMT_FLG_F)) \
        .select("account_id", "effective_dt", "currency_cd", "account_type_id", "sleeve_id", "instrument_id", "amt_val", "amt_flg", "adv_flg")
    # [19-May-21] fee_flg_mf_etf_df:WMAUCETG-12861
    fee_flg_mf_etf_df = fee_flg_mf_etf_df.filter(sf.col("dfs_slv_id").isNotNull())
    fee_flg_mf_etf_df = fee_flg_mf_etf_df.withColumn('sign', -1*sf.col('fee_val') < 0)
    fee_flg_mf_etf_df = fee_flg_mf_etf_df.withColumn('fee_val', -1*sf.col('fee_val'))
    fee_flg_mf_etf_df = fee_flg_mf_etf_df.withColumn('amt_flg', sf.when(sf.col('sign'), 'CO').otherwise('CI')) \
        .withColumn("instrument_id",sf.lit(None))
    fee_flg_mf_etf_df = fee_flg_mf_etf_df.filter(sf.col("dfs_slv_id").isNotNull()) \
        .groupby("account_id", "instrument_id", "effective_dt", "currency_cd", "account_type_id", "sleeve_id", 'adv_flg','amt_flg').sum('fee_val')
    fee_flg_mf_etf_df = fee_flg_mf_etf_df.withColumnRenamed('sum(fee_val)', 'amt_val') \
        .select("account_id", "effective_dt", "currency_cd", "account_type_id", "sleeve_id", "instrument_id", "amt_val", "amt_flg", "adv_flg")
    # [07-Jun-21]:fees on sleeves not working-WMAUCETG-13328
    fee_flg_mf_etf_df1  = fee_flg_mf_etf_df.withColumnRenamed('sum(fee_val)', 'amt_val') \
        .select("account_id", "effective_dt", "currency_cd", "account_type_id", "sleeve_id", "instrument_id", "amt_val", "amt_flg", "adv_flg")
    fee_flg_mf_etf_df1 = __check_default_sleeve_insert_brdg_acc_rel1(fee_flg_mf_etf_df1)

    fee_flg_mf_etf_df1 = fee_flg_mf_etf_df1.withColumn('amt_val', -1*sf.col('amt_val'))

    fact_capital_flw_all = fact_capital_flw_all.select(*cf_agg_df_clm_list).union( fact_cap_flow_fee_flg_df.select(*cf_agg_df_clm_list)) \
        .union( fact_cap_flow_fee_flg_instr_df.select(*cf_agg_df_clm_list)).union( fee_flg_mf_etf_df.select(*cf_agg_df_clm_list)).union( fee_flg_mf_etf_df1.select(*cf_agg_df_clm_list))
    fact_capital_flw_all.persist()

    # acount_id not null check not needed as it already done in vaildation of fact transaction
    dim_transaction_grp_df = read_from_catalog_and_filter_inactive_rec(JobContext.get_property(C_GLUE_S3_TABLE_DIM_TRANSACTION_GROUP),
                                                                       ["transaction_type_id","transaction_group_id","report_behavior_id",
                                                                        "active_flg"])
    fact_trans_new_rec_df = fact_trans_new_rec_df.join(sf.broadcast(dim_transaction_grp_df),  dim_transaction_grp_df.transaction_type_id == fact_trans_new_rec_df.transaction_type_id,  'left_outer') \
        .select(fact_trans_new_rec_df['*'], dim_transaction_grp_df.transaction_group_id,
                dim_transaction_grp_df.report_behavior_id)
    fact_trans_new_rec_df.cache()
    rpt_bhv_wise_records_df = fact_trans_new_rec_df

    #TODO: Need to update with proper flag value
    ma_fee_amt_flg = get_property_from_dim_tbl('MA_FEE_AMT_FLG').lower()
    transaction_grp_id_list = [103, 1330]

    if ma_fee_amt_flg in ['yes','y']:
        transaction_grp_id_list.extend([1120, 1121, 1122])

    fact_cap_flw_trn_grp_df = fact_trans_new_rec_df.filter( fact_trans_new_rec_df.transaction_group_id.isin(transaction_grp_id_list))
    fact_cap_flw_trn_grp_df = fact_cap_flw_trn_grp_df.groupby("account_id", "effective_dt", "currency_cd","account_type_id", "sleeve_id",'transaction_group_id','adv_flg') \
        .sum('net_amt_val') \
        .withColumnRenamed('sum(net_amt_val)', 'amt_val')

    fact_cap_flw_trn_grp_df = fact_cap_flw_trn_grp_df.withColumn('amt_flg', sf.when( fact_cap_flw_trn_grp_df.transaction_group_id == 103, 'CIL') \
                                                                 .when(fact_cap_flw_trn_grp_df.transaction_group_id == 1330, 'INT') \
                                                                 .when(fact_cap_flw_trn_grp_df.transaction_group_id == 1120, 'FE') \
                                                                 .when( fact_cap_flw_trn_grp_df.transaction_group_id == 1121, 'FR') \
                                                                 .otherwise(sf.lit('LI'))).withColumn("instrument_id", sf.lit(None))

    fact_cap_flw_trn_grp_df.persist()
    fact_capital_flw_all = fact_capital_flw_all.union(fact_cap_flw_trn_grp_df.select(*cf_agg_df_clm_list))
    # Deriving records based on transaction group 5002 and 5014
    trn_grp_records_df = derive_fiscal_trn_grp_cf_records(rpt_bhv_wise_records_df, cf_agg_df_clm_list)
    #Deriving records based on report behavior id
    rpt_bhv_wise_records_df = derive_report_behavior_wise_cf_records(rpt_bhv_wise_records_df, cf_agg_df_clm_list)
    fact_capital_flw_all = fact_capital_flw_all.select(*cf_agg_df_clm_list).union(rpt_bhv_wise_records_df.select(*cf_agg_df_clm_list)) \
        .union(trn_grp_records_df.select(*cf_agg_df_clm_list))

    if fact_capital_extra_amount_rec is not None:
        l_commitment_data_df = __derive_capital_flw_fee_records_for_extra_amounts(fact_capital_extra_amount_rec)
        fact_capital_flw_all = fact_capital_flw_all.select(*cf_agg_df_clm_list).union(l_commitment_data_df.select(*cf_agg_df_clm_list))
    fact_capital_flw_all.persist()
    fact_capital_flw_all = add_default_clm(fact_capital_flw_all, C_FACT_CAPITAL_FLOW_FEE_DEFAULT_MAPPING)
    fact_capital_flw_all = add_audit_columns(fact_capital_flw_all, JobContext.batch_date_clm)
    fact_capital_flw_all = fact_cap_flow_fee_cv_flg_df.select(C_FACT_CAPITAL_FLOW_FEE_TBL_CLM).union(fact_capital_flw_all.select(C_FACT_CAPITAL_FLOW_FEE_TBL_CLM))

    fact_capital_flw_all = __derive_advisory_fee_flg(fact_capital_flw_all)

    save_output_to_s3(fact_capital_flw_all.select(C_FACT_CAPITAL_FLOW_FEE_TBL_CLM), "fact_holder_df",
                      JobContext.get_property(C_S3_FACT_CAPITAL_FLOW_FEE_OUTPUT_LOCATION),repartition_count=None)
    JobContext.logger.info("Saved fact_capital_flow_fee ...")

def __check_default_sleeve_insert_brdg_acc_rel1(p_df):
    JobContext.logger.info("Checking if sleeve is available in dim_fund_sleeve table")
    l_default_sleeve_id = C_BASE_SLEEVE_SWP_ACCOUNT_VALUE+'-'+str(C_FIRM_ID)

    l_dfs_df =read_from_catalog_and_filter_inactive_rec(JobContext.get_property(C_GLUE_S3_TABLE_DIM_FUND_SLEEVE_PROP_KEY),
                                                        ["sor_sleeve_id","active_flg"])
    l_dfs_df=l_dfs_df.withColumn("sleeve_id",sf.concat(l_dfs_df.sor_sleeve_id,sf.lit('-'+str(C_FIRM_ID)))).distinct()
    p_df = p_df.join(sf.broadcast(l_dfs_df),p_df["sleeve_id"] == l_dfs_df["sleeve_id"],'left') \
        .select(p_df["*"],l_dfs_df["sor_sleeve_id"],sf.split(p_df["account_id"],"-").getItem(0).alias("sor_acc"))
    p_df = p_df.withColumn("sleeve_id",sf.when(sf.col("sor_sleeve_id").isNotNull(),
                                               sf.concat_ws("-","sor_acc",sf.lit(l_default_sleeve_id)))
                           .otherwise(sf.col("sleeve_id")))
    p_df.cache()
    slv_df = p_df.where(sf.col("sor_sleeve_id").isNotNull()) \
        .select("account_id",sf.concat_ws("-","sor_acc",sf.lit(C_BASE_SLEEVE_SWP_ACCOUNT_VALUE)).alias("sor_sleeve_id")).distinct()
    if slv_df.first() is not None:
        insert_sleeve_stub(slv_df)

    p_df = p_df.drop("sor_sleeve_id","sor_acc")
    return p_df


def pick_new_fact_transaction_and_select_fields(fact_transaction_df, p_append_list=None):
    if p_append_list is not None:
        for clm in p_append_list:
            C_REQUIRED_CLM_FOR_FACT_DERIVATION.append(clm)
    fact_trans_new_rec_df = fact_transaction_df.withColumnRenamed('settle_currency_cd', 'currency_cd').select( *C_REQUIRED_CLM_FOR_FACT_DERIVATION)
    fact_trans_new_rec_df = __derive_advisory_flg(fact_trans_new_rec_df).cache()
    return fact_trans_new_rec_df

###############################
#  Amount derivation section
###############################
def amounts_derivations(src_df):
    src_df = src_df.withColumn('quantity_val', sf.when(src_df.security_direction_flg, src_df.share_trans_qty)
                               .when(~src_df.security_direction_flg & src_df.share_trans_qty.isNotNull(), -1 * src_df.share_trans_qty)
                               .otherwise(0)) \
        .withColumn('net_amt_val', sf.when(src_df.money_direction_flg , src_df.tran_total_amt)
                    .when(~src_df.money_direction_flg & src_df.tran_total_amt.isNotNull(),  -1 * src_df.tran_total_amt).otherwise(0)) \
        .withColumn('principal_val', sf.when((src_df.security_direction_flg & src_df.money_direction_flg.isNotNull()), src_df.principal_amt)
                    .when(src_df.principal_amt.isNotNull() &(~src_df.security_direction_flg) & (src_df.money_direction_flg.isNotNull() ), -1 * src_df.principal_amt)
                    .when(((src_df.security_direction_flg.isNull()) & (src_df.money_direction_flg )), src_df.principal_amt)
                    .when(( src_df.principal_amt.isNotNull() &(src_df.security_direction_flg.isNull()) & (~src_df.money_direction_flg )), -1 * src_df.principal_amt)
                    .otherwise(0))

    return src_df

###############################
#  cf_Val derivation secation
###############################

def update_amt_for_cancel_trans(src_df):

    clms = src_df.schema.names
    if "transaction_status_ind" in clms:
        JobContext.logger.info("Source dataframe have transaction_status_ind")
        procesor = CancelTransCustomProcessor()
        if procesor.cancel_trans_processing():
            JobContext.logger.info("Updating amount for cancel transactions")
            src_df = src_df.withColumn("quantity_val", sf.when(sf.col("quantity_val").isNotNull() &
                                                               (sf.upper(sf.col("transaction_status_ind")) == CANCEL_TRANSACTION_STATUS_IND),
                                                               -1 * sf.col("quantity_val"))
                                       .otherwise(sf.col("quantity_val"))) \
                .withColumn("net_amt_val",sf.when(sf.col("net_amt_val").isNotNull()&
                                                  (sf.upper(sf.col("transaction_status_ind")) == CANCEL_TRANSACTION_STATUS_IND), -1 * sf.col("net_amt_val"))
                            .otherwise(sf.col("net_amt_val")))
        else:
            JobContext.logger.info("Amount updating process for cancel transaction is skipped")
    else:
        JobContext.logger.info("Source dataframe does not have transaction_status_ind")
    return src_df

def cf_val_derivation(src_df, fact_set_df, dim_prop_val_df, dim_prop_df, wrk_mv_multi_df, dim_corp_action_decl_df, dtl_corp_action_decl_sec_rate_df,
                      use_corp_dtl_tbl_flg=True, derive_effective_dt_for_wrk= False, multi_currency_derviation_required= False):
    JobContext.logger.info("Starting cf_val derivation...!!!!!!!!!")
    src_df = update_amt_for_cancel_trans(src_df)
    ca_days, ca_flag = __get_ca_prop_values_cf_val(dim_prop_df, dim_prop_val_df, fact_set_df)
    JobContext.logger.info("Derived value for corporate days: {} and corporate flag: {} ".format(ca_days, ca_flag))

    if use_corp_dtl_tbl_flg:
        JobContext.logger.info(" Executing corporate action declaration logic")
        dim_corp_action_decl_df = dim_corp_action_decl_df.withColumn('rnde', sf.row_number()
                                                                     .over(Window.partitionBy("corp_action_decl_id").orderBy(sf.desc("created_ts"))))
        dim_corp_action_decl_df = dim_corp_action_decl_df.filter("rnde = 1").filter("active_flg")



        dtl_corp_action_decl_sec_rate_df = dtl_corp_action_decl_sec_rate_df.withColumn('rnsr', sf.row_number()
                                                                                       .over( Window.partitionBy("corp_action_decl_id", "new_instrument_id").orderBy(sf.desc("created_ts")))) \
            .withColumnRenamed("created_ts","sec_created_ts")

        dtl_corp_action_decl_sec_rate_df = dtl_corp_action_decl_sec_rate_df.filter("rnsr = 1").filter("active_flg")

        JobContext.logger.info("Joining  dtl_corp_action_decl_sec_rate and  dim_corp_action_decl ")
        corp_dtl_df = dtl_corp_action_decl_sec_rate_df.join(sf.broadcast(dim_corp_action_decl_df), ['corp_action_decl_id']).filter("rnde = 1 AND rnsr = 1")
        JobContext.logger.info("Joining  completed dtl_corp_action_decl_sec_rate and  dim_corp_action_decl ")

        corp_dtl_df = format_date(corp_dtl_df, 'pay_dt')

        g_wrk_batch_corp_decl = src_df.select('instrument_id', 'effective_dt').distinct()
        # TODO - compute size
        JobContext.logger.info("Joining  corporate data with wrk_mv_multiplier_pivot ")
        corp_dtl_df = g_wrk_batch_corp_decl.join(sf.broadcast(corp_dtl_df), [corp_dtl_df.new_instrument_id == g_wrk_batch_corp_decl.instrument_id])

        corp_dtl_df = corp_dtl_df.filter(sf.datediff(g_wrk_batch_corp_decl.effective_dt, corp_dtl_df.pay_dt) <= ca_days)

        corp_dtl_df = corp_dtl_df.withColumn('prnsr', sf.row_number().over(Window.partitionBy("new_instrument_id", "corp_action_type_cd")
                                                                           .orderBy(sf.desc("pay_dt"), sf.desc("sec_created_ts"))))

        corp_dtl_df  = corp_dtl_df.filter("prnsr = 1").select(corp_dtl_df['corp_action_decl_id'], corp_dtl_df['pay_dt'],
                                                              corp_dtl_df['ex_dt'],corp_dtl_df['open_price'], corp_dtl_df['corp_action_type_cd'], corp_dtl_df['new_instrument_id']).distinct()
        corp_dtl_df.persist(StorageLevel.MEMORY_ONLY)
        JobContext.logger.info("Joining  completed data with wrk_mv_multiplier_pivot ")

        JobContext.logger.info("Joining  corporate data with source data ")
        cond =  [src_df.instrument_id == corp_dtl_df.new_instrument_id, src_df.effective_dt >= corp_dtl_df.pay_dt,
                 corp_dtl_df.corp_action_type_cd ==src_df.corp_action_type_cd ]
        src_df = src_df.join(sf.broadcast(corp_dtl_df), cond, 'left').select(src_df['*'], corp_dtl_df['corp_action_decl_id'],
                                                                             corp_dtl_df['pay_dt'], corp_dtl_df['ex_dt'],
                                                                             corp_dtl_df['open_price'], corp_dtl_df['corp_action_type_cd'])
        corp_act_flg= 'N' if 'yes' in str(ca_flag).lower() else 'Y'
        #TODO: Remove it[V.S]
        # src_df.persist(StorageLevel.MEMORY_ONLY)
        # import threading
        #
        # f1_write = threading.Thread(target=save_wrk_reprocess_corp, args=(corp_act_flg,src_df,JobContext.glue_context.spark_session._sc,"1",))
        # f1_write.start()
        src_df.cache()
        save_wrk_reprocess_corp(corp_act_flg, src_df)
        if derive_effective_dt_for_wrk:
            src_df= __derive_effective_dt(src_df, corp_act_flg)
        else:
            src_df = src_df.withColumn("fact_mv_effective_dt",sf.col('effective_dt'))

    else:
        JobContext.logger.info("Corporate action declaration skipped")
        src_df = src_df.withColumn("open_price",sf.lit(None)).withColumn("corp_action_decl_id",sf.lit(None)).withColumn("fact_mv_effective_dt",sf.col('effective_dt'))

    src_df = __derive_clm_using_wrk_mv_multi(src_df, wrk_mv_multi_df)
    if multi_currency_derviation_required:
        src_df = __derive_multi_currency_conv_rt_for_mutliplier_vals(src_df)

    # setting default values
    src_df = __set_default_values_cf_val(src_df)
    JobContext.logger.info("Joining  completed corporate data with source data ")
    src_df = __format_column_cf_val(src_df)

    def __get_capital_flow_value_internal(p_capital_flow_algo_id, p_net_amt_val, p_principal_val, p_price_val, p_quantity_val, p_amort_factor_val, p_qty_factor_val,
                                          p_accured_int_rt, p_transaction_behavior_id, p_open_price, p_corp_act_flg=ca_flag):

        return __get_capital_flow_value(p_capital_flow_algo_id, p_net_amt_val, p_principal_val, p_price_val, p_quantity_val, p_amort_factor_val, p_qty_factor_val, p_accured_int_rt,
                                        p_transaction_behavior_id, p_open_price, p_corp_act_flg)

    g_udf_capital_flow_value = sf.udf(__get_capital_flow_value_internal, C_DEFAULT_DECIMAL_TYPE)

    src_df = __add_cf_val_clm(g_udf_capital_flow_value, src_df)

    return src_df

def __derive_effective_dt(src_df,corp_act_flg):
    JobContext.logger.info("Deriving effective_dt")
    src_df= src_df.withColumn('ex_pay_eff_dt',sf.when(sf.col('ex_dt').isNotNull(), sf.col('ex_dt')).
                              when(sf.col('pay_dt').isNotNull(), sf.col('pay_dt'))
                              .otherwise(sf.col('processing_dt')))
    src_df = src_df.withColumn('pay_ex_eff_dt',sf.when(sf.col('pay_dt').isNotNull(), sf.col('pay_dt')).
                               when(sf.col('ex_dt').isNotNull(), sf.col('ex_dt'))
                               .otherwise(sf.col('processing_dt')))
    src_df = format_date(src_df,'pay_ex_eff_dt')
    src_df = format_date(src_df,'processing_dt')
    # TODO: We are putting default effective_dt to as previous, we need Hari's confirmation for it
    src_df = src_df.withColumn('fact_mv_effective_dt',
                               sf.when((sf.lit(corp_act_flg)=='N') | (sf.col('capital_flow_algo_id')!=5), sf.col('processing_dt'))
                               .when(sf.col('corp_action_decl_id').isNull() & (sf.col("transaction_behavior_id").isin(C_TRN_BHV_IDS_COR_ACT_NULL_POS_CF_VAL)), sf.col('processing_dt'))
                               .when(sf.col('corp_action_decl_id').isNull() & (sf.col("transaction_behavior_id").isin(C_TRN_BHV_IDS_COR_ACT_NULL_NEG_CF_VAL)), sf.date_add('processing_dt', -1))
                               .when(sf.col('corp_action_decl_id').isNotNull() & (sf.col("transaction_behavior_id").isin(C_TRN_BHV_IDS_COR_ACT_NOTNULL_PAY_EX_EFF_DT)), sf.col('pay_ex_eff_dt'))
                               .when(sf.col('corp_action_decl_id').isNotNull() & (sf.col("transaction_behavior_id").isin(C_TRN_BHV_IDS_COR_ACT_NOTNULL_EX_PAY_EFF_DT)), sf.col('ex_pay_eff_dt'))
                               .when(sf.col('corp_action_decl_id').isNotNull() & (sf.col("transaction_behavior_id").isin(C_TRN_BHV_IDS_COR_ACT_NOTNULL_EX_PAY_EFF_DT_MIN_1)), sf.date_add('ex_pay_eff_dt', -1))
                               .otherwise(sf.col("effective_dt")))
    JobContext.logger.info("effective_dt is derived")
    return src_df


# def save_wrk_reprocess_corp(corp_act_flg, src_df,sc,num):
def save_wrk_reprocess_corp(corp_act_flg, src_df):
    # TODO comment and check reduced time
    JobContext.logger.info("Preparing wrk_reprocess_corp records..")
    src_df.filter((sf.col('capital_flow_algo_id') == 5) & sf.col('corp_action_decl_id').isNotNull() &
                  ((sf.lit(corp_act_flg) == 'Y') & sf.col("transaction_behavior_id").isin(
                      C_TRAN_BHV_ID_FOR_WRK_REPROCESS_CORP) | (sf.lit(corp_act_flg) == 'N')))
    wrk_reprocess_corp_df = src_df.filter(sf.col('corp_action_decl_id').isNotNull()).select('corp_action_decl_id', 'account_id').distinct()
    drop_clm = ['active_flg','batch_dt']
    wrk_reprocess_corp_df = add_audit_columns(wrk_reprocess_corp_df,JobContext.batch_date_clm).drop(*drop_clm)
    save_output_to_s3(wrk_reprocess_corp_df, 'wrk_reprocess_corp_df',
                      JobContext.get_property(C_S3_WRK_REPROCESS_CORP_OUTPUT_LOCATION))


def __add_cf_val_clm(g_udf_capital_flow_value, src_df):
    src_df = src_df.withColumn('cf_val',
                               g_udf_capital_flow_value('capital_flow_algo_id', 'net_amt_val', 'principal_val',
                                                        'price_val_mv', 'quantity_val', 'amort_factor_val',
                                                        'qty_factor_val', 'accured_int_rt', 'transaction_behavior_id',
                                                        'open_price'))
    return src_df


def __format_column_cf_val(src_df):
    src_df = src_df.withColumn('principal_val', sf.col('principal_val').cast(C_DEFAULT_DECIMAL_TYPE)) \
        .withColumn('net_amt_val',sf.col('net_amt_val').cast(C_DEFAULT_DECIMAL_TYPE)) \
        .withColumn('price_val_mv', sf.col('price_val_mv').cast(C_DEFAULT_DECIMAL_TYPE)) \
        .withColumn('quantity_val', sf.col('quantity_val').cast(C_DEFAULT_DECIMAL_TYPE)) \
        .withColumn('open_price',sf.col('open_price').cast(C_DEFAULT_DECIMAL_TYPE))
    return src_df


def __set_default_values_cf_val(src_df):
    src_df = src_df.withColumn('amort_factor_val', sf.when(src_df.amort_factor_val.isNull(), 1)
                               .when(src_df.amort_factor_val == '', 1)
                               .otherwise(src_df.amort_factor_val).cast(C_DEFAULT_DECIMAL_TYPE)) \
        .withColumn('accured_int_rt', sf.when(src_df.accured_int_rt.isNull(), 0)
                    .when(src_df.accured_int_rt == '', 0)
                    .otherwise(src_df.accured_int_rt).cast(C_DEFAULT_DECIMAL_TYPE)) \
        .withColumn('qty_factor_val', sf.when(src_df.qty_factor_val.isNull(), 1)
                    .when(src_df.qty_factor_val == '', 1)
                    .otherwise(src_df.qty_factor_val).cast(C_DEFAULT_DECIMAL_TYPE))
    return src_df


def __derive_clm_using_wrk_mv_multi(src_df, wrk_mv_multi_df):
    src_df = src_df.join(wrk_mv_multi_df, [src_df.fact_mv_effective_dt == wrk_mv_multi_df.effective_dt, src_df.instrument_id == wrk_mv_multi_df.instrument_id], 'left').select(
        src_df['*'], wrk_mv_multi_df.pr.alias('price_val_mv'), wrk_mv_multi_df.ai.alias('accured_int_rt'), wrk_mv_multi_df.af.alias('amort_factor_val'),
        wrk_mv_multi_df.qf.alias('qty_factor_val'),wrk_mv_multi_df.pr_currency_cd,wrk_mv_multi_df.effective_dt.alias('mv_effective_dt_for_curr'))


    number_of_partition = src_df.rdd.getNumPartitions()
    new_partition = int(number_of_partition/2)

    # if its very small file, new parition would be 0
    if new_partition==0:
        new_partition =1

    JobContext.logger.info("Current number of partition is {}, reducing it to {} ".format(number_of_partition, new_partition))

    src_df  = src_df.repartition(new_partition)

    return src_df


def __get_ca_prop_values_cf_val(dim_prop_df, dim_prop_val_df, fact_set_df):
    prop_val_df = fact_set_df.alias('fs').join(sf.broadcast(dim_prop_val_df).alias('dpv'), 'property_value_id').join(dim_prop_df.alias('dp'), 'property_id')
    prop_val_df.cache()
    g_ca_flag = prop_val_df.filter("property_desc = 'EXCH_DPDNCY' ").select(sf.concat('property_value_desc').alias('exch_dep')).first().asDict()['exch_dep']
    g_ca_days = prop_val_df.filter("dp.property_desc = 'CORP_ACTION_DAYS' AND dp.active_flg = 't'").select('property_value_desc').first().asDict()['property_value_desc']
    return g_ca_days, g_ca_flag


def __get_capital_flow_value(p_capital_flow_algo_id, p_net_amt_val, p_principal_val, p_price_val, p_quantity_val, p_amort_factor_val, p_qty_factor_val, p_accured_int_rt,
                             p_transaction_behavior_id, p_open_price, p_corp_act_flg):
    try:
        if p_amort_factor_val is None:
            p_amort_factor_val = 1
        if p_qty_factor_val is None:
            p_qty_factor_val = 1
        if p_accured_int_rt is None:
            p_accured_int_rt = 0
        if p_quantity_val is None:
            p_quantity_val = 0
        if p_capital_flow_algo_id == 2 or p_capital_flow_algo_id == 4:
            return -1 * p_net_amt_val
        elif p_capital_flow_algo_id == 1:
            return p_net_amt_val
        elif p_capital_flow_algo_id == 3:
            if p_net_amt_val is not None and p_net_amt_val != 0:
                return p_net_amt_val
            elif p_principal_val is not None and p_principal_val != 0:
                return p_principal_val
            else:
                return p_quantity_val * p_amort_factor_val * (p_price_val * p_qty_factor_val) + (p_quantity_val*p_accured_int_rt)
        #PRU missing Corp Action Declarations spin-offs are not creating the Cash flows--WMAUCETG-13418[Jun-09-2021]
        elif p_capital_flow_algo_id == 5 and p_transaction_behavior_id in (3800, 3900):
            if p_open_price is None:
                return p_quantity_val * p_amort_factor_val * (p_price_val * p_qty_factor_val) + (p_quantity_val*p_accured_int_rt)
            else:
                return p_quantity_val * p_open_price
        elif p_capital_flow_algo_id == 5:
            if 'NO' in p_corp_act_flg:
                if p_transaction_behavior_id in [3300, 3310]:
                    return p_quantity_val * p_amort_factor_val * (p_price_val * p_qty_factor_val) + (p_quantity_val*p_accured_int_rt)
                elif p_transaction_behavior_id in [3200, 3210]:
                    if p_open_price is None:
                        return p_quantity_val * p_amort_factor_val * (p_price_val * p_qty_factor_val) + (p_quantity_val*p_accured_int_rt)
                    else:
                        return p_quantity_val * p_open_price
            elif 'YES' in p_corp_act_flg:
                if p_transaction_behavior_id in (3200, 3210):
                    if p_net_amt_val is not None and p_net_amt_val != 0:
                        return p_net_amt_val
                    elif p_principal_val is not None and p_principal_val != 0:
                        return p_principal_val
                    else:
                        return p_quantity_val * p_amort_factor_val * (p_price_val * p_qty_factor_val) + (p_quantity_val*p_accured_int_rt)
                if p_transaction_behavior_id in (3300, 3310):
                    if p_net_amt_val is not None and p_net_amt_val != 0:
                        return -1 * p_net_amt_val
                    else:

                        return p_quantity_val * p_amort_factor_val * (p_price_val * p_qty_factor_val) + (p_quantity_val*p_accured_int_rt)
    except Exception:
        print("cf_val calculation failed with error ")
    return None

def derive_transaction_type_desc(fact_transaction_df):
    JobContext.logger.info("Deriving  transaction type desc")
    g_req_col_list = ["transaction_type_id","transaction_type_desc","transaction_behavior_id","created_ts","active_flg","corp_action_type_cd"]
    dim_transaction_type_df = read_from_catalog_and_filter_inactive_rec(JobContext.get_property(C_GLUE_S3_TABLE_DIM_TRANSACTION_TYPE_PROP_KEY)
                                                                        ,p_fields_to_select=g_req_col_list)
    # There could be multiple record with same transaction_type_id, so picking the latest record
    g_windowSpec= Window.partitionBy(dim_transaction_type_df['transaction_type_id']) \
        .orderBy(dim_transaction_type_df['created_ts'].desc())
    dim_transaction_type_df = dim_transaction_type_df.withColumn("num",sf.row_number().over(g_windowSpec))
    dim_transaction_type_df = dim_transaction_type_df.where(dim_transaction_type_df["num"]==1).distinct()

    # Joining based on trnasction_type_id and slecting transaction_type_desc, transaction_behavior_id(this will be used to join with dtl_transaction_behaviour)
    fact_transaction_df = fact_transaction_df.join(sf.broadcast(dim_transaction_type_df), fact_transaction_df.transaction_type_id == dim_transaction_type_df.transaction_type_id,
                                                   "left_outer").select(fact_transaction_df['*'], dim_transaction_type_df.transaction_type_desc,
                                                                        dim_transaction_type_df.corp_action_type_cd,
                                                                        dim_transaction_type_df.transaction_behavior_id.alias('transaction_behavior_id_lookup'))
    JobContext.logger.info("Derived  transaction type desc")
    return fact_transaction_df

###############################
#  This method will join source transaction with dtl_trnsaction_behaviour on transaction_behavior_id, and select various columns,
#  it also format boolean column(from dim table its t or 'T'), it convert into True
###############################
def derive_from_dtl_transaction_behavior_tbl(fact_transaction_df):
    JobContext.logger.info("Deriving various column using dtl_transaction_behavior table ")
    dtl_transaction_behaviour_df = read_from_catalog_and_filter_inactive_rec(JobContext.get_property(C_GLUE_S3_TABLE_DTL_TRANSACTION_BEHAVIOR_PROP_KEY))
    fact_transaction_df = fact_transaction_df.join(sf.broadcast(dtl_transaction_behaviour_df),
                                                   fact_transaction_df.transaction_behavior_id_lookup == dtl_transaction_behaviour_df.transaction_behavior_id,
                                                   "left_outer").select(fact_transaction_df['*'], dtl_transaction_behaviour_df.behavior_desc,
                                                                        dtl_transaction_behaviour_df.account_capital_flow_flg,
                                                                        dtl_transaction_behaviour_df.position_capital_flow_flg,
                                                                        dtl_transaction_behaviour_df.capital_flow_algo_id.cast(ShortType()), dtl_transaction_behaviour_df.fee_flg,
                                                                        dtl_transaction_behaviour_df.transaction_behavior_id,
                                                                        dtl_transaction_behaviour_df.quantity_direction_flg.alias('security_direction_flg'),
                                                                        dtl_transaction_behaviour_df.money_direction_flg)

    fact_transaction_df = transform_boolean_clm(fact_transaction_df, "security_direction_flg")
    fact_transaction_df = transform_boolean_clm(fact_transaction_df, "money_direction_flg")
    fact_transaction_df = transform_boolean_clm(fact_transaction_df, "position_capital_flow_flg")
    fact_transaction_df = transform_boolean_clm(fact_transaction_df, "account_capital_flow_flg")
    fact_transaction_df = transform_boolean_clm(fact_transaction_df, "fee_flg")
    JobContext.logger.info("Derived  various column using  dtl_transaction_behavior table ")
    return fact_transaction_df

def __derive_capital_flw_fee_records_for_extra_amounts(p_transaction_df):
    JobContext.logger.info("Computing capital extra amounts {}, for fact_capital_flow_fee ".format(str(C_CHILD_CLASS_VALUES_FOR_CAPITAL_FLOW_EXTRA_AMOUT)))
    p_transaction_df = p_transaction_df.filter(p_transaction_df.cf_val.isNotNull()) \
        .filter('position_capital_flow_flg') \
        .filter(p_transaction_df.instrument_id.isNotNull())
    p_transaction_df = p_transaction_df.groupby("account_id", "effective_dt",  "currency_cd", "account_type_id","sleeve_id", 'instrument_id','adv_flg','child_class_val') \
        .sum("cf_val").withColumnRenamed("sum(cf_val)","amt_val")
    p_transaction_df = p_transaction_df.withColumn("amt_flg",sf.when((sf.col("child_class_val")==C_DIM_CLASS_TYPE_CHILD_CLASS_VAL_COMMITMENT),sf.lit(C_FACT_CAPITAL_FLOW_FEE_AMT_FLG_FOR_COMMITMENT))
                                                   .otherwise(sf.lit(C_FACT_CAPITAL_FLOW_FEE_AMT_FLG_FOR_DISTRIBUTION)))
    return p_transaction_df

def get_class_type_val(p_transaction_df):
    l_dim_class_type_df = read_from_s3_catalog_convert_to_df(JobContext.get_property(C_GLUE_S3_DB_NAME_PROP_KEY),
                                                             JobContext.get_property(C_GLUE_S3_TABLE_DIM_CLASS_TYPE),
                                                             ["class_type_id","child_class_type_cd","child_class_val",
                                                              "active_flg"])
    #todo validate 603-618 - save and check the output as well
    l_dim_class_type_df = transform_boolean_clm(l_dim_class_type_df,"active_flg")

    l_fact_class_entity_df = read_from_s3_catalog_convert_to_df(JobContext.get_property(C_GLUE_S3_DB_NAME_PROP_KEY),
                                                                JobContext.get_property(C_S3_FACT_CLASS_ENTITY_TBL_NME))
    g_window_spec = Window.partitionBy( l_fact_class_entity_df['entity_id'],
                                        l_fact_class_entity_df['effective_from_dt']) \
        .orderBy( l_fact_class_entity_df['created_ts'].desc())

    l_lookup_data_df = l_fact_class_entity_df.alias("a").join(sf.broadcast(l_dim_class_type_df).alias("b"),["class_type_id"]) \
        .where((l_fact_class_entity_df["entity_type_cd"] == "SECURITY")
               & (l_dim_class_type_df["child_class_type_cd"] =='AIP_SEC_TYPE')
               & (l_dim_class_type_df["active_flg"])) \
        .select("a.entity_id","b.child_class_val","a.class_type_id",
                "a.created_ts","a.effective_from_dt","a.effective_to_dt")

    l_lookup_data_df = l_lookup_data_df.withColumn("row_num",sf.row_number().over(g_window_spec))
    l_lookup_data_df = l_lookup_data_df.where(l_lookup_data_df["row_num"] == 1)

    p_transaction_df = p_transaction_df.join(sf.broadcast(l_lookup_data_df),
                                             (p_transaction_df["instrument_id"]==l_lookup_data_df["entity_id"])
                                             & (p_transaction_df["settle_dt"].between(l_lookup_data_df["effective_from_dt"],
                                                                                      l_lookup_data_df["effective_to_dt"])),
                                             "left").select(p_transaction_df["*"],l_lookup_data_df["child_class_val"])

    return p_transaction_df

def get_records_for_fact_holding(p_transaction_df):
    return p_transaction_df.where((~p_transaction_df["child_class_val"].isin(["DISTRIBUTION","COMMITMENT"]))
                                  | (p_transaction_df["child_class_val"].isNull())
                                  | (p_transaction_df["child_class_val"]==""))


def derive_transaction_type_id( src_transaction_df, p_filter_cond=None, append_zero_insor_trn = False):
    JobContext.logger.info("Deriving transaction type id")
    g_req_col_list = ["sor_tran_type_cd","transaction_type_id","created_ts","active_flg","created_program_nm"]
    dim_sor_transaction_type_df = read_from_catalog_and_filter_inactive_rec(JobContext.get_property(C_GLUE_S3_TABLE_DIM_SOR_TRANSACTION_TYPE_PROP_KEY)
                                                                            ,p_fields_to_select=g_req_col_list)
    if(p_filter_cond is not None):
        dim_sor_transaction_type_df = dim_sor_transaction_type_df.where(p_filter_cond)
    if append_zero_insor_trn:
        JobContext.logger.info("Appending zero in sor_tran_type_cd to match it with soruce")
        dim_sor_transaction_type_df = dim_sor_transaction_type_df.withColumn("sor_tran_type_cd", sf.when(sf.length("sor_tran_type_cd")==1,
                                                                                                         sf.concat(sf.lit("0"),"sor_tran_type_cd")).otherwise(sf.col("sor_tran_type_cd")))
    g_windowSpec= Window.partitionBy(dim_sor_transaction_type_df['sor_tran_type_cd']) \
        .orderBy(dim_sor_transaction_type_df['created_ts'].desc())
    dim_sor_transaction_type_df = dim_sor_transaction_type_df.withColumn("num",sf.row_number().over(g_windowSpec))
    dim_sor_transaction_type_df = dim_sor_transaction_type_df.where(dim_sor_transaction_type_df["num"]==1).distinct()


    src_transaction_df = src_transaction_df.join(sf.broadcast(dim_sor_transaction_type_df), src_transaction_df.transaction_cd == dim_sor_transaction_type_df.sor_tran_type_cd,
                                                 "left_outer").select(src_transaction_df['*'], dim_sor_transaction_type_df.transaction_type_id)
    JobContext.logger.info("Derived  transaction type id")
    return src_transaction_df

C_TIME_STAMP_FOR_SOR_TRN_REF= "time_stamp_for_sor_transaction_ref"
def add_clm_curr_time_in_nonsecond(src_transaction_df, clm_name=C_TIME_STAMP_FOR_SOR_TRN_REF):
    # first converting it to double then decimal to remove scientific notation then removing dot from digit
    # Ex: lets consider created_ts = '2019-08-06 10:32:28.535359'
    # then after converting it to double it would be '1.565087548535359E9'
    # and we convert it to decimal '1565087548.535359' and then removes dot to make it like '1565087548535359'
    return src_transaction_df.withColumn(clm_name, sf.regexp_replace(sf.col('created_ts').cast('double').cast(DecimalType(20, 6)), '\.', ''))


# This method will read all required dim table for cf_Val calculation and calculate cf_val and fee_val
# It also accept valid_capital_flow_algo_ids_fee_flg_to_cal as param, it should be capital_flow_algo id for which we need to calculate fee_Val
def derive_cf_val_fee_val(src_transaction_df, valid_capital_flow_algo_ids_fee_flg_to_cal):
    #todo select req cols
    fact_setting_df = read_from_catalog_and_filter_inactive_rec(JobContext.get_property(C_GLUE_S3_TABLE_FACT_SETTING_PROP_KEY))
    dim_prop_val_df = read_from_catalog_and_filter_inactive_rec(JobContext.get_property(C_GLUE_S3_TABLE_DIM_PROPERTY_VALUE_PROP_KEY))
    dim_prop_df = read_from_catalog_and_filter_inactive_rec(JobContext.get_property(C_GLUE_S3_TABLE_DIM_PROPERTY_POP_KEY))
    wrk_mv_multi_df = read_from_s3_catalog_convert_to_df(JobContext.get_property(C_GLUE_S3_DB_NAME_PROP_KEY), JobContext.get_property(C_GLUE_S3_TABLE_WRK_MV_MULTIPLIER_PIVOT))
    dim_corp_action_decl_df = read_from_catalog_and_filter_inactive_rec(JobContext.get_property(C_GLUE_S3_TABLE_DIM_CORP_ACTION_DEC_PROP_KEY))
    dtl_corp_action_decl_sec_rate_df = read_from_catalog_and_filter_inactive_rec(JobContext.get_property(C_GLUE_S3_TABLE_DTL_CORP_ACTION_DECL_SEC_RATE_PROP_KEY))
    src_transaction_df = src_transaction_df.withColumn("transaction_dt", src_transaction_df.effective_dt)
    src_transaction_df = cf_val_derivation(src_df=src_transaction_df, dim_prop_val_df=dim_prop_val_df, dim_prop_df=dim_prop_df,
                                           dim_corp_action_decl_df=dim_corp_action_decl_df, fact_set_df=fact_setting_df, wrk_mv_multi_df=wrk_mv_multi_df,
                                           dtl_corp_action_decl_sec_rate_df=dtl_corp_action_decl_sec_rate_df)
    src_transaction_df = derive_fee_val(src_transaction_df,valid_capital_flow_algo_ids_fee_flg_to_cal)
    src_transaction_df.persist(StorageLevel.MEMORY_ONLY)
    return src_transaction_df


def derive_fee_val( fact_transaction_df,valid_capital_flow_algo_ids_fee_flg_to_cal):
    fact_transaction_df = fact_transaction_df.withColumn('fee_val', sf.when(fact_transaction_df.capital_flow_algo_id.isin(valid_capital_flow_algo_ids_fee_flg_to_cal) ,
                                                                            fact_transaction_df.cf_val).otherwise(None))
    return fact_transaction_df

def derive_account_type_id(src_df, derived_sor_fund_id_clm_nm="sor_fund_id",dim_account_type_id_clm_name_alias="account_type_id_lookup"):
    JobContext.logger.info("Deriving account type id using dim_sor_ref_fund")
    g_dim_sor_ref_fund_df = read_from_catalog_and_filter_inactive_rec(JobContext.get_property(C_GLUE_S3_TABLE_DIM_SOR_FUND_REF_PROP_KEY))
    g_dim_sor_ref_fund_df = g_dim_sor_ref_fund_df.select("sor_fund_id","account_type_id").distinct()

    src_df = src_df.withColumn(derived_sor_fund_id_clm_nm, sf.concat(sf.col('currency_cd'), sf.lit('_'), sf.col('type_account_cd')))
    src_df = src_df.join(sf.broadcast(g_dim_sor_ref_fund_df), g_dim_sor_ref_fund_df.sor_fund_id == src_df[derived_sor_fund_id_clm_nm], 'left') \
        .select(src_df['*'], g_dim_sor_ref_fund_df.account_type_id)

    g_dim_acc_typ_df = read_from_catalog_and_filter_inactive_rec(JobContext.get_property(C_GLUE_S3_TABLE_DIM_ACCOUNT_TYPE_PROP_KEY))

    joining_condition = g_dim_acc_typ_df.account_type_id == src_df.account_type_id

    src_df = derive_account_type_id_filter_logic(p_src_df=src_df, p_account_type_df=g_dim_acc_typ_df, joining_condition=joining_condition,dim_account_type_id_clm_name_alias = dim_account_type_id_clm_name_alias)
    JobContext.logger.info("Derived account type id using dim_sor_ref_fund")
    return src_df

def derive_report_behavior_wise_cf_records(p_transaction_df, p_col_list):
    JobContext.logger.info("Deriving Fact_captial_flow_fee records based on report behavior")
    p_transaction_df = p_transaction_df.where(sf.col("report_behavior_id").isin(C_REQ_REPORT_TYPE_ID_LIST))
    l_acc_df = p_transaction_df.where(sf.col('account_capital_flow_flg') & sf.col('net_amt_val').isNotNull())
    l_acc_df = l_acc_df.withColumn("amt_flg",sf.when(sf.col('report_behavior_id')==10,sf.lit('DEP_INC'))
                                   .when(sf.col('report_behavior_id')==20,sf.lit('WTH_INC'))
                                   .when(sf.col('report_behavior_id')==30,sf.lit('DIV_INC'))
                                   .when(sf.col('report_behavior_id')==40,sf.lit('INT_INC'))
                                   .when(sf.col('report_behavior_id')==50,sf.lit('MNG_FEE'))
                                   .when(sf.col('report_behavior_id')==60,sf.when(sf.col("net_amt_val")>0,sf.lit('INV_PRC'))
                                         .otherwise(sf.lit('INV_WTH')))
                                   .when(sf.col('report_behavior_id')==70,sf.when(sf.col("net_amt_val")>0,sf.lit('ANU_DEP'))
                                         .otherwise(sf.lit('ANU_WTH')))
                                   .otherwise(sf.lit('OTH_INC')))
    l_acc_df = l_acc_df.groupby("account_id","sleeve_id","account_type_id","currency_cd","effective_dt","amt_flg",'adv_flg') \
        .sum('net_amt_val').withColumnRenamed('sum(net_amt_val)','amt_val') \
        .withColumn("instrument_id", sf.lit(None))

    l_pos_df = p_transaction_df.where(sf.col('position_capital_flow_flg') & sf.col('net_amt_val').isNotNull()
                                      & sf.col('instrument_id').isNotNull())
    l_pos_df = l_pos_df.withColumn("amt_flg",sf.when(sf.col('report_behavior_id')==10,sf.lit('DEP_INC'))
                                   .when(sf.col('report_behavior_id')==20,sf.lit('WTH_INC'))
                                   .when(sf.col('report_behavior_id')==30,sf.lit('DIV_INC'))
                                   .when(sf.col('report_behavior_id')==40,sf.lit('INT_INC'))
                                   .when(sf.col('report_behavior_id')==50,sf.lit('MNG_FEE'))
                                   .when(sf.col('report_behavior_id')==60,sf.when(sf.col("net_amt_val")>0,sf.lit('INV_PRC'))
                                         .otherwise(sf.lit('INV_WTH')))
                                   .when(sf.col('report_behavior_id')==70,sf.when(sf.col("net_amt_val")>0,sf.lit('ANU_DEP'))
                                         .otherwise(sf.lit('ANU_WTH')))
                                   .otherwise(sf.lit('OTH_INC')))
    l_pos_df = l_pos_df.groupby("account_id","instrument_id","sleeve_id","account_type_id","currency_cd","effective_dt","amt_flg", 'adv_flg') \
        .sum("net_amt_val").withColumnRenamed('sum(net_amt_val)','amt_val')
    l_all_records_df = l_acc_df.select(*p_col_list).union(l_pos_df.select(*p_col_list))

    JobContext.logger.info("Derived Fact_captial_flow_fee records based on report behavior")
    return l_all_records_df

def prepare_record_for_Fact_transaction(g_fact_transaction_df, update_sor_transaction_ref=False):
    # TODO: Due to cancel transaction change, for UBS client, all transaction should be part of fact_transaction data. We need to update code to handle it in generic way.
    # g_fact_transaction_df = g_fact_transaction_df.filter("transaction_status_ind='N'")

    if update_sor_transaction_ref:
        JobContext.logger.info("Appending timestamp in sor_transaction_Ref")
        g_fact_transaction_df = g_fact_transaction_df.withColumn("time_stamp_for_sor_transaction_ref",
                                                                 sf.regexp_replace(sf.col('created_ts').cast(TimestampType()).cast('double').cast(DecimalType(20, 6)), '\.', ''))
        g_fact_transaction_df = g_fact_transaction_df.withColumn('sor_transaction_ref',
                                                                 sf.concat_ws("_", g_fact_transaction_df.sor_transaction_ref, g_fact_transaction_df.time_stamp_for_sor_transaction_ref)) \
            .drop("time_stamp_for_sor_transaction_ref")

    g_fact_transaction_df = g_fact_transaction_df.withColumn("cf_val", sf.when(
        sf.col("account_capital_flow_flg") | sf.col("position_capital_flow_flg"), sf.col("cf_val")).otherwise(None)) \
        .withColumn("fee_val", sf.when(sf.col("fee_flg"), sf.col("fee_val")).otherwise(None))
    return g_fact_transaction_df

def __derive_multi_currency_conv_rt_for_mutliplier_vals(src_df):
    JobContext.logger.info("Multi currency based derivation starts")

    currency_logic_obj = MultiCurrencyCustomProcessor()
    execute_multi_currency_flg = currency_logic_obj.execute_multi_currency_logic()
    if not execute_multi_currency_flg:
        JobContext.logger.info("Multi currency logic skipped ..")
        return src_df

    # reading lookup tables
    l_fact_mv_multiplier_df = read_from_s3_catalog_convert_to_df(JobContext.get_property(C_GLUE_S3_DB_NAME_PROP_KEY),
                                                                 JobContext.get_property(C_GLUE_S3_TABLE_FACT_MV_MULTIPLIER),
                                                                 ["instrument_id","effective_dt","currency_cd","multiplier_type_cd",
                                                                  "multiplier_val","created_ts"])
    #todo need to distinct / latest recs?
    l_dim_instrument_df = read_from_s3_catalog_convert_to_df(JobContext.get_property(C_GLUE_S3_DB_NAME_PROP_KEY),
                                                             JobContext.get_property(C_GLUE_S3_TABLE_DIM_INSTRUMENT),
                                                             ["instrument_id","sor_instrument_id","active_flg",
                                                              "created_ts","currency_cd","instrument_type_cd"])
    l_dim_instrument_df = transform_boolean_clm(l_dim_instrument_df,"active_flg")
    l_dim_instrument_df.cache()
    # l_dim_instrument_df = l_dim_instrument_df.hint("broadcast").cache()

    l_forex_count = l_dim_instrument_df.where(sf.col("instrument_type_cd") == 'FOREX').select("sor_instrument_id").distinct().count()
    JobContext.logger.info("Forex record count - {}".format(l_forex_count))
    if l_forex_count > 1:
        # this is done for filtering fact_mv_multiplier data based on effective dt required for the particular batch
        l_mv_eff_dt_df = src_df.select("mv_effective_dt_for_curr").distinct()
        if l_mv_eff_dt_df is not None:
            l_date_list_df = l_mv_eff_dt_df.select(sf.collect_list("mv_effective_dt_for_curr"))
            l_date_list = l_date_list_df.collect()[0][0]
        else:
            l_date_list = []

        # deriving multiplier val for multi currency conversion
        l_fact_mv_multiplier_df = l_fact_mv_multiplier_df.where(sf.col("multiplier_type_cd") == "FX")
        l_fact_mv_lkp_df = l_fact_mv_multiplier_df.join(l_dim_instrument_df,["instrument_id"]) \
            .select(l_fact_mv_multiplier_df["instrument_id"],"effective_dt",l_fact_mv_multiplier_df["currency_cd"],
                    "multiplier_val",l_fact_mv_multiplier_df["created_ts"],l_dim_instrument_df["active_flg"],
                    l_dim_instrument_df["sor_instrument_id"],l_dim_instrument_df["instrument_type_cd"]) \
            .where(sf.col("instrument_type_cd")=="FOREX")
        l_window_cond = Window.partitionBy("sor_instrument_id","currency_cd","effective_dt").orderBy("effective_dt",sf.desc("created_ts"))

        l_fact_mv_lkp_df = l_fact_mv_lkp_df.withColumn("row_num",sf.row_number().over(l_window_cond))
        l_fact_mv_lkp_df = l_fact_mv_lkp_df.where(sf.col("active_flg") & (sf.col("row_num") == 1)
                                                  & sf.col("effective_dt").isin(l_date_list))
        l_fact_mv_lkp_df = l_fact_mv_lkp_df.withColumn("lkp_val",sf.concat("sor_instrument_id","effective_dt"))
        l_fact_mv_list = l_fact_mv_lkp_df.select("lkp_val","multiplier_val").rdd.collectAsMap()

        JobContext.logger.info("Multiplier val list: {}".format(l_fact_mv_list))

        # deriving base currency column - base_currency
        l_bs_currency = derive_currency_cd()
        src_df = src_df.withColumn("base_currency",sf.lit(l_bs_currency))

        # deriving security currency column - sec_currency_cd
        l_inst_window_cond = Window.partitionBy("instrument_id").orderBy(sf.desc("created_ts"))
        l_dim_instrument_df = l_dim_instrument_df.withColumn("row_num",sf.row_number().over(l_inst_window_cond))
        l_dim_instrument_df = l_dim_instrument_df.where((sf.col("row_num")==1) & sf.col("active_flg"))

        src_df = src_df.join(l_dim_instrument_df.alias("inst"),["instrument_id"],"left") \
            .select(src_df["*"],sf.col("inst.currency_cd").alias("sec_currency_cd"))

        # udf for fetching conversion rt
        def get_conversion_rt(l_main_curr,l_mv_eff_dt,l_settle_curr,l_base_curr, is_main_curr_base_curr, l_mv_val_list = l_fact_mv_list):
            try:
                if l_main_curr is None:
                    l_main_curr = ""
                if l_settle_curr is None:
                    l_settle_curr = ""
                if l_mv_eff_dt is None:
                    l_mv_eff_dt = ""
                if l_base_curr is None:
                    l_base_curr = ""

                l_main_stl_curr = str(l_main_curr) + '-' + str(l_settle_curr) + str(l_mv_eff_dt)
                l_stl_main_curr = str(l_settle_curr) + '-' + str(l_main_curr) + str(l_mv_eff_dt)

                l_main_base_curr = None
                l_settle_base_curr = None
                if not is_main_curr_base_curr:
                    l_main_base_curr = str(l_main_curr) + '-' + str(l_base_curr) + str(l_mv_eff_dt)
                    l_settle_base_curr = str(l_settle_curr) + '-' + str(l_base_curr) + str(l_mv_eff_dt)

                if l_main_stl_curr in l_mv_val_list:
                    return Decimal(l_mv_val_list.get(l_main_stl_curr))

                if l_stl_main_curr in l_mv_val_list:
                    return Decimal(1.0)/Decimal(l_mv_val_list.get(l_stl_main_curr))

                if not is_main_curr_base_curr and (l_main_base_curr in l_mv_val_list) and (l_settle_base_curr in l_mv_val_list):
                    return Decimal(l_mv_val_list.get(l_main_base_curr))/Decimal(l_mv_val_list.get(l_settle_base_curr))

                return Decimal(1.0)

            except Exception as ex:
                raise ex

        l_get_conv_rt_udf = sf.udf(get_conversion_rt, C_DEFAULT_DECIMAL_TYPE)
        l_when_conf = sf.col("capital_flow_algo_id").isin([sf.lit(3),sf.lit(5)])

        src_df = src_df.withColumn("price_val_mv",sf.when((l_when_conf) & (sf.col("pr_currency_cd") != sf.col("settle_currency_cd")),
                                                          l_get_conv_rt_udf("pr_currency_cd","mv_effective_dt_for_curr",
                                                                            "settle_currency_cd","base_currency",sf.lit(False))
                                                          *sf.col("price_val_mv")).otherwise(sf.col("price_val_mv"))) \
            .withColumn("accured_int_rt",sf.when((l_when_conf) & (sf.col("sec_currency_cd") != sf.col("settle_currency_cd")),
                                                 l_get_conv_rt_udf("sec_currency_cd","mv_effective_dt_for_curr",
                                                                   "settle_currency_cd","base_currency",sf.lit(False))
                                                 *sf.col("accured_int_rt")).otherwise(sf.col("accured_int_rt"))) \
            .withColumn("open_price",sf.when((l_when_conf) & (sf.col("base_currency") != sf.col("settle_currency_cd")),
                                             l_get_conv_rt_udf("base_currency","mv_effective_dt_for_curr",
                                                               "settle_currency_cd","base_currency",sf.lit(True))
                                             *sf.col("open_price")).otherwise(sf.col("open_price")))

        src_df.drop(*["mv_effective_dt_for_curr","base_currency","sec_currency_cd"])
        g_part_no = src_df.rdd.getNumPartitions()
        JobContext.logger.info("Partition no. - {}".format(g_part_no))
        src_df = src_df.repartition(g_part_no)
    else:
        JobContext.logger.info("As there are no multiple currency records available, hence skipping.")

    JobContext.logger.info("Multi currency based derivation ends")
    return src_df

###########################
## This mehtod will derived ADV_FLG based on its class_type_id
##############################
def __derive_adv_flg_based_on_dim_clss_type(trans_df):
    JobContext.logger.info("Derive Advisory flag based on records'im_class_type")

    dim_class_type=read_from_catalog_and_filter_inactive_rec(JobContext.get_property(C_GLUE_S3_TABLE_DIM_CLASS_TYPE))

    fact_entity_type=read_from_s3_catalog_convert_to_df(JobContext.get_property(C_GLUE_S3_DB_NAME_PROP_KEY),
                                                        JobContext.get_property(C_S3_FACT_CLASS_ENTITY_TBL_NME)) \
        .select("active_flg","class_type_id","entity_id","created_ts")


    # we have two different type of list 1) child_class_type, whose default ADV_FLG is ADV 2)  child_class_type, whose default ADV_FLG is Null
    # we need both data so creating combined list and filtering based on that
    dim_class_type_cd_with_default_value_of_adv = C_CHILD_CLASS_TYPE_CD_WITH_NON_ADV_REC[:]
    dim_class_type_cd_with_default_value_of_adv.extend(C_CHILD_CLASS_TYPE_CD_WITH_ADV_REC)

    dim_class_type = dim_class_type.filter((sf.col('child_class_type_cd')== C_DIM_CLASS_SEC_TYPE ) & sf.col('child_class_val')
                                           .isin(dim_class_type_cd_with_default_value_of_adv))

    # as list of class would be small, so collecting the in Python list
    dim_class_type= dim_class_type.collect()

    class_type_cd_to_class_id = {}
    adv_type_class_tpye_id = []
    nonadv_type_class_tpye_id = []

    # Here we are creating map of child_Class_Val and its class_Type_id, creating 2 different type list, first to contain class_type_id for these record ADG_FLG would ve ADV
    #For others it would be null
    for kv in  dim_class_type:
        clss_type_id = str(kv.class_type_id)
        child_class_val = str(kv.child_class_val)

        class_type_cd_to_class_id[child_class_val]=sf.lit(clss_type_id)
        JobContext.logger.info("Dim_class_tpye : Key: {}, value: {}".format(child_class_val, clss_type_id))
        if child_class_val in C_CHILD_CLASS_TYPE_CD_WITH_ADV_REC:
            adv_type_class_tpye_id.append(clss_type_id)

        else:
            nonadv_type_class_tpye_id.append(clss_type_id)

    JobContext.logger.info("class_type_cd_to_class_id.values() {}".format(type(class_type_cd_to_class_id.values())))
    fact_entity_type = fact_entity_type.filter(sf.col('class_type_id').isin(*class_type_cd_to_class_id.values()))

    # pick latest records only
    fact_Class_neith_dedup_window = Window. \
        partitionBy('entity_id'). \
        orderBy(sf.col('created_ts').desc())

    fact_entity_type = fact_entity_type.withColumn('row_num', sf.row_number().over(fact_Class_neith_dedup_window)).filter(sf.col('row_num')==1)
    fact_entity_type = transform_boolean_clm(fact_entity_type, "active_flg").filter('active_flg')

    fact_entity_type = fact_entity_type.select("class_type_id","entity_id").alias('fet')
    trans_df = trans_df.alias('src')
    fact_entity_type.cache()
    trans_df = trans_df.join(sf.broadcast(fact_entity_type), sf.col('fet.entity_id')==sf.col('src.instrument_id'),'left_outer').select(trans_df['*'],sf.col('fet.class_type_id').alias('class_type_id'))

    # create two Dataframe, 1)  records with class_type_id and records without class_type_id
    trn_class_type_df = trans_df.filter(sf.col('class_type_id').isNotNull())

    trn_class_type_df = trn_class_type_df.withColumn('adv_flg',sf.when(sf.col('class_type_id').isin(adv_type_class_tpye_id), sf.lit('ADV')).otherwise(sf.lit(None)))
    trn_without_class_type_df = trans_df.filter(sf.col('class_type_id').isNull())

    JobContext.logger.info("Derived Advisory flag based on records'im_class_type")
    return trn_without_class_type_df , trn_class_type_df

def __derive_advisory_flg(trans_df):
    JobContext.logger.info("Derive Advisory Flag")

    # First derive based on class_type_id
    trans_clm_with_adv_flg = C_REQUIRED_CLM_FOR_FACT_DERIVATION[:]
    trans_df = trans_df.select(*trans_clm_with_adv_flg)
    trn_without_class_type_df , trn_class_type_df = __derive_adv_flg_based_on_dim_clss_type(trans_df)

    fact_account_date = read_from_s3_catalog_convert_to_df(JobContext.get_property(C_GLUE_S3_DB_NAME_PROP_KEY),JobContext.get_property(C_GLUE_S3_TABLE_FACT_ACC_DATE_PROP_KEY),)

    default_end_dt_clm = sf.to_date(sf.lit(C_DEFAULT_END_DATE), C_DATE_FORMAT)

    fact_account_date = transform_boolean_clm(fact_account_date,'active_flg')
    fact_account_date = format_date(fact_account_date,'date_type_dt').withColumnRenamed('date_type_dt','start_dt')


    fact_account_date = fact_account_date.filter(sf.col('date_type_cd').isin([ C_DATE_DTPE_CD_ACLDT, C_DATE_DTPE_CD_PRGDT])) \
        .filter((sf.col("entity_id").isNull()) | (sf.col("entity_id") == ""))

    fact_account_dt_req_clms = ['account_id','date_type_cd','start_dt','program_ind','active_flg','created_ts']

    # First pick the latest active records
    latest_Active_Rec_Window = Window. \
        partitionBy('account_id','start_dt'). \
        orderBy(sf.col('created_ts').desc())

    fact_account_date = fact_account_date.select(*fact_account_dt_req_clms)

    fact_account_date = fact_account_date.withColumn('row_num',sf.row_number().over(latest_Active_Rec_Window)) \
        .filter(sf.col('row_num')==1).drop('row_num')

    fact_account_date = fact_account_date.filter(sf.col('active_flg'))

    # now derive end_dt parition by account_d and order by start_dt, then use lead  -1 to derive end_dt
    derive_end_dt_Windo = Window. \
        partitionBy('account_id'). \
        orderBy(sf.col('start_dt').asc())

    fact_account_date = fact_account_date.withColumn('end_dt',sf.lead(sf.col('start_dt')).over(derive_end_dt_Windo))
    fact_account_date = fact_account_date.withColumn('end_dt',sf.when(sf.col('end_dt').isNull(), default_end_dt_clm)
                                                     .otherwise(sf.date_add( sf.col('end_dt'),-1)))

    fact_account_date = fact_account_date.drop("created_ts","active_flg").alias('fact_account_date')

    trn_without_class_type_df = trn_without_class_type_df.alias('trans_df')
    trn_without_class_type_df = trn_without_class_type_df.withColumn('effective_dt',sf.to_date(sf.col('effective_dt'), 'yyyy-MM-dd'))

    # join it with fact_Account_dt

    date_join_condition = sf.col('effective_dt').between(sf.col('fact_account_date.start_dt'),sf.col('fact_account_date.end_dt'))
    trans_joined_fact_df = trn_without_class_type_df.join(sf.broadcast(fact_account_date),(sf.col('trans_df.account_id')==sf.col('fact_account_date.account_id'))
                                                          & date_join_condition,"left_outer") \
        .select(trn_without_class_type_df['*'],fact_account_date.account_id.alias('fact_account_id'),sf.col('fact_account_date.program_ind').alias('fact_program_ind') \
                ,sf.col('fact_account_date.date_type_cd').alias('date_type_cd'))

    # filter records which does not have instrument_id (by default ADV) and program_ind and date_type_cd=ACLDT (This by default is no ADV)
    trans_joined_fact_df = trans_joined_fact_df.withColumn("self_derive_adv_flg",(trans_joined_fact_df.fact_program_ind.isNull()| sf.col('fact_account_id').isNull()
                                                                                  | (sf.col('date_type_cd')== C_DATE_DTPE_CD_ACLDT)|sf.col('instrument_id').isNull() ))
    trns_df_without_program_ind = trans_joined_fact_df.filter(sf.col("self_derive_adv_flg"))

    # derive adc_flg for various direct derivation logics
    pace_cond = (sf.col('date_type_cd')== C_DATE_DTPE_CD_PRGDT) & (sf.col("fact_program_ind").isin([C_FAD_PROGRAM_CD_MA7,C_FAD_PROGRAM_CD_MAI]))
    trns_df_without_program_ind = trns_df_without_program_ind.withColumn('adv_flg', sf.when(pace_cond,sf.lit(None)) \
                                                                         .when(sf.col('fact_account_id').isNull(), sf.lit(None))
                                                                         .when(sf.col('date_type_cd')== C_DATE_DTPE_CD_ACLDT, sf.lit(None))
                                                                         .when(sf.col('instrument_id').isNull(), sf.lit(C_ADVISORY_VALUE_FOR_PROGRAM_IND_NULL))
                                                                         .otherwise(sf.lit(C_ADVISORY_VALUE_FOR_PROGRAM_IND_NULL)))

    # Records for which we need to derive adv_flg using through join
    trns_df_with_program_ind = trans_joined_fact_df.filter(~sf.col("self_derive_adv_flg"))

    # pick only selected column and get the distinct value for it, this will help to eliminate the duplicate created due to overlapping date range in
    # dtl_program_asset table
    clms_for_dtl_prog_asset = ['instrument_id','effective_dt','fact_program_ind']
    dis_trans_with_program_ind = trns_df_with_program_ind.select(*clms_for_dtl_prog_asset).distinct()

    dtl_program_asset = read_from_s3_catalog_convert_to_df(JobContext.get_property(C_GLUE_S3_DB_NAME_PROP_KEY), JobContext.get_property(C_GLUE_S3_TABLE_DTL_PROGRAM_ASSET))

    # join it with source based on instrument_id and program_ind
    dtl_program_asset = dtl_program_asset.alias('dps')
    dis_trans_with_program_ind = dis_trans_with_program_ind.alias('src')
    src_dps_join_condition = (sf.col('src.instrument_id')==sf.col('dps.instrument_id')) & (sf.col('src.fact_program_ind')==sf.col('dps.program_ind'))
    dtl_program_asset.cache()
    dps = dis_trans_with_program_ind.join(sf.broadcast(dtl_program_asset), src_dps_join_condition, 'left_outer') \
        .select(dis_trans_with_program_ind['*'],sf.col('dps.instrument_id').alias('dps_instrument_id'), \
                sf.col('dps.start_dt').alias('dps_start_dt'), sf.col('dps.end_dt').alias('dps_end_dt'), \
                sf.col('dps.created_ts').alias('dps_created_ts') )


    # now check if effective_dt is falling in joined dtl_program_asst's date range
    dps = dps.withColumn("date_rane_matched",sf.col("effective_dt").between(sf.col('dps_start_dt'),sf.col('dps_end_dt')))


    #  Now as we have ignred the effctive_dt in join it may have created duplicates
    # so parition the record based instrument_id','effective_dt','program_ind. which we know creates unique values
    # we have taken distinct on it,  this window will be order by date_rane_matched and created_ts

    window = Window. \
        partitionBy('instrument_id','effective_dt','fact_program_ind'). \
        orderBy(sf.col('date_rane_matched').desc(),sf.col('dps_created_ts').desc())
    dps = dps.withColumn('closest_match_indx', sf.row_number().over(window))

    # so if any record is matching (means closest_match_indx=True) select it, selection priority first based on closest_match_indx then created_ts
    dps_clms= ['instrument_id','effective_dt','fact_program_ind','adv_flg']
    dps = dps.filter(sf.col('closest_match_indx')==1)

    # Derive the adv_flg
    dps = dps.withColumn('adv_flg',sf.when(sf.col('dps_instrument_id').isNull(),
                                           sf.when(sf.col("fact_program_ind").isin([C_FAD_PROGRAM_CD_MAI,C_FAD_PROGRAM_CD_MA7]),
                                                   sf.lit(None)).otherwise(sf.lit(C_ADVISORY_VALUE_FOR_PROGRAM_IND_NULL))) \
                         .when(sf.col('date_rane_matched'), sf.lit(C_ADVISORY_VALUE_FOR_PROGRAM_IND_NULL)) \
                         .otherwise(sf.lit(None))).select(*dps_clms)
    # join back derived adv_flg with source
    dps = dps.alias('dps')
    dps.cache()
    trns_df_with_program_ind = trns_df_with_program_ind.alias('src')
    join_cond_with_dps_sr = (sf.col('src.instrument_id')==sf.col('dps.instrument_id')) &( sf.col('src.fact_program_ind')==sf.col('dps.fact_program_ind')) \
                            &  (sf.col('src.effective_dt')==sf.col('dps.effective_dt'))
    trns_df_with_derived_adv_flg = trns_df_with_program_ind.join(sf.broadcast(dps), join_cond_with_dps_sr, 'left_outer') \
        .select(trns_df_with_program_ind['*'],sf.col('dps.adv_flg').alias('adv_flg'))
    trns_df_with_derived_adv_flg = trns_df_with_derived_adv_flg.withColumn("pace_account_flg",(sf.col("adv_flg").isNotNull())
                                                                           &(sf.col("fact_program_ind").isin([C_FAD_PROGRAM_CD_MAI,C_FAD_PROGRAM_CD_MA7])))
    # select the column and take union

    trans_clm_with_adv_flg.append('adv_flg')
    trans_clm_with_adv_flg.append('pace_account_flg')
    trn_class_type_df = trn_class_type_df.withColumn("pace_account_flg",sf.lit(False))
    trns_df_without_program_ind = trns_df_without_program_ind.withColumn("pace_account_flg",sf.lit(False))
    trns_df_with_derived_adv_flg = trns_df_with_derived_adv_flg.select(*trans_clm_with_adv_flg)
    trns_df_without_program_ind = trns_df_without_program_ind.select(*trans_clm_with_adv_flg)
    trn_class_type_df = trn_class_type_df.select(*trans_clm_with_adv_flg)

    trns_df = trns_df_with_derived_adv_flg.union(trns_df_without_program_ind).union(trn_class_type_df)

    JobContext.logger.info("Derived Advisory Flag")
    return trns_df

def split_df_based_on_child_class_val(fact_df):
    JobContext.logger.info("Spliting Fact transaction dat abased on child class value")
    fact_capital_extra_amount_Rec = fact_df.where(fact_df["child_class_val"].isin(C_CHILD_CLASS_VALUES_FOR_CAPITAL_FLOW_EXTRA_AMOUT))
    fact_df = fact_df.where((~fact_df["child_class_val"].isin(C_DIM_CLASS_TYPE_CHILD_CLASS_VAL_COMMITMENT))
                            | (fact_df["child_class_val"].isNull())
                            | (fact_df["child_class_val"] == ""))
    JobContext.logger.info("Data splitted Fact transaction dat abased on child class value")
    return fact_df,fact_capital_extra_amount_Rec


def derive_fiscal_trn_grp_cf_records(p_src_df, p_col_list):
    JobContext.logger.info(f"Deriving Fact_captial_flow_fee records based on transaction group {C_REQ_TRN_GRP_ID_LIST}")
    p_src_df = p_src_df.where((sf.col("transaction_group_id").isin(C_REQ_TRN_GRP_ID_LIST))
                              & (sf.col('net_amt_val').isNotNull()) & (sf.col('net_amt_val') != 0))
    l_acc_df = p_src_df.where(sf.col('account_capital_flow_flg'))
    l_acc_df = l_acc_df.withColumn("amt_flg",sf.when(sf.col('transaction_group_id') == 5016,sf.lit('CSH_DEP'))
                                   .when(sf.col('transaction_group_id').isin(5027, 5028),sf.lit('CSH_WTH')))
    l_acc_df = l_acc_df.groupby("account_id","sleeve_id","account_type_id","currency_cd","effective_dt","amt_flg",'adv_flg') \
        .sum('net_amt_val').withColumnRenamed('sum(net_amt_val)','amt_val') \
        .withColumn("instrument_id", sf.lit(None))

    l_pos_df = p_src_df.where(sf.col('position_capital_flow_flg') & sf.col('instrument_id').isNotNull())
    l_pos_df = l_pos_df.withColumn("amt_flg",sf.when(sf.col('transaction_group_id') == 5016,sf.lit('CSH_DEP'))
                                   .when(sf.col('transaction_group_id').isin(5027, 5028),sf.lit('CSH_WTH')))
    l_pos_df = l_pos_df.groupby("account_id","instrument_id","sleeve_id","account_type_id","currency_cd","effective_dt","amt_flg", 'adv_flg') \
        .sum("net_amt_val").withColumnRenamed('sum(net_amt_val)','amt_val')
    l_all_records_df = l_acc_df.select(*p_col_list).union(l_pos_df.select(*p_col_list))

    JobContext.logger.info(f"Derived Fact_captial_flow_fee records based on transaction group {C_REQ_TRN_GRP_ID_LIST}")
    return l_all_records_df




C_FACT_CLASS_ENTITY_COL_LIST = ['entity_id','class_type_id','entity_type_cd','overriding_entity_type_cd','overriding_entity_id','effective_from_dt','effective_to_dt','allocation_prcnt',
                                'firm_id','active_flg','batch_dt','created_program_nm','created_user_nm','created_ts']
C_FACT_CLASS_ENTITY_DEFAULT_COL_LIST= {'entity_type_cd':C_FCE_ENTITY_TYPE_CD_SEC,'overriding_entity_type_cd':None,"effective_from_dt":C_DEFAULT_START_DATE,
                                       "effective_to_dt":C_DEFAULT_END_DATE,'allocation_prcnt':None,'overriding_entity_id':None,'firm_id':C_FIRM_ID}

def categorize_aip_instruments(api_src_df):

    JobContext.logger.info("Categorizing instrument")

    dtl_instrument_df = read_from_s3_catalog_convert_to_df(JobContext.get_property(C_GLUE_S3_DB_NAME_PROP_KEY)
                                                           ,JobContext.get_property(C_GLUE_S3_TABLE_DTL_INSTRUMENT_REF_PROP_KEY))

    dtl_instrument_df = transform_boolean_clm(dtl_instrument_df,"active_flg").filter(sf.col("instrument_ref_type_cd").isin(C_INSTRUMENT_REF_TYPE_CDS_INSU_PW_SEC_ID))
    dtl_instrument_df = pick_latest_from_dtl_instrument(dtl_instrument_df)

    join_cond = [sf.col("src.instrument_id")==sf.col("dtl.instrument_id")]
    api_src_df = api_src_df.alias("src").join(dtl_instrument_df.alias("dtl"), join_cond, "left_outer") \
        .select(api_src_df['*'], sf.col("dtl.instrument_ref_nm"))

    api_src_df = api_src_df.filter(sf.upper(sf.col("instrument_ref_nm")).like(C_API_INSTRUMENT_REF_NAME_PREFIX))

    need_categorization_df = api_src_df.select("instrument_id", "quantity_val", "cf_val", "transaction_type_desc") \
        .withColumn('row_num', sf.row_number()
                    .over(Window.partitionBy("instrument_id")
                          .orderBy(sf.desc("instrument_id")))).filter(sf.col("row_num") == 1)

    dim_class_type_df = read_from_s3_catalog_convert_to_df(JobContext.get_property(C_GLUE_S3_DB_NAME_PROP_KEY),
                                                           JobContext.get_property(C_GLUE_S3_TABLE_DIM_CLASS_TYPE))
    aip_dim_clss_type_map = transform_boolean_clm(dim_class_type_df,"active_flg") \
        .filter(sf.col("child_class_type_cd")==C_DCT_CHILD_CLASS_TYPE_CD_AIP_SEC_TYPE) \
        .filter("active_flg").select(sf.upper(sf.col("child_class_val")),"class_type_id").rdd.collectAsMap()


    fact_class_entity_df = read_from_s3_catalog_convert_to_df(JobContext.get_property(C_GLUE_S3_DB_NAME_PROP_KEY),
                                                              JobContext.get_property(C_S3_FACT_CLASS_ENTITY_TBL_NME))
    fact_class_entity_df = fact_class_entity_df.filter(sf.col("entity_type_cd") == C_FCE_ENTITY_TYPE_CD_SEC) \
        .filter(sf.col("class_type_id").isin(*aip_dim_clss_type_map.values())) \
        .withColumn('row_num', sf.row_number().over(Window.partitionBy("entity_id","class_type_id").orderBy(sf.desc("created_ts")))) \
        .filter(sf.col("row_num") == 1)

    fact_class_entity_df = transform_boolean_clm(fact_class_entity_df,"active_flg").filter("active_flg").select("entity_id").distinct()

    #TODO: Using source in right side as it should be smaller, check with large file if can broadcast it
    need_categorization_df = need_categorization_df.alias("src").join(fact_class_entity_df.alias("fce"),sf.col("src.instrument_id")==sf.col("fce.entity_id"),"left_anti") \
        .select(need_categorization_df['*']).withColumnRenamed("instrument_id","entity_id")

    insert_new_records_df = add_default_clm(need_categorization_df,C_FACT_CLASS_ENTITY_DEFAULT_COL_LIST)
    insert_new_records_df = add_audit_columns(insert_new_records_df,JobContext.batch_date_clm,add_modification_clm=False)
    # aip_fact_class_entity
    insert_new_records_df = insert_new_records_df.withColumn("class_type_id",sf.when((sf.col("quantity_val").isNotNull() & (sf.col("quantity_val")!=0)) &(sf.col("cf_val")<=.99), aip_dim_clss_type_map.get(C_DIM_CLASS_TYPE_CHILD_CLASS_VAL_COMMITMENT)) \
                                                             .when(sf.lower(sf.col("transaction_type_desc")).rlike("distribution|interest|dividend"),aip_dim_clss_type_map.get(C_DIM_CLASS_TYPE_CHILD_CLASS_VAL_DISTRIBUTION)) \
                                                             .otherwise(aip_dim_clss_type_map.get(C_DIM_CLASS_TYPE_CHILD_CLASS_VAL_CONTRIBUTION)))
    #aip_fact_class_entity
    previous_file= JobContext.file_being_processed
    new_file_being_processed = "aip_fact_class_entity"
    JobContext.file_being_processed= new_file_being_processed
    JobContext.logger.info(f"Updating file being processed from: {previous_file} to: {new_file_being_processed}")
    try:
        save_output_to_s3(insert_new_records_df.select(*C_FACT_CLASS_ENTITY_COL_LIST),"fact_class_entity",JobContext.get_property(C_S3_FACT_CLASS_ENTITY_OUTPUT_LOCATION))
    finally:
        JobContext.file_being_processed=previous_file
    JobContext.logger.info("Instrument categorized")


def adjust_trn_dates(src_df):

    processor = AdjustBackDatedTransCustomProcessor()
    if not processor.adjust_back_dated_trans():
        JobContext.logger.info("Adjusting backdates transactions skipped")
        return src_df
    JobContext.logger.info("Adjusting backdates transactions")

    mtdt_batch_dt_df = read_from_s3_catalog_convert_to_df(JobContext.get_property(C_GLUE_S3_DB_NAME_PROP_KEY),JobContext.get_property(C_GLUE_S3_TABLE_MTDT_BATCH_DATE_PROP_KEY))
    cpos_batch_date_dict = mtdt_batch_dt_df.filter(sf.col("batch_source_cd")==C_CPOS_CONV_DT).first()

    #This could happen in QA env, when we dont have fresh unload, putting it on safe side
    if cpos_batch_date_dict is None:
        JobContext.logger.info("CPOS batch date not found, skipping date adjustment")
        return src_df
    cpos_batch_date = cpos_batch_date_dict.asDict()['batch_dt']
    JobContext.logger.info(f"CPOS batch date : {cpos_batch_date}")

    cpos_next_batch_dt_df = mtdt_batch_dt_df.filter(sf.col("batch_dt")> cpos_batch_date).orderBy("batch_dt")
    cpos_next_batch_dt_dict = cpos_next_batch_dt_df.select(sf.col('batch_dt')).first()
    if cpos_next_batch_dt_dict is None:
        JobContext.logger.info(f"Next date to cpos batch date not found so using job's batch date {JobContext.batch_date}")
        cpos_next_batch_dt = JobContext.batch_date
    else:
        cpos_next_batch_dt = cpos_next_batch_dt_dict.asDict()['batch_dt']
        JobContext.logger.info(f"Next date to cpos batch date: {cpos_next_batch_dt}")
    import time
    millis = int(round(time.time()*1000))
    back_dated_check_clm = f"back_dated_trn_{millis}"
    src_df = src_df.withColumn(back_dated_check_clm, sf.col("effective_dt")<=cpos_batch_date)
    src_df = src_df.withColumn("effective_dt", sf.when(sf.col(back_dated_check_clm), sf.lit(cpos_next_batch_dt)).otherwise(sf.col("effective_dt"))) \
        .withColumn("settle_dt", sf.when(sf.col(back_dated_check_clm), sf.lit(cpos_next_batch_dt)).otherwise(sf.col("settle_dt"))) \
        .withColumn("trade_dt", sf.when(sf.col(back_dated_check_clm), sf.lit(cpos_next_batch_dt)).otherwise(sf.col("trade_dt"))) \
        .drop(back_dated_check_clm)

    JobContext.logger.info("Adjusted backdates transactions")
    return src_df

def parallel_create_fact_capital_flw(fact_trans_new_rec_df, fact_capital_extra_amount_rec = None):
    JobContext.logger.info("Staring Fact capital flow")
    # adding records based on cf_Val, add two new column so that, we can group the record in single operation and then select the not null value as target
    JobContext.logger.info("Creating fact capital flow records using  account_capital_flow_flg")
    fact_cap_flow_cf_Val_df = fact_trans_new_rec_df.withColumn('cf_val_positive', sf.when(sf.col('cf_val') >= 0, sf.col('cf_val')).otherwise(None)) \
        .withColumn('cf_val_negative', sf.when(sf.col('cf_val') < 0, sf.col('cf_val')).otherwise(None)) \
        .withColumn('sign', sf.col('cf_val') < 0)


    # filter based on account_capital_flow_flg and pick only not null cf_Val records
    fact_cap_flow_cf_Val_df.persist()
    fact_cap_flow_capital_flow_flg_df = fact_cap_flow_cf_Val_df.filter('account_capital_flow_flg').filter(fact_trans_new_rec_df.cf_val.isNotNull())
    fact_cap_flow_capital_flow_flg_df = fact_cap_flow_capital_flow_flg_df.withColumn("adv_flg",sf.when(sf.col("pace_account_flg"),sf.lit(None))
                                                                                     .otherwise(sf.col("adv_flg")))
    fact_cap_flow_capital_flow_flg_df = fact_cap_flow_capital_flow_flg_df.groupby("account_id", "effective_dt","currency_cd", "account_type_id", "sleeve_id", 'sign','adv_flg','transaction_status_ind') \
        .sum('cf_val_positive', 'cf_val_negative') \
        .withColumnRenamed('sum(cf_val_positive)', 'cf_val_positive') \
        .withColumnRenamed('sum(cf_val_negative)', 'cf_val_negative') \
        .withColumn('instrument_id', sf.lit(None))
    fact_cap_flow_capital_flow_flg_df.persist()
    JobContext.logger.info("Created fact capital flow records using  account_capital_flow_flg")

    # filter based on account_capital_flow_flg and pick only not null cf_Val records
    JobContext.logger.info("Creating fact capital flow records using  position_capital_flow_flg")
    fact_cap_flow_pos_capital_flow_flg_df = fact_cap_flow_cf_Val_df.filter( fact_trans_new_rec_df.cf_val.isNotNull()) \
        .filter('position_capital_flow_flg'). \
        filter(fact_trans_new_rec_df.instrument_id.isNotNull())
    fact_pace_df = fact_cap_flow_pos_capital_flow_flg_df.where(sf.col("pace_account_flg"))
    fact_cap_flow_pos_capital_flow_flg_df = fact_cap_flow_pos_capital_flow_flg_df.groupby("account_id", "effective_dt",  "currency_cd", "account_type_id", "sleeve_id", 'sign', 'instrument_id','adv_flg','transaction_status_ind') \
        .sum('cf_val_positive', 'cf_val_negative').withColumnRenamed('sum(cf_val_positive)','cf_val_positive') \
        .withColumnRenamed('sum(cf_val_negative)', 'cf_val_negative')

    fact_pace_df = fact_pace_df.groupby("account_id", "effective_dt",  "currency_cd", "account_type_id", "sleeve_id", 'sign', 'instrument_id','adv_flg') \
        .sum('cf_val_positive', 'cf_val_negative') \
        .withColumn('amt_val', sf.when(sf.col('sum(cf_val_positive)').isNotNull(), sf.col('sum(cf_val_positive)'))
                    .otherwise(sf.col('sum(cf_val_negative)'))) \
        .withColumn('amt_flg', sf.when(sf.col('sign'), 'TCO').otherwise('TCI')).withColumn("instrument_id",sf.lit(None))

    fact_cap_flow_pos_capital_flow_flg_df.persist()
    JobContext.logger.info("Created fact capital flow records using  position_capital_flow_flg")

    JobContext.logger.info("Taking union of  fact capital flow records created using  position_capital_flow_flg and account_capital_flow_flg")
    cf_agg_df_clm_list = ["account_id", "effective_dt", "currency_cd", "account_type_id", "sleeve_id","instrument_id", 'sign', "cf_val_positive", "cf_val_negative",'adv_flg','transaction_status_ind']
    fact_capital_flw_all = fact_cap_flow_pos_capital_flow_flg_df.select(*cf_agg_df_clm_list).union(fact_cap_flow_capital_flow_flg_df.select(*cf_agg_df_clm_list))
    fact_capital_flw_all = fact_capital_flw_all.withColumn('amt_val', sf.when(sf.col('cf_val_positive').isNotNull(), sf.col('cf_val_positive'))
                                                           .otherwise(sf.col('cf_val_negative'))) \
        .withColumn('amt_flg', sf.when(sf.col('sign'), 'CO').otherwise('CI')) \
        .drop('cf_val_positive','cf_val_negative', 'sign')
    
    fact_capital_flw_all = fact_capital_flw_all.withColumn('amt_flg',sf.when((sf.col('transaction_status_ind')=='C') & (sf.col('amt_flg')=='CO'),sf.lit('CI')) \
                                                                .when((sf.col('transaction_status_ind')=='C') & (sf.col('amt_flg')=='CI'),sf.lit('CO')) \
                                                                .otherwise(sf.col('amt_flg'))) \
                                                                .drop('transaction_status_ind')
    
    fct_col_list = fact_capital_flw_all.columns
    fact_capital_flw_all = fact_capital_flw_all.select(*fct_col_list).union(fact_pace_df.select(*fct_col_list))
    fact_capital_flw_all.persist()
    JobContext.logger.info("Union of  fact capital flow records done")
    # Add record based on fee_flg
    # To club two group by we are following approch
    fact_trans_new_rec_df = fact_trans_new_rec_df.withColumn("commission_val", sf.col("commission_val").cast(C_DEFAULT_DECIMAL_TYPE)) \
        .withColumn('commission_val',sf.when(sf.col("commission_val")!=0, sf.col("commission_val")).otherwise(sf.lit(None)))
    fact_trans_new_rec_df = fact_trans_new_rec_df.withColumn("commission_val",sf.when(sf.col('transaction_status_ind') == 'N', -1*sf.col('commission_val')).otherwise(sf.col('commission_val')))
    fact_cap_flow_fee_flg_df_w_cmv = fact_trans_new_rec_df.filter('fee_flg').groupby("account_id", "effective_dt", "currency_cd", "account_type_id", "sleeve_id", 'adv_flg').sum('fee_val',"commission_val")
    cf_agg_df_clm_list = ["account_id", "effective_dt", "currency_cd", "account_type_id", "sleeve_id",  "instrument_id", 'amt_flg', 'amt_Val','adv_flg']


    fact_cap_flow_fee_flg_df = fact_cap_flow_fee_flg_df_w_cmv.withColumnRenamed('sum(fee_val)', 'amt_val').withColumn( 'amt_flg', sf.lit(C_FACT_CAPITAL_FLOW_FEE_AMT_FLG_F)) \
        .withColumn('instrument_id', sf.lit(None)) \
        .select("account_id", "effective_dt", "currency_cd","account_type_id", "sleeve_id", "instrument_id", "amt_val", "amt_flg", "adv_flg")
    JobContext.logger.info("Calculating commission val for position and account level")
    fact_cap_flow_fee_cv_flg_df = fact_trans_new_rec_df.filter(sf.col("commission_val").isNotNull()) \
        .groupby("account_id", "effective_dt", "currency_cd", "account_type_id", "sleeve_id",'adv_flg').sum("commission_val").withColumnRenamed('sum(commission_val)', 'amt_val') \
        .withColumn( 'amt_flg', sf.lit(C_FACT_CAPITAL_FLOW_FEE_AMT_FLG_F)) \
        .withColumn('instrument_id', sf.lit(None)) \
        .select("account_id", "effective_dt", "currency_cd","account_type_id", "sleeve_id", "instrument_id", "amt_val", "amt_flg", "adv_flg")

    fact_cap_flow_fee_flg_cm_instr_df = fact_trans_new_rec_df.filter(sf.col("instrument_id").isNotNull()) \
        .filter(sf.col("commission_val").isNotNull()) \
        .groupby("account_id", "effective_dt", "currency_cd", "account_type_id", "sleeve_id",
                 'adv_flg',"instrument_id").sum("commission_val").withColumnRenamed('sum(commission_val)', 'amt_val') \
        .withColumn('amt_flg', sf.lit(C_FACT_CAPITAL_FLOW_FEE_AMT_FLG_F))
    fact_cap_flow_fee_cv_flg_df = fact_cap_flow_fee_cv_flg_df.select(*cf_agg_df_clm_list) \
        .union(fact_cap_flow_fee_flg_cm_instr_df.select(*cf_agg_df_clm_list))
    fact_cap_flow_fee_cv_flg_df = add_default_clm(fact_cap_flow_fee_cv_flg_df, C_FACT_CAPITAL_FLOW_FEE_DEFAULT_MAPPING)
    fact_cap_flow_fee_cv_flg_df = add_audit_columns(fact_cap_flow_fee_cv_flg_df, JobContext.batch_date_clm,
                                                    created_progrm_nm=C_FACT_CAPITAL_FLOW_FEE_COMMISION_FLG_PRG_NM)

    # [25-Jun-20] Added cap_flow_fee_flg for all accounts_id's and instrument_id's
    fact_cap_flow_fee_flg_instr_df = fact_trans_new_rec_df.filter('fee_flg') \
        .filter(sf.col("instrument_id").isNotNull())

    dfs_df = read_from_catalog_and_filter_inactive_rec(JobContext.get_property(C_GLUE_S3_TABLE_DIM_FUND_SLEEVE_PROP_KEY),
                                                       ["sor_sleeve_id","active_flg"])
    dfs_df = dfs_df.withColumn("dfs_slv_id",sf.concat_ws("-","sor_sleeve_id",sf.lit(C_FIRM_ID)))
    l_src = fact_cap_flow_fee_flg_instr_df.alias("src")
    dfs = sf.broadcast(dfs_df).alias("lkp")
    fact_cap_flow_fee_flg_instr_df = l_src.join(dfs,sf.col("src.sleeve_id") == sf.col("lkp.dfs_slv_id"),"left") \
        .select(fact_cap_flow_fee_flg_instr_df["*"],dfs_df["dfs_slv_id"])
    fee_flg_mf_etf_df = fact_cap_flow_fee_flg_instr_df.filter(sf.col("dfs_slv_id").isNotNull())
    fact_cap_flow_fee_flg_instr_df = fact_cap_flow_fee_flg_instr_df.filter(sf.col("dfs_slv_id").isNull()) \
        .groupby("account_id", "instrument_id", "effective_dt", "currency_cd", "account_type_id", "sleeve_id", 'adv_flg').sum('fee_val')
    fact_cap_flow_fee_flg_instr_df = fact_cap_flow_fee_flg_instr_df.withColumnRenamed('sum(fee_val)', 'amt_val').withColumn( 'amt_flg', sf.lit(C_FACT_CAPITAL_FLOW_FEE_AMT_FLG_F)) \
        .select("account_id", "effective_dt", "currency_cd", "account_type_id", "sleeve_id", "instrument_id", "amt_val", "amt_flg", "adv_flg")
    # [19-May-21] fee_flg_mf_etf_df:WMAUCETG-12861
    fee_flg_mf_etf_df = fee_flg_mf_etf_df.filter(sf.col("dfs_slv_id").isNotNull())
    fee_flg_mf_etf_df = fee_flg_mf_etf_df.withColumn('sign', -1*sf.col('fee_val') < 0)
    fee_flg_mf_etf_df = fee_flg_mf_etf_df.withColumn('fee_val', -1*sf.col('fee_val'))
    fee_flg_mf_etf_df = fee_flg_mf_etf_df.withColumn('amt_flg', sf.when(sf.col('sign'), 'CO').otherwise('CI')) \
        .withColumn("instrument_id",sf.lit(None))
    fee_flg_mf_etf_df = fee_flg_mf_etf_df.filter(sf.col("dfs_slv_id").isNotNull()) \
        .groupby("account_id", "instrument_id", "effective_dt", "currency_cd", "account_type_id", "sleeve_id", 'adv_flg','amt_flg').sum('fee_val')
    fee_flg_mf_etf_df = fee_flg_mf_etf_df.withColumnRenamed('sum(fee_val)', 'amt_val') \
        .select("account_id", "effective_dt", "currency_cd", "account_type_id", "sleeve_id", "instrument_id", "amt_val", "amt_flg", "adv_flg")
    # [07-Jun-21]:fees on sleeves not working-WMAUCETG-13328
    fee_flg_mf_etf_df1  = fee_flg_mf_etf_df.withColumnRenamed('sum(fee_val)', 'amt_val') \
        .select("account_id", "effective_dt", "currency_cd", "account_type_id", "sleeve_id", "instrument_id", "amt_val", "amt_flg", "adv_flg")
    fee_flg_mf_etf_df1 = __check_default_sleeve_insert_brdg_acc_rel1(fee_flg_mf_etf_df1)

    fee_flg_mf_etf_df1 = fee_flg_mf_etf_df1.withColumn('amt_val', -1*sf.col('amt_val'))

    fact_capital_flw_all = fact_capital_flw_all.select(*cf_agg_df_clm_list).union( fact_cap_flow_fee_flg_df.select(*cf_agg_df_clm_list)) \
        .union( fact_cap_flow_fee_flg_instr_df.select(*cf_agg_df_clm_list)).union( fee_flg_mf_etf_df.select(*cf_agg_df_clm_list)).union( fee_flg_mf_etf_df1.select(*cf_agg_df_clm_list))
    fact_capital_flw_all.persist()

    # acount_id not null check not needed as it already done in vaildation of fact transaction
    dim_transaction_grp_df = read_from_catalog_and_filter_inactive_rec(JobContext.get_property(C_GLUE_S3_TABLE_DIM_TRANSACTION_GROUP),
                                                                       ["transaction_type_id","transaction_group_id","report_behavior_id",
                                                                        "active_flg"])
    fact_trans_new_rec_df = fact_trans_new_rec_df.join(sf.broadcast(dim_transaction_grp_df),  dim_transaction_grp_df.transaction_type_id == fact_trans_new_rec_df.transaction_type_id,  'left_outer') \
        .select(fact_trans_new_rec_df['*'], dim_transaction_grp_df.transaction_group_id,
                dim_transaction_grp_df.report_behavior_id)
    fact_trans_new_rec_df.cache()
    rpt_bhv_wise_records_df = fact_trans_new_rec_df

    #TODO: Need to update with proper flag value
    ma_fee_amt_flg = get_property_from_dim_tbl('MA_FEE_AMT_FLG').lower()
    transaction_grp_id_list = [103, 1330]

    if ma_fee_amt_flg in ['yes','y']:
        transaction_grp_id_list.extend([1120, 1121, 1122])

    fact_cap_flw_trn_grp_df = fact_trans_new_rec_df.filter( fact_trans_new_rec_df.transaction_group_id.isin(transaction_grp_id_list))
    fact_cap_flw_trn_grp_df = fact_cap_flw_trn_grp_df.groupby("account_id", "effective_dt", "currency_cd","account_type_id", "sleeve_id",'transaction_group_id','adv_flg') \
        .sum('net_amt_val') \
        .withColumnRenamed('sum(net_amt_val)', 'amt_val')

    fact_cap_flw_trn_grp_df = fact_cap_flw_trn_grp_df.withColumn('amt_flg', sf.when( fact_cap_flw_trn_grp_df.transaction_group_id == 103, 'CIL') \
                                                                 .when(fact_cap_flw_trn_grp_df.transaction_group_id == 1330, 'INT') \
                                                                 .when(fact_cap_flw_trn_grp_df.transaction_group_id == 1120, 'FE') \
                                                                 .when( fact_cap_flw_trn_grp_df.transaction_group_id == 1121, 'FR') \
                                                                 .otherwise(sf.lit('LI'))).withColumn("instrument_id", sf.lit(None))

    fact_cap_flw_trn_grp_df.persist()
    fact_capital_flw_all = fact_capital_flw_all.union(fact_cap_flw_trn_grp_df.select(*cf_agg_df_clm_list))
    # Deriving records based on transaction group 5002 and 5014
    trn_grp_records_df = derive_fiscal_trn_grp_cf_records(rpt_bhv_wise_records_df, cf_agg_df_clm_list)
    #Deriving records based on report behavior id
    rpt_bhv_wise_records_df = derive_report_behavior_wise_cf_records(rpt_bhv_wise_records_df, cf_agg_df_clm_list)
    fact_capital_flw_all = fact_capital_flw_all.select(*cf_agg_df_clm_list).union(rpt_bhv_wise_records_df.select(*cf_agg_df_clm_list)) \
        .union(trn_grp_records_df.select(*cf_agg_df_clm_list))

    if fact_capital_extra_amount_rec is not None:
        l_commitment_data_df = __derive_capital_flw_fee_records_for_extra_amounts(fact_capital_extra_amount_rec)
        fact_capital_flw_all = fact_capital_flw_all.select(*cf_agg_df_clm_list).union(l_commitment_data_df.select(*cf_agg_df_clm_list))
    fact_capital_flw_all.persist()
    fact_capital_flw_all = add_default_clm(fact_capital_flw_all, C_FACT_CAPITAL_FLOW_FEE_DEFAULT_MAPPING)
    fact_capital_flw_all = add_audit_columns(fact_capital_flw_all, JobContext.batch_date_clm)
    fact_capital_flw_all = fact_cap_flow_fee_cv_flg_df.select(C_FACT_CAPITAL_FLOW_FEE_TBL_CLM).union(fact_capital_flw_all.select(C_FACT_CAPITAL_FLOW_FEE_TBL_CLM))
    save_output_to_s3(fact_capital_flw_all.select(C_FACT_CAPITAL_FLOW_FEE_TBL_CLM), "fact_holder_df",
                      JobContext.get_property( C_S3_FACT_CAPITAL_FLOW_FEE_OUTPUT_LOCATION),repartition_count=None)
    JobContext.logger.info("Saved fact_holding ...")

def __derive_advisory_fee_flg(fact_capital_flw_all):
    JobContext.logger.info("Derive Advisory Flag")
    fact_account_date = read_from_s3_catalog_convert_to_df(JobContext.get_property(C_GLUE_S3_DB_NAME_PROP_KEY),JobContext.get_property(C_GLUE_S3_TABLE_FACT_ACC_DATE_PROP_KEY),)
    default_end_dt_clm = sf.to_date(sf.lit(C_DEFAULT_END_DATE), C_DATE_FORMAT)

    fact_account_date = transform_boolean_clm(fact_account_date,'active_flg')
    fact_account_date = format_date(fact_account_date,'date_type_dt').withColumnRenamed('date_type_dt','start_dt')

    fact_account_date = fact_account_date.filter(sf.col('date_type_cd').isin([C_DATE_DTPE_CD_ACLDT, C_DATE_DTPE_CD_PRGDT])) \
        .filter((sf.col("entity_id").isNull()) | (sf.col("entity_id") == ""))

    fact_account_dt_req_clms = ['account_id','date_type_cd','start_dt','program_ind','active_flg','created_ts']

    # First pick the latest active records
    latest_Active_Rec_Window = Window. \
        partitionBy('account_id','start_dt'). \
        orderBy(sf.col('created_ts').desc())

    fact_account_date = fact_account_date.select(*fact_account_dt_req_clms)

    fact_account_date = fact_account_date.withColumn('row_num', sf.row_number().over(latest_Active_Rec_Window)) \
        .filter(sf.col('row_num') == 1).drop('row_num')

    fact_account_date = fact_account_date.filter(sf.col('active_flg'))

    # now derive end_dt parition by account_d and order by start_dt, then use lead  -1 to derive end_dt
    derive_end_dt_Windo = Window. \
        partitionBy('account_id'). \
        orderBy(sf.col('start_dt').asc())

    fact_account_date = fact_account_date.withColumn('end_dt', sf.lead(sf.col('start_dt')).over(derive_end_dt_Windo))
    fact_account_date = fact_account_date.withColumn('end_dt', sf.when(sf.col('end_dt').isNull(), default_end_dt_clm)
                                                     .otherwise(sf.date_add(sf.col('end_dt'), -1)))

    fact_account_date = fact_account_date.drop("created_ts", "active_flg").alias('fact_account_date')

    fact_capital_flw_all = fact_capital_flw_all.alias('fact_df')
    fact_capital_flw_all = fact_capital_flw_all.withColumn('effective_dt', sf.to_date(sf.col('effective_dt'), 'yyyy-MM-dd'))

    # join it with fact_Account_dt
    date_join_condition = sf.col('effective_dt').between(sf.col('fact_account_date.start_dt'),sf.col('fact_account_date.end_dt'))
    fact_capital_flw_join = fact_capital_flw_all.join(sf.broadcast(fact_account_date), (sf.col('fact_df.account_id') == sf.col('fact_account_date.account_id'))
                                                      & date_join_condition, "left_outer")\
        .select(fact_capital_flw_all['*'], fact_account_date.account_id.alias('fact_account_id'), sf.col('fact_account_date.program_ind').alias('fact_program_ind'),
                sf.col('fact_account_date.date_type_cd').alias('date_type_cd'))

    fact_cap_df = fact_capital_flw_join.withColumn('adv_flg_f', sf.when(sf.col('fact_account_id').isNull(), sf.lit(None))
                                                   .when(sf.col('date_type_cd') == C_DATE_DTPE_CD_ACLDT, sf.lit(None))
                                                   .otherwise(sf.lit(C_ADVISORY_VALUE_FOR_PROGRAM_IND_NULL)))
    # From 10/18/2022 we are deriving ADV flg for only Accounts and not deriving for Accounts & Securities
    # Combination for transactions.
    fact_cap_df = fact_cap_df.withColumn('adv_flg_new_derv',
                                         sf.when((sf.col('amt_flg') == C_FACT_CAPITAL_FLOW_FEE_AMT_FLG_F) & (sf.col('instrument_id').isNull()), sf.col('adv_flg_f'))
                                         .otherwise(sf.col('adv_flg'))).drop('adv_flg', 'adv_flg_f')
    fact_cap_df = fact_cap_df.withColumnRenamed('adv_flg_new_derv', 'adv_flg').select(C_FACT_CAPITAL_FLOW_FEE_TBL_CLM)

    return fact_cap_df
