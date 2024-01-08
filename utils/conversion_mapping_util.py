####################################################################################################################
# This script will hold Columns rename for RS DB tables from GP tables.
#-------------------------------------------------------------------------------------------------------------------
#					History
#  Date			   Version No	  Developer			Change
# -------		   ----------	  ---------			------
# 03/21/2023		 0.1		  Rajalakshmi R		Initial Version
# ------------------------------------------------------------------------------------------------------------------
####################################################################################################################

from pyspark.sql import functions as sf
from utils.JobContext import JobContext
from pyspark.sql.functions import col
import boto3

C_EFFECTIVE_DT_TABLES = ['fact_mv_multipler', 'agg_transaction', 'fact_holding', 'fact_price','fact_transaction','fact_capital_flow_fee', 'wrk_mv_multiplier_pivot',
                         'fact_account_mv_0','fact_account_mv_1','fact_account_mv_2','fact_account_mv_3', 'fact_account_mv_4', 'fact_account_mv_5', 'fact_account_mv_6',
                         'fact_account_mv_7','fact_account_mv_8', 'fact_account_mv_9', 'fact_transaction_0','fact_transaction_1','fact_transaction_2', 'fact_transaction_3',
                         'fact_transaction_4','fact_transaction_5','fact_transaction_6','fact_transaction_7','fact_transaction_8','fact_transaction_9','fact_holding', 
                         'fact_capital_flow_fee_0', 'fact_capital_flow_fee_1','fact_capital_flow_fee_2', 'fact_capital_flow_fee_3', 'fact_capital_flow_fee_4', 'fact_capital_flow_fee_5',
                         'fact_capital_flow_fee_6', 'fact_capital_flow_fee_7', 'fact_capital_flow_fee_8', 'fact_capital_flow_fee_9','fact_mv_multipler0','fact_mv_multipler1',
                         'fact_mv_multipler2', 'fact_mv_multipler3', 'fact_mv_multipler4','fact_mv_multipler5', 'fact_mv_multipler6', 'fact_mv_multipler7', 'fact_mv_multipler8','fact_mv_multipler9']

C_NO_PARTITION_FILED_TABLES = ['mtdt_ror_account_grp_filter', 'mtdt_exclude_account', 'mtdt_exclude_instr',
                               'wrk_recalculate', 'mtdt_account_bucket', 'mtdt_instrument_bucket']

V_FACT_HOLDING = ['fact_holding']

V_FACT_CAPITAL_FLOW_FEE = ['fact_capital_flow_fee_0', 'fact_capital_flow_fee_1','fact_capital_flow_fee_2', 'fact_capital_flow_fee_3',
                           'fact_capital_flow_fee_4', 'fact_capital_flow_fee_5', 'fact_capital_flow_fee_6', 'fact_capital_flow_fee_7',
                           'fact_capital_flow_fee_8','fact_capital_flow_fee_9']

V_FACT_TRANSACTION = ['fact_transaction_0', 'fact_transaction_1', 'fact_transaction_2', 'fact_transaction_3',
                       'fact_transaction_4', 'fact_transaction_5', 'fact_transaction_6', 'fact_transaction_7',
                       'fact_transaction_8','fact_transaction_9']

V_FACT_MV_MULTIPLIER = ['fact_mv_multipler_0','fact_mv_multipler_1','fact_mv_multipler_2','fact_mv_multipler_3',
                     'fact_mv_multipler_4','fact_mv_multipler_5','fact_mv_multipler_6','fact_mv_multipler_7',
                     'fact_mv_multipler_8','fact_mv_multipler_9']


def columns_to_select(p_src_df, table_name):
    if table_name in V_FACT_TRANSACTION:
        # Assign column names to existing DataFrame
        COLUMNS_TO_SELECT = ['transaction_type_id','transaction_type_desc','account_id','instrument_id',
                    'account_type_id','transaction_behavior_id','behavior_desc','source_system_id','source_system_desc',
                    'corp_action_decl_id','sleeve_id','trade_dt','settle_dt','effective_dt','trade_currency_cd',
                    'settle_currency_cd','sor_transaction_ref','account_capital_flow_flg','position_capital_flow_flg',
                    'capital_flow_algo_id','fee_flg','cf_val','fee_val','spot_exchange_rt','trailer_txt','quantity_val',
                    'price_val','principal_val','commission_val','misc_expense_val','net_amt_val','amortization_factor_val',
                    'accrued_interest_val','batch_cd','entry_cd','sec_fee_val','postage_val','other_fee','credit_debit_flg',
                    'external_transaction_flg','tax_withheld_amt','foreign_tax_withheld_amt','transaction_status_ind','firm_id',
                    'active_flg','batch_dt','created_program_nm','created_user_nm','created_ts']
        p_src_df = p_src_df.select([col(c) for c in COLUMNS_TO_SELECT])
    return p_src_df

def columns_rename(p_src_df, table_name):
    if table_name in V_FACT_HOLDING:
        p_src_df = p_src_df.withColumnRenamed("pos_bal_flg", "qty_val_flg")
    elif table_name in V_FACT_CAPITAL_FLOW_FEE:
        p_src_df = p_src_df.withColumnRenamed("cf_fee_flg", "amt_flg")
    return p_src_df

def table_rename(p_table_name):
    if p_table_name in V_FACT_CAPITAL_FLOW_FEE:
        p_table_name = 'fact_capital_flow_fee'
    elif p_table_name in V_FACT_TRANSACTION:
         p_table_name = 'fact_transaction'       
    elif p_table_name in V_FACT_MV_MULTIPLIER:
         p_table_name = 'fact_mv_multipler'       
    return p_table_name

