C_S3_DIM_INSTRUMENT_EXCP_OUTPUT_LOC_PROP_KEY = "s3.dim_instrument.excp.output.location"
C_S3_DIM_INSTRUMENT_OUTPUT_LOC_PROP_KEY = "s3.dim_instrument.output.location"
C_S3_DTL_INSTRUMENT_REF_OUTPUT_LOCATION_PROP_KEY = "s3.dtl_instrument_ref.output.location"
C_S3_DTL_INSTRUMENT_REF_EXCP_LOCATION_PROP_KEY = "s3.dtl_instrument_ref.excp.output.location"
C_S3_FACT_SET_STG_EXCP_OUTPUT_LOCATION_PROP_KEY= "s3.fact_set.stg.excp.output.location"
C_S3_FACT_SET_STG_OUTPUT_LOCATION_PROP_KEY = "s3.factset_ror.stg.output.location"

C_GLUE_S3_DB_NAME_PROP_KEY = "glue.s3.db.name"
#Redshift Properties
C_GLUE_REDSHIFT_DB_NAME_PROP_KEY = "redshift.db.name"
C_GLUE_REDSHIFT_SCHEMA_NAME_PROP_KEY = "redshift.schema.name"
C_GLUE_RS_DB_CONNECTION_NAME_PROP_KEY = "redshift.connection.name"
# GreenPlum Properties
C_GLUE_GP_DB_NAME_PROP_KEY = "glue.gp.db.name"
C_GLUE_GP_SCHEMA_NAME_PROP_KEY = "glue.gp.schema.name"
C_GLUE_GP_DB_CONNECTION_NAME_PROP_KEY = "glue.gp.connection.name"

C_GLUE_S3_TABLE_DTL_INSTRUMENT_REF_PROP_KEY = "glue.s3.table.dtl_instrument_ref"
C_GLUE_S3_TABLE_MTDT_INSTRUMENT_ID_GEN_PROP_KEY = "glue.s3.table.mtdt_instrument_id_gen"
C_GLUE_S3_TABLE_DIM_SOURCE_SYSTEM_PROP_KEY = "glue.s3.table.dim_source_system"
C_GLUE_S3_TABLE_FACT_SETTING_PROP_KEY = "glue.s3.table.fact_setting"
C_GLUE_S3_TABLE_DIM_PROPERTY_VALUE_PROP_KEY= "glue.s3.table.dim_property_value"
C_GLUE_S3_TABLE_DIM_INSTRUMENT="glue.s3.table.dim_instrument"
C_GLUE_S3_TABLE_FACT_SET_PROP_KEY = "glue.s3.table.fact_set"
C_GLUE_S3_TABLE_BRDG_INSTRU_REL_PROP_KEY = "glue.s3.table.brdg_instrument_relation"

C_FILE_TYPE_BEING_PROCESSED_GL_PROP_KEY = 'FILE_TYPE'
C_RUN_VALIDATION_GL_PROP_KEY = "RUN_VALIDATION"
C_BATCH_DATE_PROP_KEY = 'BATCH_DATE'
C_CLIENT_NBR_GL_PROP_KEY = 'CLIENT_NUMBER'
C_RECORD_TYPE_GL_PROP_KEY = 'RECORD_TYPE'
C_ENVIRONMENT_GL_PROP_KEY = 'ENV'
C_SOURCE_SYSTEM_NAME_PROP_KEY = 'SOURCE_SYSTEM_NM'
C_EXTRACTS_FILE_PROP_KEY = 'PROP'
C_BATCH_TYPE_PROP_KEY = 'BATCH_TYPE'

GLUE_S3_TABLE_FACT_SET_PROP_KEY = "glue.s3.table.fact_set"
C_S3_FACT_PRICE_TBL_NME="glue.s3.table.fact_price"
C_PROPERTY_LIST = ["glue_catalog_config.properties", "s3_config.properties","env.properties"]

C_S3_FACT_BENCHMARK_ROR_OUTPUT_LOCATION_PROP_KEY = "s3.fact_benchmark_ror.output.location"
C_S3_FACT_BENCHMARK_ROR_EXCP_OUTPUT_LOCATION_PROP_KEY = "s3.fact_benchmark_ror.excp.output.location"


# Source: Pace Security, Target: Fact Entity class
C_S3_PACE_SEC_INPUT_LOCATION = "s3.pace_sec.input.location"
C_S3_FACT_CLASS_ENTITY_OUTPUT_LOCATION = "s3.fact_class_entity.output.location"
C_GLUE_S3_TABLE_DIM_CLASS_TYPE="glue.s3.table.dim_class_type"
C_S3_FACT_CLASS_ENTITY_TBL_NME= "glue.s3.table.fact_class_entity"
C_S3_PACE_SEC_EXCP_OUTPUT_LOCATION="s3.pace_sec.stg.excp.output.location"
C_S3_FACT_CLASS_ENTITY_EXCP_OUTPUT_LOCATION= "s3.fact_class_entity.excp.output.location"
C_S3_PACE_SEC_STG_OUTPUT_LOCATION = "s3.pace_sec.stg.output.location"

C_GLUE_S3_TABLE_DIM_CURRENCY_PROP_KEY= "glue.s3.table.dim_currency"


# Stub Generic Instrument Creation
C_S3_STUB_INSTRUMENT_SRC_STG_OUTPUT_LOC_PROP_KEY = 's3.{}.stg.output.location'
C_S3_STUB_ACCOUNT_SRC_STG_OUTPUT_LOC_PROP_KEY = 's3.{}.stg.output.location'
C_S3_STUB_INSTRUMENT_SRC_STG_EXCP_OUTPUT_LOC_PROP_KEY = 's3.{}.stg.excp.output.location'
C_GLUE_S3_INSTRUMENT_SRC_TABLE_PROP_KEY='glue.s3.table.{}'

C_S3_RIMES_STG_OUTPUT_LOCATION_PROP_KEY = "s3.rimes.stg.output.location"
C_S3_RIMES_INPUT_LOCATION_PROP_KEY= "s3.rimes.input.location"
C_S3_RIMES_STG_EXCP_OUTPUT_LOCATION_PROP_KEY ="s3.rimes.stg.excp.output.location"

C_GLUE_S3_TABLE_VEST_MARK_PROP_KEY='glue.s3.table.vest_mark'
C_S3_VEST_MARK_INPUT_LOCATION_PROP_KEY= "s3.vest_mark.input.location"
C_S3_VEST_MARK_STG_OUTPUT_LOCATION_PROP_KEY="s3.vest_mark.stg.output.location"
C_S3_VEST_MARK_STG_EXCP_OUTPUT_LOCATION_PROP_KEY="s3.vest_mark.stg.excp.output.location"
C_GLUE_S3_TABLE_DIM_SLEEVE_PROP_KEY= 'glue.s3.table.dim_sleeve'
C_S3_DIM_SLEEVE_OUTPUT_LOCATION_PROP_KEY= 's3.dim_sleeve.output.location'
C_S3_DIM_SLEEVE_EXCP_OUTPUT_LOCATION_PROP_KEY= 's3.dim_sleeve.excp.output.location'
C_GLUE_S3_TABLE_MTDT_SLEEVE_GEN_PROP_KEY='glue.s3.table.mtdt_sleeve_id_gen'
C_GLUE_S3_TABLE_DIM_ACCOUNT_PROP_KEY = 'glue.s3.table.dim_account'
C_GLUE_S3_TABLE_DTL_ACCOUNT_REF_PROP_KEY = 'glue.s3.table.dtl_account_ref'
C_GLUE_S3_TABLE_BRDG_ACC_RELATION_PROP_KEY = 'glue.s3.table.brdg_account_relation'
C_S3_BRDG_ACC_RELATION_OUTPUT_LOCATION_PROP_KEY="s3.brdg_account_relation.output.location"
C_S3_BRDG_ACC_RELATION_EXCP_OUTPUT_LOCATION_PROP_KEY="s3.brdg_account_relation.excp.output.location"
C_GLUE_S3_TABLE_FACT_ACC_DATE_PROP_KEY= "glue.s3.table.fact_account_date"
C_S3_FACT_ACC_DATE_OUTPUT_LOCATION_PROP_KEY="s3.fact_account_date.output.location"
C_S3_FACT_ACC_DATE_EXCP_OUTPUT_LOCATION_PROP_KEY="s3.fact_account_date.excp.output.location"


# Keybank
C_GLUE_S3_TABLE_DIM_SOR_TRANSACTION_TYPE_PROP_KEY= 'glue.s3.table.dim_sor_transaction_type'
C_GLUE_S3_TABLE_DIM_TRANSACTION_TYPE_PROP_KEY= "glue.s3.table.dim_transaction_type"
C_GLUE_S3_TABLE_DTL_TRANSACTION_BEHAVIOR_PROP_KEY= "glue.s3.table.dtl_transaction_behavior"
C_GLUE_S3_TABLE_DIM_ACCOUNT_TYPE_PROP_KEY= "glue.s3.table.dim_account_type"
C_GLUE_S3_TABLE_WRK_MV_MULTIPLIER_PIVOT = "glue.s3.table.wrk_mv_multiplier_pivot"
C_S3_FACT_SOR_HOLDING_OUTPUT_LOCATION_PROP_KEY= 's3.fact_sor_holding.output.location'
C_S3_FACT_SOR_HOLDING_EXCP_OUTPUT_LOCATION_PROP_KEY= 's3.fact_sor_holding.excp.output.location'

C_GLUE_S3_TABLE_DIM_PROPERTY_POP_KEY = "glue.s3.table.dim_property"
C_GLUE_S3_TABLE_DIM_CORP_ACTION_DEC_PROP_KEY = "glue.s3.table.dim_corp_action_decl"
C_GLUE_S3_TABLE_DTL_CORP_ACTION_DECL_SEC_RATE_PROP_KEY = "glue.s3.table.dtl_corp_action_decl_sec_rate"



#keytrans
C_S3_KEYTRANS_INPUT_LOCATION_PROP_KEY= "s3.keytrans.input.location"
C_S3_KEYTRANS_LTYPE_STG_OUTPUT_LOCATION_PROP_KEY="s3.keytrans_ltype.stg.output.location"
C_S3_KEYTRANS_LTYPE_STG_EXCP_OUTPUT_LOCATION_PROP_KEY="s3.keytrans_ltype.stg.excp.output.location"
C_S3_KEYTRANS_STYPE_STG_OUTPUT_LOCATION_PROP_KEY="s3.keytrans_stype.stg.output.location"
C_S3_KEYTRANS_STYPE_STG_EXCP_OUTPUT_LOCATION_PROP_KEY="s3.keytrans_stype.stg.excp.output.location"

# Account Stub
C_S3_DIM_ACCOUNT_EXCP_OUTPUT_LOCATION_PROP_KEY="s3.dim_account.excp.output.location"
C_S3_DIM_ACCOUNT_OUTPUT_LOCATION_PROP_KEY="s3.dim_account.output.location"

#keyrecon
C_S3_KEYRECON_INPUT_LOCATION_PROP_KEY= "s3.keyrecon.input.location"
C_S3_KEYRECON_STG_EXCP_OUTPUT_LOCATION_PROP_KEY= 's3.keyrecon.stg.excp.output.location'
C_S3_KEYRECON_STG_OUTPUT_LOCATION_PROP_KEY= 's3.keyrecon.stg.output.location'

#HPA
C_S3_HPA_EXCP_OUTPUT_LOCATION="s3.hpa.excp.output.location"
C_S3_HPA_INPUT_LOCATION_PROP_KEY="s3.hpa.input.location"
C_S3_STG_HOUSE_PRICE_OUTPUT_LOCATION="s3.hpa.stg.output.location"

#MSA
C_S3_MSA_INPUT_LOCATION_PROP_KEY = "s3.msa.input.location"
C_S3_MSA_STG_OUTPUT_LOCATION_PROP_KEY = "s3.msa.stg.output.location"
C_S3_MSA_EXCP_OUTPUT_LOCATION_KEY = "s3.msa.excp.output.location"
C_GLUE_S3_TABLE_DIM_QUANTITY_FACTOR_PROP_KEY= "glue.s3.table.dim_quantity_factor"


C_S3_STG_TRANSACTION_TODAY_OUTPUT_LOCATION= "s3.stg_transaction_today.output.location"
C_S3_STG_TRANSACTION_TODAY_EXCP_OUTPUT_LOCATION = "s3.stg_transaction_today.excp.output.location"
C_S3_FACT_TRANSACTION_OUTPUT_LOCATION= "s3.fact_transaction.output.location"
C_S3_STG_TRANSACTION_PARALLEL_INPUT_LOC="s3.div.stg_transaction_today.input.location"
C_GLUE_S3_TABLE_TRANS_STYPE = "glue.s3.table.trans_stype"
C_GLUE_S3_TABLE_TRANS_LTYPE = "glue.s3.table.trans_ltype"

C_S3_FACT_HOLDING_OUTPUT_LOCATION= "s3.fact_holding.output.location"
C_S3_FACT_HOLDING_EXCP_OUTPUT_LOCATION= "s3.fact_holding.excp.output.location"
C_S3_FACT_CAPITAL_FLOW_FEE_OUTPUT_LOCATION= "s3.fact_capital_flow_fee.output.location"
C_GLUE_S3_TABLE_DIM_TRANSACTION_GROUP="glue.s3.table.dim_transaction_group"

#MSB
C_S3_MSB_INPUT_LOCATION_PROP_KEY= "s3.msb.input.location"
C_S3_MSB_STG_OUTPUT_LOCATION_PROP_KEY= "s3.msb.stg.output.location"
C_S3_MSB_EXCP_OUTPUT_LOCATION_KEY = "s3.msb.excp.output.location"

#MSK
C_S3_MSK_INPUT_LOCATION_PROP_KEY="s3.msk.input.location"
C_S3_MSK_STG_OUTPUT_LOCATION_PROP_KEY="s3.msk.stg.output.location"
C_S3_MSK_EXCP_OUTPUT_LOCATION_KEY="s3.msk.excp.output.location"

#MSG
C_S3_MSG_INPUT_LOCATION_PROP_KEY="s3.msg.input.location"
C_S3_MSG_OUTPUT_LOCATION_PROP_KEY="s3.msg.stg.output.location"
C_S3_MSG_EXCP_LOCATION_PROP_KEY="s3.msg.excp.output.location"

#MSE
C_S3_MSE_EXCP_OUTPUT_LOCATION="s3.mse.excp.output.location"
C_S3_MSE_INPUT_LOCATION_PROP_KEY="s3.mse.input.location"
C_S3_STG_SEC_PRICE_OUTPUT_LOCATION="s3.mse.stg.output.location"

#MSF
C_S3_MSF_EXCP_OUTPUT_LOCATION="s3.msf.excp.output.location"
C_S3_MSF_INPUT_LOCATION_PROP_KEY="s3.msf.input.location"
C_S3_STG_SEC_INC_DECL_OUTPUT_LOCATION="s3.msf.stg.output.location"

#QF_FACT_PRICE
C_GLUE_S3_STG_SEC_INFO_LOCATION_PROP_KEY="glue.s3.table.stg_sec_info"
C_GLUE_S3_STG_SEC_DESC_LOCATION_PROP_KEY="glue.s3.table.stg_sec_desc"
C_GLUE_S3_STG_SEC_REF_LOCATION_PROP_KEY="glue.s3.table.stg_sec_ref"
C_GLUE_S3_STG_BOND_RATE_LOCATION_PROP_KEY="glue.s3.table.stg_bond_rate"
C_GLUE_S3_STG_AMORT_RATE_LOCATION_PROP_KEY="glue.s3.table.stg_amort_rate"
C_GLUE_S3_STG_UNDERLYING_SEC_INFO_LOCATION_PROP_KEY="glue.s3.table.stg_underlying_sec_info"
C_GLUE_S3_STG_SEC_PRICE_LOCATION_PROP_KEY="glue.s3.table.stg_sec_price"
C_GLUE_S3_STG_SEC_INC_DECL_LOCATION_PROP_KEY="glue.s3.table.stg_sec_inc_decl"
C_GLUE_S3_STG_HOUSE_PRICE_LOCATION_PROP_KEY="glue.s3.table.stg_house_price"


C_S3_QF_FACT_PRICE_OUTPUT_LOCATION_PROPERTY_KEY="s3.qf_fact_price.output.location"
C_S3_QF_FACT_PRICE_EXCP_LOCATION_PROPERTY_KEY="s3.fact_price.excp.output.location"

#FHP(Foreign House Pricing)
C_S3_FHP_EXCP_OUTPUT_LOCATION="s3.fhp.excp.output.location"
C_S3_FHP_INPUT_LOCATION_PROP_KEY="s3.fhp.input.location"
C_S3_FHP_HOUSE_PRICE_OUTPUT_LOCATION="s3.fhp.stg.output.location"

#Constant for S3 write partitioning
C_S3_REDSHIFT_LOAD_PATH="s3.redshift.load.path"

#MSC
C_S3_MSC_INPUT_LOCATION_PROP_KEY= "s3.msc.input.location"
C_S3_MSC_OUTPUT_LOCATION_PROP_KEY= "s3.msc.stg.output.location"
C_S3_MSC_EXCP_LOCATION_PROP_KEY= "s3.msc.excp.output.location"
C_S3_READSHIFT_LOAD_FILES_BASE_LOCATION= "s3.readshift_load_files.base.location"

#MSL
C_S3_MSL_EXCP_OUTPUT_LOCATION="s3.msl.excp.output.location"
C_S3_MSL_INPUT_LOCATION_PROP_KEY="s3.msl.input.location"
C_S3_MSL_STG_OUTPUT_LOCATION="s3.msl.stg.output.location"
C_S3_FACT_SET_INPUT_LOCATION ="s3.fact_set.input.location"

C_S3_STG_AMORT_RATE_STAGING_LOCATION_PROP_KEY="glue.s3.table.stg_amort_rate"
C_S3_STG_BOND_RATE_STAGING_LOCATION_PROP_KEY="glue.s3.table.stg_bond_rate"
C_S3_FACT_PRICE_OUTPUT_LOCATION_PROP_KEY="s3.fact_price.output.location"
C_S3_FACT_PRICE_EXCP_OUTPUT_LOCATION="s3.fact_price.excp.output.location"
C_GLUE_S3_TABLE_STG_SEC_PRICE_PROP_KEY= "glue.s3.table.stg_sec_price"
C_GLUE_S3_TABLE_STG_HOUSE_PRICE_PROP_KEY= "glue.s3.table.stg_house_price"
C_GLUE_S3_TABLE_DIM_INSTRUMENT_PROP_KEY= "glue.s3.table.dim_instrument"


C_S3_DTL_INSTRUMENT_DESC_OUTPUT_LOCATION_PROP_KEY="s3.dtl_instrument_desc.output.location"
C_S3_DTL_INSTRUMENT_DESC_EXCP_LOCATION_PROP_KEY="s3.dtl_instrument_desc.excp.output.location"
C_GLUE_S3_TABLE_DTL_INSTRUMENT_DESC_PROP_KEY="glue.s3.table.dtl_instrument_desc"
C_S3_STG_SEC_DESC_STAGING_LOCATION_PROP_KEY="glue.s3.table.stg_sec_desc"

#B2D
C_S3_B2D_INPUT_LOCATION_PROP_KEY="s3.b2d.input.location"
C_S3_B2D_OUTPUT_LOCATION_PROP_KEY="s3.b2d.stg.output.location"
C_S3_B2D_EXCP_LOCATION_PROP_KEY="s3.b2d.excp.output.location"

#B2E
C_S3_B2E_INPUT_LOCATION_PROP_KEY="s3.b2e.input.location"
C_S3_B2E_OUTPUT_LOCATION_PROP_KEY="s3.b2e.stg.output.location"
C_S3_B2E_EXCP_LOCATION_PROP_KEY="s3.b2e.excp.output.location"

#UCA
C_S3_UCA_INPUT_LOCATION_PROP_KEY="s3.uca.input.location"
C_S3_UCA_OUTPUT_LOCATION_PROP_KEY="s3.uca.stg.output.location"
C_S3_UCA_EXCP_LOCATION_PROP_KEY="s3.uca.excp.output.location"

#HIA
C_S3_HIA_INPUT_LOCATION_PROP_KEY="s3.hia.input.location"
C_S3_HIA_OUTPUT_LOCATION_PROP_KEY="s3.hia.stg.output.location"
C_S3_HIA_EXCP_LOCATION_PROP_KEY="s3.hia.excp.output.location"

#NRB
C_S3_NRB_INPUT_LOCATION_PROP_KEY="s3.nrb.input.location"
C_S3_NRB_OUTPUT_LOCATION_PROP_KEY="s3.nrb.stg.output.location"
C_S3_NRB_EXCP_LOCATION_PROP_KEY="s3.nrb.excp.output.location"

#H2A
C_S3_H2A_INPUT_LOCATION_PROP_KEY="s3.h2a.input.location"
C_S3_H2A_OUTPUT_LOCATION_PROP_KEY="s3.h2a.stg.output.location"
C_S3_H2A_EXCP_LOCATION_PROP_KEY="s3.h2a.excp.output.location"

#TIPS
C_S3_TIPS_INPUT_LOCATION_PROP_KEY="s3.tips.input.location"
C_S3_TIPS_STG_OUTPUT_LOCATION_PROP_KEY="s3.tips.stg.output.location"
C_S3_TIPS_STG_EXCP_OUTPUT_LOCATION_PROP_KEY="s3.tips.stg.excp.output.location"

#Security stub creation conversion
C_S3_PACEHIST_INPUT_LOCATION_PROP_KEY= "s3.pacehist.input.location"
C_S3_PACEHIST_STG_OUTPUT_LOCATION_PROP_KEY= "s3.pacehist.stg.output.location"
C_S3_SECLINK_INPUT_LOCATION_PROP_KEY= "s3.seclink.input.location"

#PFOLVAL Account Sleeve
C_S3_PFOLVAL_INPUT_LOCATION_PROP_KEY="s3.pfolval.input.location"
C_S3_WRK_FACT_HOLDING_OUTPUT_LOCATION_PROP_KEY = "s3.wrk_fact_holding.output.location"

C_S3_FACT_MV_MULTIPLIER_EXCP_OUTPUT_LOCATION_PROP_KEY= "s3.fact_mv_multiplier.excp.output.location"
C_S3_FACT_MV_MULTIPLIER_OUTPUT_LOCATION_PROP_KEY= "s3.fact_mv_multiplier.output.location"

#Breaks
C_S3_BREAKS_INPUT_LOCATION_PROP_KEY= 's3.breaks.input.location'
#Cashflow
C_S3_CASHFLOW_INPUT_LOCATION_PROP_KEY= "s3.cashflow.input.location"
C_S3_ADVTCF_INPUT_LOCATION_PROP_KEY= "s3.advtcf.input.location"
C_S3_FACT_CAPITAL_FLOW_FEE_EXCP_OUTPUT_LOCATION_PROP_KEY= "s3.fact_capital_flow_fee.excp.output.location"

#pfoljoin
C_S3_PFOLJOIN_INPUT_LOCATION_PROP_KEY= "s3.pfoljoin.input.location"

#RTNVAL(KPST1900)
C_S3_POSVAL_INPUT_LOCATION_PROP_KEY= "s3.posval.input.location"
C_S3_STG_POSVAL_OUTPUT_LOCATION_PROP_KEY = "s3.posval.stg.output.location"
C_S3_DIM_STANDARD_CODE_PROP_KEY = "glue.s3.table.dim_standard_code"

#RTNVAL(KPST1900)
C_S3_RTNVAL_INPUT_LOCATION_PROP_KEY= 's3.rtnval.input.location'
C_S3_FACT_ROR_HIST_EXCP_OUTPUT_LOCATION_PROP_KEY= 's3.fact_hist_ror.excp.output.location'
C_S3_FACT_ROR_HIST_OUTPUT_LOCATION_PROP_KEY= 's3.fact_hist_ror.output.location'
C_S3_POSVAL_STUB_DISTINCT_STG_OUTPUT_LOCATION_PROP_KEY = "s3.posval.stub.distinct.stg.output.location"
C_S3_POSVAL_STG_FACT_HOLDING_OUTPUT_LOCATION_PROP_KEY = "s3.posval.stg_fact_holding.output.location"

#SECRTN & SECVAL
C_S3_SECRTN_INPUT_LOCATION_PROP_KEY= 's3.secrtn.input.location'
C_S3_SECVAL_INPUT_LOCATION_PROP_KEY= 's3.secval.input.location'
C_S3_SECFLOW_INPUT_LOCATION_PROP_KEY='s3.secflow.input.location'

#WTDFLOW file for RTNVAL
C_S3_WTDFLOW_INPUT_LOCATION_PROP_KEY= 's3.wtdflow.input.location'

#H2B
C_S3_H2B_INPUT_LOCATION_PROP_KEY="s3.h2b.input.location"
C_GLUE_S3_TABLE_MTDT_EXCLUDE_ACCOUNT_PROP_KEY="glue.s3.table.mtdt_exclude_account"
C_S3_STG_BOOK_KEEPING_TRANS_EXCP_LOCATION_PROP_KEY="s3.h2b.excp.output.location"
C_S3_STG_BOOK_KEEPING_TRANS_OUTPUT_LOCATION_PROP_KEY="s3.h2b.stg.output.location"

#HJA_STG_TRANS_TRAILER_JONR
C_S3_HJA_INPUT_LOCATION_PROP_KEY="s3.hja.input.location"
C_S3_HJA_STG_TRANS_TRAILER_JONR_EXCP_OUTPUT_LOCATION_PROP_KEY="s3.hja.excp.output.location"
C_S3_HJA_STG_TRANS_TRAILER_JONR_OUTPUT_LOCATION_PROP_KEY="s3.hja.stg.output.location"

#HOA
C_S3_HOA_INPUT_LOCATION_PROP_KEY="s3.hoa.input.location"
C_S3_HOA_STG_HOLDING_EXCP_OUTPUT_LOCATION_PROP_KEY= 's3.hoa.excp.output.location'
C_S3_HOA_STG_HOLDING_OUTPUT_LOCATION_PROP_KEY= 's3.hoa.stg.output.location'

C_S3_AVEJOE_ADV_INPUT_LOCATION_PORP_KEY ="s3.avejoe_adv.input.location"
C_S3_AVEJOE_TOT_INPUT_LOCATION_PORP_KEY ="s3.avejoe_tot.input.location"
C_S3_AVEJOE_SLV_INPUT_LOCATION_PORP_KEY="s3.avejoe_slv.input.location"
C_S3_AVEJOE_ACC_EXTRACT_OUTPUT_LOCATION_PORP_KEY ="s3.avejoe_acc_extract.output.location"
C_S3_AVEJOE_ACC_ADV_TOT_EXTRACT_OUTPUT_LOCATION_PROP_KEY="s3.avejoe_acc_adv_tot_extract.output.location"
C_S3_AVEJOE_SLV_ACC_EXTRACT_OUTPUT_LOCATION_PROP_KEY="s3.avejoe_slv_acc_extract.output.location"

#BAB
C_S3_BAB_INPUT_LOCATION_PROP_KEY="s3.bab.input.location"
C_S3_BAB_STG_HOLDING_EXCP_OUTPUT_LOCATION_PROP_KEY= 's3.bab.excp.output.location'
C_S3_BAB_STG_HOLDING_OUTPUT_LOCATION_PROP_KEY= 's3.bab.stg.output.location'

# Account Stub Creation for batch files
C_GLUE_S3_TABLE_DIM_SOR_FUND_REF_PROP_KEY= "glue.s3.table.dim_sor_fund_ref"
C_GLUE_S3_TABLE_DIM_ADVISOR_PROP_KEY="glue.s3.table.dim_advisor"
C_GLUE_S3_TABLE_DIM_BUSINESS_UNIT_PROP_KEY="glue.s3.table.dim_business_unit"
C_S3_DIM_ADVISOR_STUB_OUTPUT_LOCATION_PROP_KEY='s3.dim_advisor.output.location'
C_S3_BUSINESS_UNIT_OUTPUT_LOCATION_PROP_KEY= 's3.dim_business_unit.output.location'
C_S3_DIM_BUSINESS_UNIT_EXCP_LOCATION_PROP_KEY='s3.dim_business_unit.excp.output.location'
C_S3_DIM_ADVISOR_EXCP_LOCATION_PROP_KEY='s3.dim_advisor.excp.output.location'


#CAPS
C_S3_CAPS_INPUT_LOCATION_PROP_KEY="s3.caps.input.location"
C_S3_CAPS_STG_EXCP_OUTPUT_LOCATION_PROP_KEY="s3.caps.stg.excp.output.location"
C_GLUE_S3_TABLE_DIM_SOR_CAPS_CLASS_REF="glue.s3.table.dim_sor_caps_class_ref"
C_GLUE_S3_TABLE_DIM_SOR_GICS_CLASS_REF="glue.s3.table.dim_sor_gics_class_ref"
C_GLUE_S3_TABLE_DIM_SOR_INDUSTRY_GRP_REF="glue.s3.table.dim_sor_industry_grp_ref"
C_GLUE_S3_TABLE_DIM_SOR_PROD_CD_REF="glue.s3.table.dim_sor_prod_cd_ref"
C_GLUE_S3_TABLE_DIM_SOR_CAPS_CLASS_TYPE_REF="glue.s3.table.dim_sor_caps_class_type_ref"


C_GLUE_S3_TABLE_DIM_INSTUMENT="glue.s3.table.dim_instrument"

C_GLUE_S3_TABLE_DIM_SOR_MARKET_GROUP_REF="glue.s3.table.dim_sor_market_group_ref"
C_GLUE_S3_TABLE_DIM_SOR_SECURITY_ASSET_CLASS_REF="glue.s3.table.dim_sor_security_asset_class_ref"
C_GLUE_S3_TABLE_DIM_SOR_SECURITY_CLASS_REF="glue.s3.table.dim_sor_security_class_ref"


C_S3_529_DIVIDEND_TRANS_INPUT_LOCATION_PROP_KEY = "s3.529_dividend_trans.input.location"
C_S3_529_DIVIDEND_TRANS_STG_LOCATION_PROP_KEY = "s3.529_dividend_trans.stg.location"
C_S3_529_DIVIDEND_TRANS_EXCP_LOCATION_PROP_KEY = "s3.529_dividend_trans.excp.location"


C_S3_529_NSCC_TRANS_INPUT_LOCATION_PROP_KEY = "s3.529_nscc_trans.input.location"
C_S3_529_NSCC_TRANS_STG_LOCATION_PROP_KEY = "s3.529_nscc_trans.stg.location"
C_S3_529_NSCC_TRANS_EXCP_LOCATION_PROP_KEY = "s3.529_nscc_trans.excp.location"


C_S3_529_POSITIONS_INPUT_LOCATION_PROP_KEY = "s3.529_positions.input.location"
C_S3_529_POSITIONS_STG_LOCATION_PROP_KEY = "s3.529_positions.stg.location"
C_S3_529_POSITIONS_EXCP_LOCATION_PROP_KEY = "s3.529_positions.excp.location"

#Annuties
C_S3_ANNUITIES_TXN_INPUT_LOCATION_PROP_KEY= "s3.annuities_txn.input.location"
C_S3_ANNUITIES_POS_INPUT_LOCATION_PROP_KEY= "s3.annuities_pos.input.location"
C_S3_STG_ANNUITIES_TXN_EXCP_LOCATION_PROP_KEY= "s3.annuities_txn.excp.output.location"
C_S3_STG_ANNUITIES_POS_EXCP_LOCATION_PROP_KEY= "s3.annuities_pos.excp.output.location"
C_S3_STG_ANNUITIES_TXN_OUTPUT_LOCATION_PROP_KEY= "s3.annuities_txn.stg.output.location"
C_S3_STG_ANNUITIES_POS_OUTPUT_LOCATION_PROP_KEY= "s3.annuities_pos.stg.output.location"

C_S3_CAPS_STG_OUTPUT_LOCATION_PROP_KEY="s3.caps.stg.output.location"

C_S3_STG_SEC_REF_INPUT_LOCATION_PROP_KEY="s3.msc.stg.output.location"
C_S3_FUND5504_INPUT_LOCATION_PROP_KEY="s3.fund5504.input.location"

C_S3_LOOKUP_BASE_LOCATION_PROP_KEY= "s3.lookup.base.location"
C_S3_LOOKUP_TABLE_CONFIG_LOCATION_PROP_KEY = "s3.lookup.table.config.location"

C_S3_MTDT_BATCH_DATE_INPUT_FILE_PROP_KEY="s3.mtdt_batch_date.input.location"
C_GLUE_S3_TABLE_MTDT_BATCH_DATE_PROP_KEY="glue.s3.table.mtdt_batch_date"
C_S3_MTDT_BATCH_DATE_OUTPUT_LOCATION_PROP_KEY="s3.mtdt_batch_date.output.location"

C_S3_ACPTCF_INPUT_LOCATION_PROP_KEY= "s3.acptcf.input.location"

#DTL_FI_SCHEDULE
C_GLUE_S3_TABLE_DTL_FI_SCHEDULE_PROP_KEY="glue.s3.table.dtl_fi_schedule"
C_S3_DTL_FI_SCHEDULE_OUTPUT_LOCATION_PROP_KEY= "s3.dtl_fi_schedule.output.location"
C_S3_DTL_FI_SCHEDULE_EXCP_OUTPUT_LOCATION_PROP_KEY="s3.dtl_fi_schedule.excp.output.location"



C_S3_STG_TRANSACTION_TODAY_OUTPUT_REDSHIFT_LOCATION="s3.stg_transaction_today.output.redshift.location"

#ZMFRSP_DIM_MUTUAL_FUND_XREF
C_S3_STG_MUTUAL_FUND_XREF_INPUT_LOCATION_PROP_KEY="s3.stg_mutual_fund_xref.input.location"
C_S3_STG_MUTUAL_FUND_XREF_OUTPUT_LOCATION_PROP_KEY="s3.stg_mutual_fund_xref.stg.output.location"
C_S3_STG_MUTUAL_FUND_XREF_EXCP_LOCATION_PROP_KEY="s3.stg_mutual_fund_xref.excp.location"

C_S3_DIM_MUTUAL_FUND_XREF_OUTPUT_LOCATION_PROP_KEY="s3.dim_mutual_fund_xref.output.location"
C_S3_DIM_MUTUAL_FUND_XREF_EXCP_LOCATION_PROP_KEY="s3.dim_mutual_fund_xref.excp.location"

C_REDSHIFT_DB_NAME_PROP_KEY="redshift.db.name"
C_REDSHIFT_SCHEMA_NAME_PROP_KEY="redshift.schema.name"
C_REDSHIFT_CONNECTION_NAME_PROP_KEY="redshift.connection.name"

#PACEHIST to DTL_PROGRAM_ASSET Conversion
C_S3_DTL_PROGRAM_ASSET_OUTPUT_LOCATION_PROP_KEY= "s3.dtl_program_asset.output.location"
C_S3_DTL_PROGRAM_ASSET_EXCP_OUTPUT_LOCATION_PROP_KEY= "s3.dtl_program_asset.excp.location"

#msf_msk_dim_corp_action_decl
C_GLUE_S3_TABLE_DIM_SOR_CORP_ACTION_TYPE_REF_PROP_KEY="glue.s3.table.dim_sor_corp_action_type_ref"
C_GLUE_S3_TABLE_DIM_CORP_ACTION_DECL_PROP_KEY="glue.s3.table.dim_corp_action_decl"
C_GLUE_S3_TABLE_MTDT_CORP_ACTION_ID_GEN_PROP_KEY="glue.s3.table.mtdt_corp_action_decl_id_gen"
C_GLUE_MTDT_AUTO_UPDATE_PROP_KEY="glue.s3.table.mtdt_auto_update"
C_S3_DIM_CORP_ACTION_DECL_EXCP_OUTPUT_LOCATION_KEY="s3.dim_corp_action_decl_excp.output.location"
C_S3_DIM_CORP_ACTION_DECL_OUTPUT_LOCATION_KEY="s3.dim_corp_action_decl.output.location"

#SECLINK to DTL_INSTRUMENT_REF
C_S3_BRDG_INSTRU_REL_EXCP_LOCATION_PROP_KEY="s3.brdg_instrument_relation.excp.location"
C_S3_BRDG_INSTRU_REL_OUTPUT_LOCATION_PROP_KEY="s3.brdg_instrument_relation.output.location"

# extract generator util
C_S3_EXTRACTS_CONFIG_LOCATION_PROP_KEY="s3.extracts.config.location"
C_S3_EXTRACTS_TEMP_OUTPUT_LOCATION_PROP_KEY="s3.extracts.temp.output.location"
#acphist
C_S3_ACPHIST_INPUT_LOCATION_PROP_KEY="s3.acphist.input.location"
C_S3_BRDG_INSTRU_REL_OUTPUT_LOCATION_PROP_KEY="s3.brdg_instrument_relation.output.location"

#SEC_DIM_INSTRUMENT
C_GLUE_S3_TABLE_DIM_COUPON_FREQ_CODES_PROP_KEY="glue.s3.table.dim_coupon_freq_codes"
C_GLUE_S3_TABLE_DIM_SOR_DAY_COUNT_REF_PROP_KEY="glue.s3.table.dim_sor_day_count_ref"
C_S3_DTL_INSTRUMENT_DESC_OUTPUT_LOC_PROP_KEY="s3.dtl_instrument_desc.output.location"
C_S3_WRK_RECALCULATE_OUTPUT_LOC_PROP_KEY = "s3.wrk_recalculate.output.location"

#ai_extracts
C_S3_WMAPBPSAIEXTRACTS_OUTPUT_LOCATION_PROP_KEY="s3.wmapbpsaiextract.output.location"
#WMAPBPSAI
C_S3_WMAPBPSAI_INPUT_LOCATION_PROP_KEY="s3.wmapbpsai.input.location"
C_S3_WMAPBPSAI_PARALLEL_INPUT_LOCATION_PROP_KEY="s3.wmapbpsai.parallel.input.location"
C_S3_WMAPBPSAI_STG_EXCP_LOCATION_PROP_KEY="s3.wmapbpsai.excp.output.location.key"
C_S3_WMAPBPSAI_STG_OUTPUT_LOCATION_PROP_KEY="s3.wmapbpsai.stg.output.location"
C_GLUE_S3_TABLE_WRK_RECALC_AI_INSTR_DATES_PROP_KEY = "glue.s3.table.wrk_recalc_ai_instr_dates"

#ACPRTN
C_S3_ACPRTN_INPUT_LOCATION_PROP_KEY="s3.acprtn.input.location"
C_S3_ACPVAL_INPUT_LOCATION_PROP_KEY="s3.acpval.input.location"
C_S3_ACPFLOW_INPUT_LOCATION_PROP_KEY="s3.acpflow.input.location"

#pfoljoin

C_S3_DIM_ACCOUNTLINK_EXCP_OUTPUT_LOCATION_PROP_KEY="s3.dim_account_link.excp.output.location"
C_S3_DIM_ACCOUNT_LINK_OUTPUT_LOCATION_PROP_KEY="s3.dim_account_link.output.location"

#avejoe_tot_adv_security_ror
C_S3_AVEJOE_TOT_ADV_SECURITY_ROR_INPUT_LOCATION_PORP_KEY="s3.avejoe_tot_adv_security_ror.input.location"
C_S3_AVEJOE_TOT_ADV_SECURITY_ROR_OUTPUT_LOCATION_PORP_KEY="s3.avejoe_tot_adv_security_ror.output.location"

C_S3_WRK_REPROCESS_CORP_OUTPUT_LOCATION="s3.wrk_reprocess_corp.output.location"

#B2B
C_S3_B2B_INPUT_LOCATION_PROP_KEY = "s3.b2b.input.location"
C_S3_STG_COMMON_TRAN_INFO_EXCP_LOCATION_PROP_KEY="s3.b2b.excp.output.location"
C_S3_STG_COMMON_TRAN_INFO_OUTPUT_LOCATION_PROP_KEY="s3.b2b.stg.output.location"

#B2F
C_S3_B2F_INPUT_LOCATION_PROP_KEY = "s3.b2f.input.location"
C_S3_STG_COMMISSION_INFO_EXCP_LOCATION_PROP_KEY="s3.b2f.excp.output.location"
C_S3_STG_COMMISSION_INFO_OUTPUT_LOCATION_PROP_KEY="s3.b2f.stg.output.location"

C_GLUE_S3_TABLE_MTDT_EXCLUDE_INSTR_PROP_KEY = "glue.s3.table.mtdt_exclude_instr"
C_GLUE_S3_TABLE_MTDT_AUTO_UPDATE_PROP_KEY = "glue.s3.table.mtdt_auto_update"


#asset class avejoe security ror
C_S3_AVEJOE_ASSET_CLASS_INPUT_LOCATION_PORP_KEY="s3.avejoe_asset_class.input.location"
C_S3_AVEJOE_ASSET_CLASS_EXTRACT_OUTPUT_LOCATION_PORP_KEY="s3.avejoe_asset_class.output.location"



C_S3_COMP_PERF_STG_EXCP_LOCATION_PROP_KEY= "s3.comp_perf.stg.excp.location"
C_S3_COMP_ROR_STG_EXCP_LOCATION_PROP_KEY= "s3.comp_ror.stg.excp.location"

#composite_stub
C_S3_COMPACCESS_INPUT_LOCATION_PROP_KEY = "s3.comp_access.input.location"
C_S3_COMP_ROR_INPUT_LOCATION_PROP_KEY="s3.comp_ror.input.location"
C_S3_COMP_PERF_INPUT_LOCATION_PROP_KEY="s3.comp_perf.input.location"
C_GLUE_S3_TABLE_DIM_ACCOUNT_GROUP_PROP_KEY="glue.s3.table.dim_account_group"
C_S3_DIM_ACCOUNT_GROUP_OUTPUT_LOCATION_PROP_KEY="s3.dim_account_group.output.location"

#Instrument_id_gen
C_GLUE_S3_TABLE_MTDT_INSTR_ID_GEN_PROP_KEY="glue.s3.table.mtdt_instr_id_gen"
C_S3_MTDT_INSTR_ID_GEN_OUTPUT_LOCATION_PROP_KEY = "s3.mtdt_instr_id_gen.output.location"

#Multi_currency
C_GLUE_S3_TABLE_FACT_MV_MULTIPLIER="glue.s3.table.fact_mv_multiplier"

C_S3_FACT_ENTITY_VALUE_OUTPUT_LOCATION= "s3.fact_entity_value.output.location"
C_S3_FACT_ENTITY_VALUE_EXCP_OUTPUT_LOCATION = "s3.fact_entity_value.excp.output.location"

#Annuities ref table
C_GLUE_S3_TABLE_DIM_SOR_CHAR_AMT_REF_PROP_KEY = "glue.s3.table.dim_sor_char_amt_ref"
# BENCHMARK FILES
C_S3_BEN350_INPUT_LOCATION_PROP_KEY='s3.ben350.input.location'
C_S3_BEN350_DELTA_INPUT_LOCATION_PROP_KEY='s3.ben350_delta.input.location'
C_S3_BEN800_INPUT_LOCATION_PROP_KEY='s3.ben800.input.location'
C_S3_BEN800_DELTA_INPUT_LOCATION_PROP_KEY='s3.ben800_delta.input.location'
C_S3_BEN_MONTHLY_INPUT_LOCATION_PROP_KEY='s3.ben_monthly.input.location'
C_S3_BENCHMARK_RETURNS_DAILY_INPUT_LOCATION_PROP_KEY = "s3.benchmark_returns_daily.input.location"
C_S3_BENCHMARK_0215_INPUT_LOCATION_PROP_KEY = "s3.benchmark_0215.input.location"

C_S3_KEYBANK_ROR_INPUT_LOCATION_PROP_KEY="s3.keybank_ror.input.location"
C_S3_KEYBANK_PERF_INPUT_LOCATION_PROP_KEY="s3.keybank_perf.input.location"

C_S3_KEYBANK_PERF_STG_EXCP_LOCATION_PROP_KEY= "s3.keybank_perf.stg.excp.location"
C_S3_KEYBANK_ROR_STG_EXCP_LOCATION_PROP_KEY= "s3.keybank_ror.stg.excp.location"


#pace_eligible
C_S3_PACE_ELIGIBLE_INPUT_LOCATION_PROP_KEY="s3.pace_eligible.input.location"
C_S3_PACE_ELIGIBLE_EXCP_LOCATION_PROP_KEY="s3.pace_eligible.excp.output.location"
C_S3_PACE_ELIGIBLE_STG_LOCATION_PROP_KEY="s3.pace_eligible.stg.output.location"


#fund_merger
C_S3_FUND_MERGER_INPUT_LOCATION_PROP_KEY="s3.fund_merger.input.location"
C_S3_FUND_MERGER_EXCP_LOCATION_PROP_KEY="s3.fund_merger.excp.output.location"
C_S3_FUND_MERGER_STG_LOCATION_PROP_KEY="s3.fund_merger.stg.output.location"

#kill & keep
C_S3_KILL_KEEP_INPUT_LOCATION_PROP_KEY="s3.kill_keep.input.location"
C_S3_KILL_KEEP_EXCP_LOCATION_PROP_KEY="s3.kill_keep.excp.output.location"
C_S3_KILL_KEEP_STG_LOCATION_PROP_KEY="s3.kill_keep.stg.output.location"


#Corp action id gen
C_GLUE_S3_TABLE_MTDT_CORP_ACTN_DECL_ID_GEN_PROP_KEY="glue.s3.table.mtdt_corp_actn_decl_id_gen"
C_S3_MTDT_CORP_ACTN_DECL_ID_GEN_OUTPUT_LOCATION_PROP_KEY="s3.mtdt_corp_actn_decl_id_gen.output.location"

#PBSV0400
C_S3_BEN_PBSV0400_INPUT_LOCATION='s3.ben_pbsv0400.input.location'
C_S3_BEN_PBSV0400_DELTA_INPUT_LOCATION='s3.ben_pbsv0400_delta.input.location'

#PBSV0450
C_S3_BEN_PBSV0450_INPUT_LOCATION='s3.ben_pbsv0450.input.location'
C_S3_BEN_PBSV0450_DELTA_INPUT_LOCATION='s3.ben_pbsv0450_delta.input.location'

C_S3_BEN_PBSV0450_STG_EXCP_INPUT_LOCATION='s3.ben_pbsv0450.stg.excp.input.location'
C_S3_BEN_PBSV0600_INPUT_LOCATION='s3.ben_pbsv0600.input.location'
C_S3_BEN_PBSV0600_DELTA_INPUT_LOCATION='s3.ben_pbsv0600_delta.input.location'

#universe_conversion
C_S3_UNIVERSE_INPUT_LOCATION_PROP_KEY ="s3.universe.input.location"
C_S3_FACT_UNIVERSE_COMPARISON_OUTPUT_LOCATION_PROP_KEY="s3.fact_universe_comparison.output.location"
C_S3_FACT_UNIVERSE_COMPARISON_EXCP_OUTPUT_LOCATION="s3.fact_universe_comparison.excp.location"
C_GLUE_S3_TABLE_DIM_UNIVERSE_PROP_KEY="glue.s3.table.dim_universe"


#benchmark 0300
C_S3_CONV_BENCHMARK_0300_INPUT_LOCATION_PROP_KEY= "s3.conv_benchmark_0300.input.location"
C_S3_CONV_BENCHMARK_0300_DELTA_INPUT_LOCATION_PROP_KEY="s3.conv_benchmark_0300_delta.input.location"
C_S3_CONV_BENCHMARK_0300_DLYMLY_INPUT_LOCATION_PROP_KEY= "s3.conv_benchmark_0300.dlymly.input.location"
C_GLUE_S3_TABLE_DIM_SOR_BEN_GROUP_CD_REF_PROP_KEY="glue.s3.table.dim_sor_ben_group_cd_ref"


#revport
C_S3_REVPORT_INPUT_LOCATION_PROP_KEY = "s3.revport.input.location"
C_S3_STG_REVPORT_EXCP_LOCATION_PROP_KEY="s3.revport.stg.excp.output.location"
C_S3_STG_REVPORT_OUTPUT_LOCATION_PROP_KEY="s3.revport.stg.output.location"

#Sleeve id gen
C_GLUE_S3_TABLE_MTDT_SLEVE_ID_GEN_PROP_KEY="glue.s3.table.mtdt_sleve_id_gen"
C_S3_MTDT_SLEEVE_ID_GEN_OUTPUT_LOCATION_PROP_KEY="s3.mtdt_sleeve_id_gen.output.location"
C_GLUE_S3_TABLE_DIM_FUND_SLEEVE_PROP_KEY="glue.s3.table.dim_fund_sleeve"
C_S3_DIM_FUND_SLEEVE_OUTPUT_LOCATION_PROP_KEY="s3.dim_fund_sleeve.output.location"
C_S3_DIM_FUND_SLEEVE_EXCP_OUTPUT_LOC_PROP_KEY = "s3.dim_fund_sleeve.excp.output.location"

#keybank_uan
C_S3_KEYBANK_UAN_INPUT_LOCATION_PROP_KEY="s3.keybank_uan.input.location"
C_S3_KEYBANK_UAN_STG_OUTPUT_LOCATION_PROP_KEY="s3.keybank_uan.stg.ouput.location"
C_S3_KEYBANK_UAN_STG_EXCP_LOCATION_PROP_KEY="s3.keybank_uan.excp.output.location"
C_S3_DTL_ACCOUNT_REF_OUTPUT_LOCATION_PROP_KEY="s3.dtl_account_ref.output.location"
C_S3_DTL_ACCOUNT_REF_EXCP_OUTPUT_LOCATION_PROP_KEY="s3.dtl_account_ref.excp.location"

#Toolset A2I
C_S3_TOOLSET_A2I_INPUT_LOCATION_PROP_KEY="s3.toolset_a2i.input.location"
C_S3_TOOLSET_A2I_STG_OUTPUT_LOCATION_PROP_KEY="s3.toolset_a2i.stg.output.location"
C_S3_TOOLSET_A2I_STG_EXCP_LOCATION_PROP_KEY="s3.toolset_a2i.excp.location"

#foa_universe_brdg_entity_relation
C_S3_FOA_UNIVERSE_BRDG_ENTITY_RELATION_INPUT_LOCATION_PROP_KEY="s3.foa_universe_brdg_entity_relation.input.location"
C_GLUE_S3_TABLE_DIM_FOA_PROP_KEY="glue.s3.table.dim_foa"
C_GLUE_S3_TABLE_BRDG_ENTITY_RELATION_PROP_KEY="glue.s3.table.brdg_entity_relation"
C_S3_FOA_UNIVERSE_BRDG_ENTITY_RELATION_STG_OUTPUT_LOCATION_PROP_KEY="s3.foa_universe_brdg_entity_relation.stg.output.location"
C_S3_FOA_UNIVERSE_BRDG_ENTITY_RELATION_STG_EXCP_LOCATION="s3.foa_universe_brdg_entity_relation.stg.excp.location"
C_S3_BRDG_ENTITY_RELATION_OUTPUT_LOCATION_PROP_KEY="s3.brdg_entity_relation.output.location"
C_S3_DIM_UNIVERSE_OUTPUT_LOCATION_PROP_KEY="s3.dim_universe.output.location"
C_S3_DIM_FOA_OUTPUT_LOCATION_PROP_KEY="s3.dim_foa.output.location"

C_GLUE_S3_WRK_PACE_ELIGIBLE_STG_OUTPUT_LOCATION="s3.wrk_pace_eligible.output.redshift.location"
C_GLUE_S3_TABLE_WRK_PACE_ELIGIBLE = "glue.s3.table.wrk_pace_eligible"
C_GLUE_S3_TABLE_DTL_PROGRAM_ASSET="glue.s3.table.dtl_program_asset"

#benchmark asset class
C_S3_ASSETCLASS_TIER1_INPUT_LOCATION_PROP_KEY="s3.assetclass_tier1.input.location"
C_S3_ASSETCLASS_TIER2_INPUT_LOCATION_PROP_KEY="s3.assetclass_tier2.input.location"
C_S3_ASSETCLASS_TIER3_INPUT_LOCATION_PROP_KEY="s3.assetclass_tier3.input.location"
C_S3_ASSETCLASS_TIER4_INPUT_LOCATION_PROP_KEY="s3.assetclass_tier4.input.location"

C_S3_ASSETCLASS_TIER1_DELTA_INPUT_LOCATION_PROP_KEY="s3.assetclass_tier1_delta.input.location"
C_S3_ASSETCLASS_TIER2_DELTA_INPUT_LOCATION_PROP_KEY="s3.assetclass_tier2_delta.input.location"
C_S3_ASSETCLASS_TIER3_DELTA_INPUT_LOCATION_PROP_KEY="s3.assetclass_tier3_delta.input.location"
C_S3_ASSETCLASS_TIER4_DELTA_INPUT_LOCATION_PROP_KEY="s3.assetclass_tier4_delta.input.location"

#PROSPECTUS
C_S3_PROSPECTUS_INPUT_LOCATION_PROP_KEY="s3.prospectus_ben.input.location"
C_S3_PROSPECTUS_BEN_EXCP_OUTPUT_LOCATION_PROP_KEY="s3.prospectus_ben.excp.location"
C_S3_PROSPECTUS_BEN_STG_OUTPUT_LOCATION_PROP_KEY="s3.prospectus_ben.stg.output.location"

# Stage Fund Number
C_S3_FUND_NUMBER_STG_OUTPUT_LOCATION="s3.fund_nbr.stg.output.location"
C_S3_FUND_NUMBER_STG_EXCP_OUTPUT_LOCATION="s3.fund_nbr.stg.excp.output.location"


# Benchmark 1100, 1200, 1500
C_S3_CONV_BENCHMARK_1100_INPUT_LOCATION_PROP_KEY="glue.s3.ben1100.input.location"
C_S3_CONV_BENCHMARK_1200_INPUT_LOCATION_PROP_KEY="glue.s3.ben1200.input.location"
C_S3_CONV_BENCHMARK_1500_INPUT_LOCATION_PROP_KEY="glue.s3.ben1500.input.location"


#CARMA Account
C_S3_ACC_BRANCH_CHANGES_INPUT_LOCATION_PROP_KEY="s3.acc_branch_changes.input.location"
C_S3_ACC_CONSULTWORK_REL_INPUT_LOCATION_PROP_KEY="s3.acc_consultwork_rel.input.location"
C_S3_ACC_DETAILS_INPUT_LOCATION_PROP_KEY="s3.acc_details.input.location"
C_S3_ACC_FA_RELATION_INPUT_LOCATION_PROP_KEY="s3.acc_fa_relation.input.location"

C_S3_ACC_BRANCH_CHANGES_OUTPUT_LOCATION_PROP_KEY="s3.acc_branch_changes.stg.output.location"
C_S3_ACC_CONSULTWORK_REL_OUTPUT_LOCATION_PROP_KEY="s3.acc_consultwork_rel.stg.output.location"
C_S3_ACC_DETAILS_OUTPUT_LOCATION_PROP_KEY="s3.acc_details.stg.output.location"
C_S3_ACC_FA_RELATION_OUTPUT_LOCATION_PROP_KEY="s3.acc_fa_relation.stg.output.location"

C_S3_ACC_BRANCH_CHANGES_EXCP_LOCATION_PROP_KEY="s3.acc_branch_changes.excp.output.location"
C_S3_ACC_CONSULTWORK_REL_EXCP_LOCATION_PROP_KEY="s3.acc_consultwork_rel.excp.output.location"
C_S3_ACC_DETAILS_EXCP_LOCATION_PROP_KEY="s3.acc_details.excp.output.location"
C_S3_ACC_FA_RELATION_EXCP_LOCATION_PROP_KEY="s3.acc_fa_relation.excp.output.location"

C_S3_BREAKS_ACCOUNT_DATE_OUTPUT_LOCATION_PROP_KEY= "s3.breaks_account_date.output.location"

#CARMA- ACCOUNT CONSULTANT WORK

#avejoe_kebank_acc_perf
C_S3_AVEJOE_KEBANK_INPUT_LOCATION_PROP_KEY="s3.avejoe_keybank.input.location"
C_S3_AVEJOE_KEYBANK_ACC_PERF_OUTPUT_LOCATION_PROP_KEY="S3.avejoe_keybank_acc_perf.output.location"

#Benchmark Association PM
C_S3_BEN_ASSOCIATION_INPUT_LOCATION_PROP_KEY="s3.ben_association.input.location"
C_S3_BEN_ASSOCIATION_EXCP_LOCATION_PROP_KEY="s3.ben_association.excp.output.location"
C_S3_BEN_ASSOCIATION_STG_OUTPUT_LOCATION_PROP_KEY="s3.ben_association.stg.output.location"
C_S3_BRDG_ENTITY_RELATION_EXCP_OUTPUT_LOCATION="s3.brdg_entity_relation.excp.location"

#toolset_acount_restrictions
C_S3_TOOLSET_ACCOUNT_RESTRICTIONS_INPUT_LOCATION_PROP_KEY="s3.toolset_account_restrictions.input.location"
C_S3_TOOLSET_ACCOUNT_RESTRICTIONS_STG_OUTPUT_LOCATION_PROP_KEY="s3.toolset_account_restrictions.stg.output.location"
C_S3_TOOLSET_ACCOUNT_RESTRICTIONS_STG_EXCP_OUTPUT_LOCATION_PROP_KEY="s3.toolset_account_restrictions.stg.excp.output.location"

#avejoe_pmp_composite_perf
C_S3_AVEJOE_PMP_INPUT_LOCATION_PROP_KEY="s3.avejoe_pmp.input.location"
C_S3_AVEJOE_PMP_COMP_PERF_OUTPUT_LOCATION_PROP_KEY="s3.avejoe_pmp_comp_perf.output.location"

C_S3_FIYIELD_INPUT_LOCATION_PROP_KEY="s3.fiyield.input.location"

#Toolset Account Target
C_S3_ACCOUNT_TARGETS_INPUT_LOCATION_PROP_KEY="s3.account_targets.input.location"
C_S3_ACCOUNT_TARGETS_EXCP_LOCATION_PROP_KEY="s3.account_targets.excp.output.location"
C_S3_ACCOUNT_TARGETS_STG_OUTPUT_LOCATION_PROP_KEY="s3.account_targets.stg.output.location"
C_S3_DIM_TARGETS_OUTPUT_LOCATION_PROP_KEY="s3.dim_targets.output.location"
C_S3_DIM_TARGETS_EXCP_OUTPUT_LOCATION_PROP_KEY="s3.dim_targets.excp.output.location"

#SM Sec RATINGS
C_S3_SM_SECRATING_INPUT_LOCATION_PROP_KEY="s3.sm_secrating.input.location"
C_S3_SM_SECRATING_EXCP_LOCATION_PROP_KEY="s3.sm_secrating.excp.output.location"
C_S3_SM_SECRATING_STG_OUTPUT_LOCATION_PROP_KEY="s3.sm_secrating.stg.output.location"


#security_master_ref
C_S3_SECURITY_MASTER_REF_INPUT_LOCATION_PROP_KEY="s3.security_master_ref.input.location"
C_S3_SECURITY_MASTER_REF_STG_OUTPUT_LOCATION_PROP_KEY="s3.security_master_ref.stg.output.location"

#Conv DTL_FI_SCHEDILE
C_S3_FISCHD_INPUT_LOCATION_PROP_KEY="s3.fischd.input.location"

#Mstar_Mf_Unbundled
C_S3_MSTAR_MF_UNBUNDLED_INPUT_LOCATION_PROP_KEY="s3.mstar_mf_unbundled.input.location"
C_S3_MSTAR_MF_UNBUNDLED_STG_OUTPUT_LOCATION_PROP_KEY="s3.mstar_mf_unbundled.stg.output.location"
C_S3_MSTAR_MF_UNBUNDLED_STG_EXCP_OUTPUT_LOCATION_PROP_KEY="s3.mstar_mf_unbundled.stg.excp.output.location"


C_S3_ADV_ACC_INC_DATE_STG_EXCP_OUTPUT_LOCATION= "s3.adv_acc_inc_date.stg.excp.output.location"
C_S3_ADV_ACC_INC_DATE_STG_OUTPUT_LOCATION= "s3.adv_acc_inc_date.stg.output.location"
C_S3_ADV_ACC_INC_DATE_INPUT_LOCATION="s3.adv_acc_inc_date.input.location"
C_GLUE_S3_TABLE_DTL_ENTITY_REF_PROP_KEY= 'glue.s3.table.dtl_entity_ref'
C_S3_DTL_ENTITY_REF_OUTPUT_LOCATION="s3.dtl_entity_ref.output.location"

#Account linking-LCO
C_S3_LCO_INPUT_LOCATION_PROP_KEY="s3.lco.input.location"
C_S3_LCO_STG_OUTPUT_LOCATION_PROP_KEY="s3.lco.stg.output.location"
C_S3_LCO_STG_EXCP_OUTPUT_LOCATION_PROP_KEY="s3.lco.stg.excp.location"
C_GLUE_S3_TABLE_DIM_ACCOUNT_LINK_PROP_KEY="glue.s3.table.dim_account_link"


C_S3_SECURITY_OTHERS_INPUT_LOCATION_PROP_KEY="s3.sec_other.input.location"
C_S3_SECURITY_OTHERS_STG_OUTPUT_LOCATION_PROP_KEY="s3.sec_other.stg.output.location"
C_S3_SECURITY_OTHERS_STG_EXCP_OUTPUT_LOCATION_PROP_KEY="s3.sec_other.stg.excp.output.location"

#foa_unbundle
C_S3_FOA_UNBUNDLED_INPUT_LOCATION_PROP_KEY ="s3.foa_unbundled.input.location"
C_S3_FOA_UNBUNDLED_STG_OUTPUT_LOCATION_PROP_KEY="s3.foa_unbundled.stg.output.location"
C_S3_FOA_UNBUNDLED_STG_EXCP_OUTPUT_LOCATION_PROP_KEY="s3.foa_unbundled.stg.excp.output.location"

#DIM_PROGRAM
C_GLUE_S3_TABLE_DIM_PROGRAM_PROP_KEY ="glue.s3.table.dim_program"

C_S3_WRK_MULT_INSTRUMENT_FILTER_OUTPUT_PROP_KEY ="s3.wrk_mult_instrument_filter.output.location"

C_GLUE_S3_TABLE_DIM_TARGETS_PROP_KEY= 'glue.s3.table.dim_targets'
C_S3_TARGETS_BEN_ASSOCIATION_STG_OUTPUT_LOCATION_PROP_KEY= 's3.targets_ben_association.output.location'
C_S3_STG_TARGETS_BEN_ASSOCIATION_EXCP_LOCATION= "s3.targets_ben_association.stg.excp.location"

#avejoe_mwrsec_tot_adv
C_S3_AVEJOE_MWRSEC_TOT_ADV_EXTRACT_INPUT_LOCATION_PORP_KEY="s3.avejoe_mwrsec_tot_adv_extract.input.location"
C_S3_AVEJOE_MWRSEC_TOT_ADV_EXTRACT_OUTPUT_LOCATION_PORP_KEY="s3.avejoe_mwrsec_tot_adv_extract.output.location"

#avejoe_mwracc_tot_adv
C_S3_AVEJOE_MWRACC_TOT_ADV_EXTRACT_INPUT_LOCATION_PORP_KEY="s3.avejoe_mwracc_tot_adv_extract.input.location"
C_S3_AVEJOE_MWRACC_TOT_ADV_EXTRACT_OUTPUT_LOCATION_PORP_KEY="s3.avejoe_mwracc_tot_adv_extract.output.location"

#CARMA Account purge
C_S3_ACCOUNT_PURGE_INPUT_LOCATION_PROP_KEY ="s3.account_purge.input.location"
C_S3_ACCOUNT_PURGE_STG_OUTPUT_LOCATION_PROP_KEY ="s3.account_purge.stg.output.location"
C_S3_ACCOUNT_PURGE_STG_EXCP_OUTPUT_LOCATION_PROP_KEY ="s3.account_purge.stg.excp.output.location"

#CARMA_OPTION_OVERLAY
C_S3_OPTION_OVERLAY_CHANGES_INPUT_LOCATION_PROP_KEY="s3.carma_option_overlay.input.location"
C_S3_OPTION_OVERLAY_CHANGES_STG_OUTPUT_LOCATION_PROP_KEY="s3.carma_option_overlay.stg.output.location"
C_S3_OPTION_OVERLAY_CHANGES_STG_EXCP_OUTPUT_LOCATION_PROP_KEY="s3.carma_option_overlay.stg.excp.output.location"
C_GLUE_S3_TABLE_FACT_CAPITAL_FLOW_FEE_PROP_KEY = 'glue.s3.table.fact_capital_flow_fee'


#fihist
C_S3_FIHIST_INPUT_LOCATION_PROP_KEY="s3.fihist.input.location"

# benchmark 0700
C_S3_CONV_BENCHMARK_0700_INPUT_LOCATION_PROP_KEY= "s3.conv_benchmark_0700.input.location"
C_S3_CONV_BENCHMARK_0700_DELTA_INPUT_LOCATION_PROP_KEY= "s3.conv_benchmark_0700_delta.input.location"

# Benchmark Daily & Monthly
C_S3_AVEJOE_BNE_DAILY_ROR_INPUT_LOCATION_PROP_KEY="s3.avejoe_ben_daily.input.location"
C_S3_AVEJOE_BNE_MON_ROR_INPUT_LOCATION_PROP_KEY="s3.avejoe_ben_mon.input.location"
C_S3_AVEJOE_BEN_DAILY_ROR_OUTPUT_LOCATION_PROP_KEY="s3.avejoe_ben_daily.output.location"
C_S3_AVEJOE_BEN_MON_ROR_OUTPUT_LOCATION_PROP_KEY="s3.avejoe_ben_mon.output.location"

#AIP
C_S3_AIP_INPUT_LOCATION_PROP_KEY="s3.aip.input.location"
C_S3_CONV_BEN_UPAV_DATA_EXCP_LOCATION = "s3.conv_ben_upav_data.excp.location"

#Conv blend upav
C_S3_BEN_BLEND_UPAV_1500_EXCP_OUTPUT_LOCATION_PROP_KEY = "s3.ben_blend_upav_1500.stg.excp.output.location"

#fiscal
C_S3_FISCAL_INPUT_LOCATION_PROP_KEY="s3.fiscal.input.location"
C_S3_FISCAL_STG_EXCP_OUTPUT_LOCATION_PROP_KEY="s3.fiscal.stg.excp.location"
C_GLUE_S3_TABLE_DIM_SOR_TRAN_GROUP_PROP_KEY="glue.s3.table.dim_sor_tran_group_id_ref"
C_S3_STG_FOA_BEN_TGT_ACC_STG_LOCATION= "s3.stg_foa_ben_tgt_acc.stg.location"


# Conv_card_activity
C_S3_CARD_ACTIVITY_INPUT_LOCATION_PROP_KEY = "s3.card_activity.input.location"

#BAA
C_S3_BAA_INPUT_LOCATION_PROP_KEY="s3.baa.input.location"
C_S3_BAA_OUTPUT_LOCATION_PROP_KEY="s3.baa.stg.output.location"
C_S3_BAA_EXCP_LOCATION_PROP_KEY="s3.baa.excp.output.location"

#BUA
C_S3_BUA_INPUT_LOCATION_PROP_KEY="s3.bua.input.location"
C_S3_BUA_OUTPUT_LOCATION_PROP_KEY="s3.bua.stg.output.location"
C_S3_BUA_EXCP_OUTPUT_LOCATION_PROP_KEY="s3.bua.excp.output.location"

#BUB
C_S3_BUB_INPUT_LOCATION_PROP_KEY="s3.bub.input.location"
C_S3_BUB_OUTPUT_LOCATION_PROP_KEY="s3.bub.stg.output.location"
C_S3_BUB_EXCP_LOCATION_PROP_KEY="s3.bub.excp.output.location"

#HUA
C_S3_HUA_INPUT_LOCATION_PROP_KEY="s3.hua.input.location"
C_S3_HUA_OUTPUT_LOCATION_PROP_KEY="s3.hua.stg.output.location"
C_S3_HUA_EXCP_OUTPUT_LOCATION_PROP_KEY="s3.hua.excp.output.location"

# awaccts_mtdt_asset_wizard_acc_filter
C_S3_AWACCTS_INPUT_LOCATION_PROP_KEY="s3.awaccts.input.location"
C_S3_MTDT_ASST_WIZ_ACC_FLTR_OUTPUT_LOCATION="s3.mtdt_asset_wizard_acc_filter.output.location"

C_S3_FACT_ACCOUNT_MV_OUTPUT_LOCATION= "s3.fact_account_mv.output.location"
C_S3_FACT_ACCOUNT_MV_EXCP_OUTPUT_LOCATION= "s3.fact_account_mv.excp.output.location"

C_S3_PFOLVAL_FACT_ACCOUNT_OUTPUT_LOCATION="s3.fact_account_mv.output.location"
C_S3_PFOLVAL_FACT_ACCOUNT_EXCP_OUTPUT_LOCATION="s3.fact_account_mv.excp.output.location"


C_S3_DIR_CLEANUP_BEFORE_SAVE = "s3.dir.cleanup.before.save"
C_S3_EXCP_DIR_CLEANUP_BEFORE_SAVE = "s3.excp.dir.cleanup.before.save"

# Avejoe MWR asssest class
C_S3_AVEJOE_MWR_ASSET_CLASS_INPUT_LOCATION_PORP_KEY="s3.avejoe_mwr_asset_class.input.location"
C_S3_AVEJOE_MWR_ASSET_CLASS_EXTRACT_OUTPUT_LOCATION_PORP_KEY="s3.avejoe_mwr_asset_class.output.location"


C_GLUE_S3_TABLE_MTDT_ACC_BEN_ID_GEN= "glue.s3.table.mtdt_acc_ben_id_gen"

C_S3_MTDT_ACC_BEN_ID_GEN_OUTPUT_LOCATION_PROP_KEY = "s3.mtdt_acc_ben_id_gen.output.location"

#CARMA Consult Work
C_S3_CONSULT_WORK_RELTN_INPUT_LOCATION_PROP_KEY="s3.consult_work_reltn.input.location"
C_S3_STG_CONSULT_WORK_RELTN_OUTPUT_LOCATION_PROP_KEY="s3.consult_work_reltn.stg.output.location"
C_S3_STG_CONSULT_WORK_RELTN_EXCP_OUTPUT_LOCATION_PROP_KEY="s3.consult_work_reltn.excp.output.location"
C_S3_DIM_SUB_GROUP_OUTPUT_LOCATION_PROP_KEY="s3.dim_sub_group.output.location"
C_S3_DIM_SUB_GROUP_EXCP_OUTPUT_LOCATION_PROP_KEY="s3.dim_sub_group.excp.output.location"
C_S3_DIM_ACCOUNT_GROUP_EXCP_OUTPUT_LOCATION_PROP_KEY = "s3.dim_account_group.excp.output.location"
C_GLUE_S3_TABLE_DIM_SUB_GROUP_PROP_KEY="glue.s3.table.dim_sub_group"


C_S3_STG_ANNUITIES_SUB_OUTPUT_LOCATION_PROP_KEY= "s3.annuities_sub.stg.output.location"
C_S3_ANNUITIES_SUB_INPUT_LOCATION_PROP_KEY= "s3.annuities_sub.input.location"
C_S3_STG_ANNUITIES_SUB_EXCP_LOCATION_PROP_KEY= "s3.annuities_sub.excp.output.location"

C_S3_FACT_ANNUITIES_MV_OUTPUT_LOCATION= "s3.fact_annuities_mv.output.location"
C_S3_FACT_ANNUITIES_MV_EXCP_OUTPUT_LOCATION= "s3.fact_annuities_mv.excp.output.location"

C_S3_HIST_MNDT_INPUT_LOCATION_PROP_KEY="s3.hist_mndt.input.location"

C_S3_ACCTMPLT_INPUT_LOCATION_PROP_KEY="s3.acctmplt.input.location"
C_S3_DIM_TEMPLATE_OUTPUT_LOCATION_PROP_KEY="s3.dim_template.output_location"
C_S3_FACT_ACCOUNT_TEMPLATE_OUTPUT_LOCATION_PROP_KEY="s3.fact_account_template.output_location"
C_S3_FACT_ACCOUNT_TEMPLATE_EXCP_OUTPUT_LOCATION_PROP_KEY="s3.fact_account_template.excp.output.location"

#LCO Dim account link staging
C_S3_DIM_ACCOUNTLINKSTG_EXCP_OUTPUT_LOCATION_PROP_KEY="s3.dim_account_link_stg.excp.output.location"
C_S3_DIM_ACCOUNT_LINK_STG_OUTPUT_LOCATION_PROP_KEY="s3.dim_account_link_stg.output.location"

# Avejoe Account adv tot slv extracts
C_S3_AVEJOE_ACCOUNT_TOT_INPUT_LOCATION_PORP_KEY="s3.avejoe_account_tot.input.location"
C_S3_AVEJOE_ACCOUNT_ADV_INPUT_LOCATION_PORP_KEY="s3.avejoe_account_adv.input.location"
C_S3_AVEJOE_ACCOUNT_SLV_INPUT_LOCATION_PORP_KEY="s3.avejoe_account_slv.input.location"
C_S3_AVEJOE_ACCOUNT_TOT_EXTRACT_OUTPUT_LOCATION_PORP_KEY="s3.avejoe_account_tot_extract.output.location"
C_S3_AVEJOE_ACCOUNT_ADV_EXTRACT_OUTPUT_LOCATION_PORP_KEY="s3.avejoe_account_adv_extract.output.location"
C_S3_AVEJOE_ACCOUNT_SLV_EXTRACT_OUTPUT_LOCATION_PORP_KEY="s3.avejoe_account_slv_extract.output.location"



C_S3_FACT_ASSETS_LIABILITIES_OUTPUT_LOCATION= "s3.fact_assets_liabilities.output.location"
C_S3_FACT_ASSETS_LIABILITIES_EXCP_OUTPUT_LOCATION= "s3.fact_assets_liabilities.excp.output.location"


C_S3_STG_AUTOMATED_ASSET_BALANCE_OUTPUT_LOCATION_PROP_KEY= "s3.automated_asset_balance.stg.output.location"
C_S3_AUTOMATED_ASSET_BALANCE_INPUT_LOCATION_PROP_KEY= "s3.automated_asset_balance.input.location"
C_S3_STG_AUTOMATED_ASSET_BALANCE_EXCP_LOCATION_PROP_KEY= "s3.automated_asset_balance.excp.output.location"

C_S3_STG_AUTOMATED_LIABILITY_BALANCE_OUTPUT_LOCATION_PROP_KEY= "s3.automated_liability_balance.stg.output.location"
C_S3_AUTOMATED_LIABILITY_BALANCE_INPUT_LOCATION_PROP_KEY= "s3.automated_liability_balance.input.location"
C_S3_STG_AUTOMATED_LIABILITY_BALANCE_EXCP_LOCATION_PROP_KEY= "s3.automated_liability_balance.excp.output.location"

#mtdt_dod_filter
C_S3_MTDT_DOD_FILTER_INPUT_LOCATION_PROP_KEY='s3.input.location.mtdt_dod_filter'
C_S3_MTDT_DOD_FILTER_OUTPUT_LOCATION_PROP_KEY='s3.mtdt_dod_filter.output.location'

#PWSEC vs ADP Weekend correction
C_S3_PWSEC_VS_ADP_WEEKEND_CORRECTION_INPUT_LOCATION_PROP_KEY = "s3.pwsec_vs_adp_weekend_correction.input.location"
C_S3_PWSEC_VS_ADP_WEEKEND_CORRECTION_STG_LOCATION_PROP_KEY = "s3.pwsec_vs_adp_weekend_correction.stg.location"
C_S3_PWSEC_VS_ADP_WEEKEND_CORRECTION_EXCP_LOCATION_PROP_KEY = "s3.pwsec_vs_adp_weekend_correction.excp.location"

C_S3_INSU_POSITION_INPUT_LOCATION_PROP_KEY="s3.insurance_position.input.location"
C_S3_INSU_POSITION_STG_EXCP_LOCATION_PROP_KEY="s3.insurance_position.stg.excp.output.location"
C_S3_INSU_POSITION_STG_OUTPUT_LOCATION_PROP_KEY="s3.insurance_position.stg.output.location"

C_S3_STG_MANUAL_LIABILITIES_OUTPUT_LOCATION_PROP_KEY= "s3.manual_liabilities.stg.output.location"
C_S3_MANUAL_LIABILITIES_INPUT_LOCATION_PROP_KEY= "s3.manual_liabilities.input.location"
C_S3_STG_MANUAL_LIABILITIES_EXCP_LOCATION_PROP_KEY= "s3.manual_liabilities.excp.output.location"

# Automated Holdings Details(UBS Files)
C_S3_AUTOMATED_HOLDINGS_DETAILS_INPUT_LOCATION_PROP_KEY= "s3.automated_holdings_details.input.location"
C_S3_STG_AUTOMATED_HOLDINGS_DETAILS_OUTPUT_LOCATION_PROP_KEY= "s3.automated_holdings_details.stg.output.location"
C_S3_STG_AUTOMATED_HOLDINGS_DETAILS_EXCP_LOCATION_PROP_KEY= "s3.automated_holdings_details.excp.output.location"

# Outside Account Details
C_S3_OUTSIDE_ACCOUNT_DETAILS_INPUT_LOCATION_PROP_KEY= "s3.outside_account_details.input.location"
C_S3_STG_OUTSIDE_ACCOUNT_DETAILS_OUTPUT_LOCATION_PROP_KEY= "s3.outside_account_details.stg.output.location"
C_S3_STG_OUTSIDE_ACCOUNT_DETAILS_EXCP_LOCATION_PROP_KEY= "s3.outside_account_details.excp.output.location"

# Manual Outside Holdings
C_S3_MANUAL_OUTSIDE_HOLDINGS_INPUT_LOCATION_PROP_KEY= "s3.manual_outside_holdings.input.location"
C_S3_STG_MANUAL_OUTSIDE_HOLDINGS_OUTPUT_LOCATION_PROP_KEY= "s3.manual_outside_holdings.stg.output.location"
C_S3_STG_MANUAL_OUTSIDE_HOLDINGS_EXCP_LOCATION_PROP_KEY= "s3.manual_outside_holdings.excp.output.location"

# LDBV6000
C_S3_LIABILITIES_FACT_ASSETS_LIABILITIES_INPUT_LOCATION_PROP_KEY= "s3.liabilities_fact_assets_liabilities.input.location"
C_S3_STG_LIABILITIES_FACT_ASSETS_LIABILITIES_OUTPUT_LOCATION_PROP_KEY= "s3.liabilities_fact_assets_liabilities.stg.output.location"
C_S3_STG_LIABILITIES_FACT_ASSETS_LIABILITIES_EXCP_LOCATION_PROP_KEY= "s3.liabilities_fact_assets_liabilities.excp.output.location"

C_S3_INMV0600_INPUT_LOCATION_PROP_KEY= "s3.inmv0600.input.location"
C_S3_STG_INMV0600_OUTPUT_LOCATION_PROP_KEY= "s3.inmv0600.stg.output.location"
C_S3_STG_INMV0600_EXCP_LOCATION_PROP_KEY= "s3.inmv0600.excp.output.location"

C_S3_LDBV6700_INPUT_LOCATION_PROP_KEY= "s3.ldbv6700.input.location"
C_S3_STG_LDBV6700_OUTPUT_LOCATION_PROP_KEY= "s3.ldbv6700.stg.output.location"
C_S3_STG_LDBV6700_EXCP_LOCATION_PROP_KEY= "s3.ldbv6700.excp.output.location"

# LDBCVAR
C_S3_LDBCVAR_INPUT_LOCATION_PROP_KEY="s3.ldbcvar.input.location"
C_S3_STG_LDBCVAR_OUTPUT_LOCATION_PROP_KEY="s3.ldbcvar.stg.output.location"
C_S3_STG_LDBCVAR_EXCP_LOCATION_PROP_KEY="s3.ldbcvar.excp.output.location"

# Rmbccif5
C_S3_RMBCCIF5_INPUT_LOCATION_PROP_KEY="s3.rmbccif5.input.location"
C_S3_STG_RMBCCIF5_OUTPUT_LOCATION_PROP_KEY="s3.rmbccif5.stg.output.location"
C_S3_STG_RMBCCIF5_EXCP_LOCATION_PROP_KEY="s3.rmbccif5.excp.output.location"
# Rmbccif6
C_S3_RMBCCIF6_INPUT_LOCATION_PROP_KEY="s3.rmbccif6.input.location"
C_S3_STG_RMBCCIF6_OUTPUT_LOCATION_PROP_KEY="s3.rmbccif6.stg.output.location"
C_S3_STG_RMBCCIF6_EXCP_LOCATION_PROP_KEY="s3.rmbccif6.excp.output.location"


C_S3_MANUAL_ASSET_BALANCE_INPUT_LOCATION_PROP_KEY= "s3.manual_asset_balance.input.location"
C_S3_STG_MANUAL_ASSET_BALANCE_OUTPUT_LOCATION_PROP_KEY= "s3.manual_asset_balance.stg.output.location"
C_S3_STG_MANUAL_ASSET_BALANCE_EXCP_LOCATION_PROP_KEY= "s3.manual_asset_balance.excp.output.location"

#acc_perf_ror_daily
C_S3_DAILY_ROR_EXTRACT_INPUT_LOCATION_PROP_KEY="s3.acc_perf_ror_dly.input.location"
C_S3_DAILY_ROR_EXTRACT_OUTPUT_LOCATION="s3.tool_cpos_acc_perf_ror.output.location"

#529 tran nocf
C_S3_529_TRAN_NOCF_INPUT_LOCATION_PROP_KEY = "s3.529_tran_nocf.input.location"
C_S3_529_TRAN_NOCF_STG_LOCATION_PROP_KEY = "s3.529_tran_nocf.stg.output.location"

#529 tran noref
C_S3_529_TRAN_NOREF_INPUT_LOCATION_PROP_KEY = "s3.529_tran_noref.input.location"
C_S3_529_TRAN_NOREF_STG_LOCATION_PROP_KEY = "s3.529_tran_noref.stg.output.location"

# RMA
C_S3_MTDSTMT_INPUT_LOCATION_PROP_KEY = "s3.mtdstmt.input.location"
C_S3_CASHADV_INPUT_LOCATION_PROP_KEY = "s3.cashadv.input.location"
C_S3_MTDSTMT_STG_OUTPUT_LOCATION_PROP_KEY = "s3.mtdstmt.stg.output.location"
C_S3_CASHADV_STG_OUTPUT_LOCATION_PROP_KEY = "s3.cashadv.stg.output.location"
C_S3_MTDSTMT_STG_EXCP_OUTPUT_LOCATION_PROP_KEY="s3.mtdstmt.stg.excp.output.location"
C_S3_CASHADV_STG_EXCP_OUTPUT_LOCATION_PROP_KEY="s3.cashadv.stg.excp.output.location"
C_S3_DELTAPAYEE_STG_OUTPUT_LOCATION_PROP_KEY = "s3.deltapayee.stg.output.location"

#TDR BIMS Asset Classification
C_S3_TDR_BIMS_ASSET_CLASS_INPUT_LOCATION_PROP_KEY = "s3.tdr_bims_asset_class.input.location"
C_S3_STG_TDR_BIMS_ASSET_CLASS_OUTPUT_LOCATION_PROP_KEY= "s3.tdr_bims_asset_class.stg.output.location"

#TDR CARMA Primary Product
C_S3_TDR_CARMA_PRIMARY_PROD_INPUT_LOCATION_PROP_KEY = "s3.tdr_carma_primary_prod.input.location"
C_S3_STG_TDR_CARMA_PRIMARY_PROD_OUTPUT_LOCATION_PROP_KEY= "s3.tdr_carma_primary_prod.stg.output.location"

#Monthly ROR
C_S3_UBS_MONTHLY_HIST_INPUT_LOCATION = "s3.ubs_monthly_hist.input.location"
C_S3_UBS_MONTHLY_HIST_STG_OUTPUT_LOCATION_PROP_KEY = "s3.ubs_monthly_hist.stg.output.location"
C_S3_UBS_MONTHLY_HIST_STG_EXCP_OUTPUT_LOCATION_PROP_KEY =  "s3.ubs_monthly_hist.stg.excp.output.location"

C_S3_LINK_BREAKS_ACCOUNT_DATE_OUTPUT_LOCATION_PROP_KEY = "s3.link_breaks_account_date.output.location"

#conv_acc_perf_fact_class_entity
C_S3_ACC_PERF_INPUT_LOCATION_PROP_KEY ="s3.acc_perf.input.location"

#mwr_acc_adv_tot_slv
C_S3_AVEJOE_MWRACC_ADV_TOT_INPUT_LOCATION_PORP_KEY = "s3.avejoe_mwracc_adv_tot.input.location"
C_S3_AVEJOE_MWRACC_SLV_INPUT_LOCATION_PORP_KEY = "s3.avejoe_mwracc_slv.input.location"
C_S3_AVEJOE_MWRACC_ACC_ADV_TOT_EXTRACT_OUTPUT_LOCATION_PROP_KEY="s3.avejoe_mwracc_adv_tot_extract.output.location"
C_S3_AVEJOE_MWRACC_ACC_SLV_EXTRACT_OUTPUT_LOCATION_PROP_KEY = "s3.avejoe_mwracc_slv_extract.output.location"

#TrueupBenGenXDelta upav 1500
C_S3_STG_BEN_UPAV1500_DELTA_INPUT_LOCATION_PROP_KEY = "s3.stg_ben_upav1500_delta.input.location"
C_S3_STG_BEN_UPAV1200_DELTA_INPUT_LOCATION_PROP_KEY = "s3.stg_ben_upav1200_delta.input.location"
C_S3_STG_BEN_UPAV1500_DELTA_ENTITY_OUTPUT_LOCATION = "s3.stg_ben_upav1500_delta.output.location"
C_S3_STG_BEN_UPAV1200_DELTA_ENTITY_OUTPUT_LOCATION = "s3.stg_ben_upav1200_delta.output.location"

# conv_cashflow_fact_capital_flow_fee
C_S3_STG_CASHFLOW_LOCATION= "s3.stg_cashflow.output.location"

#parallel_cadecl_dim_corp_act_decl
C_S3_STG_PARALLEL_CADECL_DIM_CORP_ACT_DECL_INPUT_LOCATION_PROP_KEY = "s3.stg.parallelcadecldimcorpactdecl.input.location"
C_S3_PARALLEL_CADECL_DIM_CORP_ACT_DECL_STG_OUTPUT_LOCATION_PROP_KEY = "s3.parallelcadecldimcorpactdecl.stg.output.location"
C_S3_STG_PARALLEL_CADECL_DIM_CORP_ACT_DECL_EXCP_OUTPUT_LOCATION_PROP_KEY="s3.parallelcadecldimcorpactdecl.stg.excp.output.location"
C_S3_DTL_CORP_ACTION_DECL_SEC_RATE_OUTPUT_LOCATION_PROP_KEY = "s3.dtl_corp_action_decl_sec_rate.output.location"

# comp_mv_mtly_manager_composites
C_S3_COMP_MV_MTLY_INPUT_LOCATION_PROP_KEY = "s3.comp_mv_mtly.input.location"
C_S3_COMP_MV_MTLY_MANAGER_STG_OUTPUT_LOCATION_PROP_KEY = "s3.comp_mv_mtly_manager.stg.output.location"
C_S3_STG_COMP_MV_MTLY_MANAGER_OUTPUT_LOCATION = "s3.stg_comp_mv_mtly_manager.output.location"

#stg_fiscal_activity_trans
C_S3_STG_PARALLEL_FISCAL_ACTTIVITY_INPUT_LOCATION_PROP_KEY="s3.stg.parallelstgfiscalactivity.input.location"
C_S3_PARALLEL_FISCAL_ACTTIVITY_STG_OUTPUT_LOCATION_PROP_KEY="s3.parallelstgfiscalactivity.stg.output.location"
C_S3_PARALLEL_FISCAL_ACTTIVITY_OUTPUT_LOCATION_PROP_KEY="s3.parallelstgfiscalactivity.output.location"

#cpos_price
C_S3_SEC_PRICE_INPUT_LOCATION_PROP_KEY ="s3.kpst6200_sec_price.input.location"
C_S3_SEC_PRICE_OUTPUT_LOCATION_PROP_KEY ="s3.kpst6200_sec_price.output.location"

#rcat5600_dim_account_link
C_S3_RCAT5600_INPUT_LOCATION_PROP_KEY="s3.rcat5600.input.location"

#Benchmark Feeds
C_S3_BENCHMARK_FEEDS_INPUT_LOCATION = "s3.benchmark.feeds.input.location"
C_S3_BENCHMARK_FEEDS_OUTPUT_LOCATION = "s3.benchmark.feeds.output.location"

#day_zero_rr_dtl_entity_ref
C_S3_DAY_ZERO_RR_DTL_ENTITY_REF_INPUT_LOCATION_PROP_KEY="s3.stg.dayzerorrdtlentityref.input.location"
C_S3_ADVISOR_STG_LOCATION_PROP_KEY='s3.advisor.stg.location'

#RPT FOA Accounts AGG
C_S3_RPT_FOA_ACCOUNTS_REPORTS_INPUT_LOCATION = "s3.rpt.foa.accounts.agg.reports.input.location"
C_S3_RPT_FOA_ACCOUNTS_REPORTS_OUTPUT_LOCATION = "s3.rpt.foa.accounts.reports.agg.output.location"
C_S3_RPT_FOA_ACCOUNTS_EXTRACTS_INPUT_LOCATION = "s3.rpt.foa.accounts.agg.extracts.input.location"
C_S3_RPT_FOA_ACCOUNTS_EXTRACTS_REPORTS_OUTPUT_LOCATION = "s3.rpt.foa.accounts.agg.extracts.output.location"

#keyrecon_gim2
C_S3_STG_KEYRECON_GIM2_INPUT_LOCATION_PROP_KEY="s3.stg_keyrecon_gim2.input.location"
C_S3_STG_KEYRECON_GIM2_OUTPUT_LOCATION_PROP_KEY="s3.stg_keyrecon_gim2.stg.output.location"

#delta_account_target_ben_association
C_GLUE_S3_TABLE_DELTA_ACCOUNT_TARGETS_PROP_KEY = "glue.s3.table.wrk_tdr_delta_acc_targets"

# pace_eligible_dtl_program_asset
C_S3_DIM_SUB_ADVISOR_OUTPUT_LOCATION_PROP_KEY='s3.dim_sub_advisor.output.location'
C_GLUE_S3_TABLE_DIM_SUB_ADVISOR_PROP_KEY='glue.s3.table.dim_sub_advisor'

# S3 Directory Delete Paths for Account and Instrument Stub Changes
C_S3_KEYRECON_FACT_SOR_HOLDING_OUTPUT_DIR_DELETE_LOCATION_PROP_KEY="s3.keyrecon.dir.delete.output.location"
C_S3_DAYZERO_KEYRECON_FACT_HOLDING_OUTPUT_DIR_DELETE_LOCATION_PROP_KEY="s3.dayzero.keyrecon.dir.delete.output.location"
C_S3_FIVE29_ANNUITIES_FACT_SOR_HOLDING_OUTPUT_DIR_DELETE_LOCATION_PROP_KEY="s3.five29.annuities.dir.delete.output.location"
C_S3_DAYZERO_FIVE29_ANNUITIES_FACT_HOLDING_OUTPUT_DIR_DELETE_LOCATION_PROP_KEY="s3.dayzero.five29.annuities.dir.delete.output.location"
C_S3_ANNUITIES_FACT_SOR_HOLDING_OUTPUT_DIR_DELETE_LOCATION_PROP_KEY = 's3.annuities_fact_sor_holding.dir.delete.output.location'
C_S3_ANNUITIES_FACT_HOLDING_OUTPUT_DIR_DELETE_LOCATION_PROP_KEY = 's3.annuities_fact_holding.dir.delete.output.location'

#kpsxn606
C_S3_CONV_KPSXN606_INPUT_LOCATION_PROP_KEY="s3.conv_kpsxn606.input.location"

#Glue rds details
C_RDS_DB_NAME_PROP_KEY="rds.db.name"
C_RDS_CONNECTION_NAME_PROP_KEY="rds.connection.name"
C_RDS_SCHEMA_NAME_PROP_KEY="rds.schema.name"
PROP_S3_RDS_WORK_FOLDER_CONV = 's3.rds.fact.position.value.conv.input.location'
PROP_S3_RDS_WORK_FOLDER_DAILY = 's3.rds.fact.position.value.daily.input.location'
PROP_S3_RDS_WORK_FOLDER_DAILY_DELETE = 's3.rds.fact.position.value.daily.delete.input.location'
PROP_S3_RDS_WORK_FOLDER_SYNC = 's3.rds.fact.position.value.sync.input.location'
PROP_S3_RDS_WORK_FOLDER_UI_SYNC = 's3.rds.fact.position.value.ui.sync.input.location'
PROP_S3_RDS_WORK_FOLDER_SYNC_ACC = 's3.rds.fact.position.value.sync.accounts.input.location'
PROP_S3_RDS_WORK_FOLDER_UI_SYNC_ACC = 's3.rds.fact.position.value.ui.sync.accounts.input.location'
PROP_S3_RDS_WORK_FOLDER_FPV_RECON = 's3.rds.fact.position.value.recon.output.location'

# S3 Directory Paths for Money Fund Changes
C_S3_MONEY_FUNDS_INPUT_LOCATION_PROP_KEY="s3.stg_Money_funds.input.location"
C_S3_MONEY_FUNDS_STG_OUTPUT_LOCATION_PROP_KEY = "s3.Money_funds.stg.output.location"
C_S3_MONEY_FUNDS_STG_EXCP_OUTPUT_LOCATION_PROP_KEY = "s3.Money_funds.stg.excp.output.location"

#sec_frequency
C_S3_CONV_SEC_FREQ_INPUT_LOCATION_PROP_KEY='s3.conv_sec_frequency.input.location'

#fa_list
C_S3_FA_LIST_INPUT_LOCATION_PROP_KEY = 's3.fa_list.input.location'
C_S3_FA_LIST_STG_LOCATION_PROP_KEY = 's3.fa_list.stg.output.location'
C_S3_FA_LIST_STG_EXCP_OUTPUT_LOCATION_PROP_KEY = "s3.fa_list.stg.excp.output.location"

#Cashflow_Delta
C_S3_CASHFLOW_DELTA_INPUT_LOCATION_PROP_KEY= "s3.cashflow.delta.input.location"

#Benchmark_feeds
C_S3_BENCHMARK_FEEDS_STG_OUTPUT_LOCATION = "s3.benchmark.feeds.stg.output.location"

#Avejoe_Stage_Files
C_S3_AVEJOE_ASSET_CLASS_EXTRACT_STG_OUTPUT_LOCATION_PORP_KEY = "s3.avejoe_asset_class_extract.stg.output.location"
C_S3_AVEJOE_ACCOUNT_TOT_EXTRACT_STG_OUTPUT_LOCATION_PORP_KEY = "s3.avejoe_account_tot_extract.stg.output.location"
C_S3_AVEJOE_ACCOUNT_ADV_EXTRACT_STG_OUTPUT_LOCATION_PORP_KEY = "s3.avejoe_account_adv_extract.stg.output.location"
C_S3_AVEJOE_ACCOUNT_SLV_EXTRACT_STG_OUTPUT_LOCATION_PORP_KEY = "s3.avejoe_account_slv_extract.stg.output.location"
C_S3_AVEJOE_BEN_DAILY_ROR_STG_OUTPUT_LOCATION_PROP_KEY = "s3.avejoe_ben_daily_ror.stg.output.location"
C_S3_AVEJOE_BEN_MON_ROR_STG_OUTPUT_LOCATION_PROP_KEY = "s3.avejoe_ben_mon_ror.stg.output.location"
C_S3_AVEJOE_SLV_ACC_EXTRACT_STG_OUTPUT_LOCATION_PORP_KEY = "s3.avejoe_slv_acc_extract.stg.output.location"
C_S3_AVEJOE_ACC_ADV_TOT_EXTRACT_STG_OUTPUT_LOCATION_PROP_KEY = "s3.avejoe_acc_adv_tot_extract.stg.output.location"
C_S3_AVEJOE_KEYBANK_ACC_PERF_STG_OUTPUT_LOCATION_PROP_KEY = "s3.avejoe_keybank_acc_perf.stg.output.location"
C_S3_AVEJOE_MWR_ASSET_CLASS_EXTRACT_STG_OUTPUT_LOCATION_PORP_KEY = "s3.avejoe_mwr_asset_class_extract.stg.output.location"
C_S3_AVEJOE_MWRACC_ACC_SLV_EXTRACT_STG_OUTPUT_LOCATION_PROP_KEY = "s3.avejoe_mwracc_slv_extract.stg.output.location"
C_S3_AVEJOE_MWRACC_ACC_ADV_TOT_EXTRACT_STG_OUTPUT_LOCATION_PROP_KEY = "s3.avejoe_mwracc_adv_tot_extract.stg.output.location"
C_S3_AVEJOE_MWRSEC_TOT_ADV_EXTRACT_STG_OUTPUT_LOCATION_PORP_KEY = "s3.avejoe_mwrsec_tot_adv_extract.stg.output.location"
C_S3_AVEJOE_PMP_COMP_PERF_STG_OUTPUT_LOCATION_PROP_KEY = "s3.avejoe_pmp_comp_perf.stg.output.location"
C_S3_AVEJOE_TOT_ADV_SECURITY_ROR_STG_OUTPUT_LOCATION_PORP_KEY = "s3.avejoe_tot_adv_security_ror.stg.output.location"