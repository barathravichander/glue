from constant.property_constant import C_GLUE_S3_TABLE_DIM_ACCOUNT_PROP_KEY, C_GLUE_S3_TABLE_DIM_PROPERTY_POP_KEY, C_GLUE_S3_TABLE_DIM_CLASS_TYPE, \
    C_GLUE_S3_TABLE_TRANS_LTYPE, C_GLUE_S3_TABLE_DTL_INSTRUMENT_REF_PROP_KEY, C_GLUE_S3_TABLE_TRANS_STYPE, C_S3_FACT_CLASS_ENTITY_TBL_NME, \
    C_GLUE_S3_TABLE_DIM_SLEEVE_PROP_KEY, C_GLUE_S3_TABLE_BRDG_ACC_RELATION_PROP_KEY, C_GLUE_S3_TABLE_FACT_ACC_DATE_PROP_KEY, \
    C_GLUE_S3_TABLE_DTL_INSTRUMENT_DESC_PROP_KEY, C_GLUE_S3_TABLE_DIM_ADVISOR_PROP_KEY, C_GLUE_S3_TABLE_DIM_BUSINESS_UNIT_PROP_KEY, \
    C_GLUE_S3_TABLE_DTL_FI_SCHEDULE_PROP_KEY, C_GLUE_S3_TABLE_DIM_CORP_ACTION_DECL_PROP_KEY, C_GLUE_S3_TABLE_DIM_INSTRUMENT_PROP_KEY, \
    C_GLUE_S3_TABLE_DIM_ACCOUNT_GROUP_PROP_KEY, C_GLUE_S3_TABLE_MTDT_INSTR_ID_GEN_PROP_KEY, C_GLUE_S3_TABLE_MTDT_CORP_ACTN_DECL_ID_GEN_PROP_KEY, \
    C_GLUE_S3_TABLE_DIM_UNIVERSE_PROP_KEY, C_GLUE_S3_TABLE_MTDT_SLEVE_ID_GEN_PROP_KEY, C_GLUE_S3_TABLE_DIM_FOA_PROP_KEY, \
    C_GLUE_S3_TABLE_BRDG_ENTITY_RELATION_PROP_KEY, C_GLUE_S3_TABLE_BRDG_INSTRU_REL_PROP_KEY, C_GLUE_S3_TABLE_DTL_ACCOUNT_REF_PROP_KEY, \
    C_GLUE_S3_TABLE_DTL_ENTITY_REF_PROP_KEY, C_GLUE_S3_TABLE_DIM_ACCOUNT_LINK_PROP_KEY, C_GLUE_S3_TABLE_DIM_FUND_SLEEVE_PROP_KEY, \
    C_GLUE_S3_TABLE_DIM_TARGETS_PROP_KEY, C_GLUE_S3_TABLE_MTDT_BATCH_DATE_PROP_KEY, C_GLUE_S3_TABLE_MTDT_ACC_BEN_ID_GEN,\
    C_GLUE_S3_TABLE_DIM_SUB_GROUP_PROP_KEY,C_GLUE_S3_TABLE_DELTA_ACCOUNT_TARGETS_PROP_KEY, C_GLUE_S3_TABLE_DIM_SUB_ADVISOR_PROP_KEY
from utils.JobContext import JobContext
from utils.config_classes import ResolveChoiceConfig, TableConfig


class TableConfigLoader(object):

    @staticmethod
    def load():
        table_config_map= {}
        JobContext.logger.info("Preparing table configuration")
        TableConfigLoader.__prepare_dtl_instruemt_ref_config(table_config_map)
        TableConfigLoader.__prepare_keytrans_ltype_config(table_config_map)
        TableConfigLoader.__prepare_keytrans_stype_config(table_config_map)
        TableConfigLoader.__prepare_dim_account(table_config_map)
        TableConfigLoader.__prepare_fact_class_entity_config(table_config_map)
        TableConfigLoader.__prepare_dim_sleeve_config(table_config_map)
        TableConfigLoader.__prepare_brdg_account_relation_config(table_config_map)
        TableConfigLoader.__prepare_fact_account_date_config(table_config_map)
        TableConfigLoader.__prepare_dtl_instrument_desc_config(table_config_map)
        TableConfigLoader.__prepare_dim_advisor_config(table_config_map)
        TableConfigLoader.__prepare_dim_business_unit_config(table_config_map)
        TableConfigLoader.__prepare_dtl_fi_schedule_config(table_config_map)
        TableConfigLoader.__prepare_dim_corp_action_decl_config(table_config_map)
        TableConfigLoader.__prepare_dim_instrument_config(table_config_map)
        TableConfigLoader.__prepare_dim_account_group(table_config_map)
        TableConfigLoader.__prepare_mtdt_instrument_id_gen_config(table_config_map)
        TableConfigLoader.__prepare_mtdt_corp_action_decl_id_gen_config(table_config_map)
        TableConfigLoader.__prepare_dim_universe_config(table_config_map)
        TableConfigLoader.__prepare_mtdt_sleeve_id_gen_config(table_config_map)
        TableConfigLoader.__prepare_dim_foa_config(table_config_map)
        TableConfigLoader.__prepare_brdg_entity_relation_config(table_config_map)
        TableConfigLoader.__prepare_brdg_instrument_relation(table_config_map)
        TableConfigLoader.__prepare_dtl_account_ref_config(table_config_map)
        TableConfigLoader.__prepare_dtl_entity_ref(table_config_map)
        TableConfigLoader.__prepare_dim_account_link(table_config_map)
        TableConfigLoader.__prepare_dim_fund_sleeve_config(table_config_map)
        TableConfigLoader.__prepare_dim_targets_config(table_config_map)
        TableConfigLoader.__prepare_mtdt_batch_date(table_config_map)
        TableConfigLoader.__prepare_mtdt_acc_id_gen_config(table_config_map)
        TableConfigLoader.__prepare_dim_sub_group(table_config_map)
        TableConfigLoader.__prepare_wrk_tdr_delta_acc_targets(table_config_map)
        TableConfigLoader.__prepare_dim_sub_advisor(table_config_map)
        JobContext.logger.info("Table configuration prepared")
        return table_config_map

    # Record with stub
    @staticmethod
    def __prepare_dim_instrument_config(p_table_config_map):
        field_to_cast_map = {'pending_payment_days': 'cast:string', 'interest_rt': 'cast:double'}
        column_list= ["instrument_id","issue_dt","start_dt","maturity_dt","end_dt","first_accrual_dt",
                      "first_coupon_dt","pre_refund_dt","sor_instrument_id","instrument_desc","source_system_id","currency_cd",
                      "instrument_name_1","instrument_name_2","instrument_type_cd","constituent_benchmark_id",
                      "constituent_benchmark_start_dt","constituent_benchmark_end_dt","constituent_benchmark_prct","qty_factor_val",
                      "interest_rt","coupon_freq_cd","pending_payment_days","opt_call_put_flg","opt_strike_price_amt",
                      "underlying_instrument_id","year_count_cd","day_count_cd","interest_rate_type_cd","firm_id","active_flg","batch_dt",
                      "created_program_nm","created_user_nm","created_ts","modified_program","modified_user","modified_ts"]
        TableConfigLoader.__create_table_config(p_table_config_map, field_to_cast_map, True, C_GLUE_S3_TABLE_DIM_INSTRUMENT_PROP_KEY,column_list= column_list)

    @staticmethod
    def __prepare_dim_corp_action_decl_config(p_table_config_map):
        column_list = ["corp_action_decl_id","pay_dt","ex_dt","record_dt","sor_corp_action_id",
                       "source_system_id","corp_action_type_cd","original_instrument_id","cash_rt",
                       "corp_action_index","voluntary_flg","firm_id","active_flg","batch_dt",
                       "created_program_nm","created_user_nm","created_ts","modified_program_nm",
                       "modified_user_nm","modified_ts"]
        TableConfigLoader.__create_table_config(p_table_config_map, None, True, C_GLUE_S3_TABLE_DIM_CORP_ACTION_DECL_PROP_KEY,column_list= column_list)

    @staticmethod
    def __prepare_dtl_instruemt_ref_config(p_table_config_map):
        field_to_cast_map = {'sor_account_id': 'cast:string', 'account_nm': 'cast:string'}
        column_list = ["instrument_id","source_system_id","instrument_ref_type_cd","instrument_ref_nm","start_dt","end_dt","firm_id","active_flg","batch_dt","created_program_nm","created_user_nm","created_ts","modified_program","modified_user","modified_ts"]
        TableConfigLoader.__create_table_config(p_table_config_map, field_to_cast_map, True, C_GLUE_S3_TABLE_DTL_INSTRUMENT_REF_PROP_KEY,column_list= column_list)

    @staticmethod
    def __prepare_dtl_instrument_desc_config(p_table_config_map):
        field_to_cast_map = {'line_txt_nbr': 'cast:string'}
        column_list = ["instrument_id","instrument_desc","line_txt_nbr","firm_id","source_system_id","active_flg","batch_dt","created_program_nm","created_user_nm","created_ts","modified_program","modified_user","modified_ts"]
        TableConfigLoader.__create_table_config(p_table_config_map, field_to_cast_map, True, C_GLUE_S3_TABLE_DTL_INSTRUMENT_DESC_PROP_KEY,column_list= column_list)


    @staticmethod
    def __prepare_dim_account(p_table_config_map):
        field_to_cast_map = {'sor_account_id': 'cast:string', 'account_nm': 'cast:string'}
        column_list  = ["account_id","sor_account_id","source_system_id","account_nm","currency_cd","tax_status_cd","load_cache_flg","fiscal_year_end","firm_id","active_flg","batch_dt","created_program_nm","created_user_nm","created_ts","modified_program_nm","modified_user_nm","modified_ts"]
        TableConfigLoader.__create_table_config(p_table_config_map, field_to_cast_map, True, C_GLUE_S3_TABLE_DIM_ACCOUNT_PROP_KEY,column_list = column_list)

    # Record with stub
    @staticmethod
    def __prepare_keytrans_ltype_config(p_table_config_map):
        field_to_cast_map =  {'tx_date': 'cast:string','sys_date': 'cast:string', 'set_date': 'cast:string'}
        TableConfigLoader.__create_table_config(p_table_config_map, field_to_cast_map, False, C_GLUE_S3_TABLE_TRANS_LTYPE, True)

    # Record with stub
    @staticmethod
    def __prepare_keytrans_stype_config(p_table_config_map):
        field_to_cast_map =  {'tx_date': 'cast:string','sys_date': 'cast:string'}
        TableConfigLoader.__create_table_config(p_table_config_map, field_to_cast_map, False, C_GLUE_S3_TABLE_TRANS_STYPE, True)
        # Record with stub

    @staticmethod
    def __prepare_fact_class_entity_config(p_table_config_map):
        field_to_cast_map =  {'entity_id': 'cast:string'}
        column_list =["entity_id","class_type_id","entity_type_cd","overriding_entity_type_cd","overriding_entity_id","effective_from_dt","effective_to_dt","allocation_prcnt","firm_id","active_flg","batch_dt","created_program_nm","created_user_nm","created_ts"]
        TableConfigLoader.__create_table_config(p_table_config_map, field_to_cast_map, True, C_S3_FACT_CLASS_ENTITY_TBL_NME, column_list =column_list)

    #for vestmark
    @staticmethod
    def __prepare_dim_sleeve_config(p_table_config_map):
        field_to_cast_map =  {'sor_sleeve_id': 'cast:string'}
        column_list =["sleeve_id","sor_sleeve_id","sleeve_gf_id","sleeve_nm","sleeve_desc","sleeve_type_nm","primary_model_id","money_manager_id","additional_models_id","currency_cd","firm_id","active_flg","batch_dt","created_program_nm","created_ts","created_user_nm","modified_program_nm","modified_user_nm","modified_ts"]
        TableConfigLoader.__create_table_config(p_table_config_map, field_to_cast_map, True, C_GLUE_S3_TABLE_DIM_SLEEVE_PROP_KEY,column_list = column_list)

    @staticmethod
    def __prepare_brdg_account_relation_config(p_table_config_map):
        column_list = ["account_id","entity_type_cd","entity_id","start_dt","end_dt","relation_type_cd","target_alloc_prcnt","manual_override_flg","inc_reason_cd","inc_notes","exc_reason_cd","exc_notes","firm_id","active_flg","batch_dt","created_program_nm","created_user_nm","created_ts","modified_program_nm","modified_user_nm","modified_ts"]
        TableConfigLoader.__create_table_config(p_table_config_map, None, True, C_GLUE_S3_TABLE_BRDG_ACC_RELATION_PROP_KEY,column_list = column_list)

    @staticmethod
    def __prepare_fact_account_date_config(p_table_config_map):
        column_list = ['account_id','entity_type_cd','entity_id','date_type_cd','date_type_dt',
                                                              'perf_cont_flg','foa_cd','reason_cd','firm_id','program_ind','program_num_id','active_flg','batch_dt',
                                                              'created_program_nm','created_user_nm','created_ts']
        TableConfigLoader.__create_table_config(p_table_config_map, None, True, C_GLUE_S3_TABLE_FACT_ACC_DATE_PROP_KEY, column_list = column_list)

    @staticmethod
    def __prepare_dim_advisor_config(p_table_config_map):
        column_list = ['advisor_id','business_unit_id','sor_advisor_id','advisor_nm','firm_id','active_flg','batch_dt','created_program_nm','created_user_nm','created_ts','modified_program_nm','modified_user_nm','modified_ts']
        TableConfigLoader.__create_table_config(p_table_config_map, None, True, C_GLUE_S3_TABLE_DIM_ADVISOR_PROP_KEY, column_list = column_list)

    @staticmethod
    def __prepare_dim_business_unit_config(p_table_config_map):
        column_list = ['business_unit_id','sor_business_unit_id','business_unit_name','parent_business_unit_id','child_business_unit_id','firm_id','active_flg','batch_dt','created_program_nm','created_user_nm','created_ts','modified_program_nm','modified_user_nm','modified_ts']
        TableConfigLoader.__create_table_config(p_table_config_map, None, True, C_GLUE_S3_TABLE_DIM_BUSINESS_UNIT_PROP_KEY, column_list = column_list)

    @staticmethod
    def __prepare_dtl_fi_schedule_config(p_table_config_map):
        column_list = ["instrument_id","schedule_type_cd","start_dt","end_dt","schedule_type_dt","interest_rt","call_put_price_amt","do_not_accrue_flg","default_flg","firm_id","active_flg","batch_dt","created_program_nm","created_user_nm","created_ts","modified_program","modified_user","modified_ts"]
        TableConfigLoader.__create_table_config(p_table_config_map, None, True, C_GLUE_S3_TABLE_DTL_FI_SCHEDULE_PROP_KEY, column_list = column_list)


    @staticmethod
    def __prepare_dim_account_group(p_table_config_map):
        TableConfigLoader.__create_table_config(p_table_config_map, None, True, C_GLUE_S3_TABLE_DIM_ACCOUNT_GROUP_PROP_KEY)

    @staticmethod
    def __prepare_mtdt_instrument_id_gen_config(p_table_config_map):
        column_list = ["instrument_id","instrument_ref_nm","active_flg","created_user_nm","created_ts"]
        TableConfigLoader.__create_table_config(p_table_config_map, None, True, C_GLUE_S3_TABLE_MTDT_INSTR_ID_GEN_PROP_KEY, column_list = column_list)

    @staticmethod
    def __prepare_mtdt_corp_action_decl_id_gen_config(p_table_config_map):
        column_list = ["corp_action_decl_id","sor_corp_action_id","active_flg","created_user_nm","created_ts"]
        TableConfigLoader.__create_table_config(p_table_config_map, None, True, C_GLUE_S3_TABLE_MTDT_CORP_ACTN_DECL_ID_GEN_PROP_KEY, column_list = column_list)

    @staticmethod
    def __prepare_dim_universe_config(p_table_config_map):
        column_list = ["universe_cd","universe_desc","active_flg","batch_dt","created_program_nm","created_user_nm","created_ts"]
        TableConfigLoader.__create_table_config(p_table_config_map, None, True, C_GLUE_S3_TABLE_DIM_UNIVERSE_PROP_KEY, column_list = column_list)

    @staticmethod
    def __prepare_mtdt_sleeve_id_gen_config(p_table_config_map):
        column_list = ["sleeve_gf_id","sleeve_id","instrument_id","active_flg","created_user_nm","created_ts"]
        TableConfigLoader.__create_table_config(p_table_config_map, None, True, C_GLUE_S3_TABLE_MTDT_SLEVE_ID_GEN_PROP_KEY, column_list = column_list)

    @staticmethod
    def __prepare_dim_foa_config(p_table_config_map):
        column_list = ["foa_cd","foa_nm","program_num_id","active_flg","batch_dt","created_program_nm","created_user_nm","created_ts"]
        TableConfigLoader.__create_table_config(p_table_config_map, None, True, C_GLUE_S3_TABLE_DIM_FOA_PROP_KEY, column_list = column_list)

    @staticmethod
    def __prepare_brdg_entity_relation_config(p_table_config_map):
        column_list = ["entity_id","entity_type_cd","related_entity_id","related_entity_type_cd","start_dt",
                       "end_dt","relation_type_cd","target_alloc_prcnt","manual_override_flg","inc_reason_cd",
                       "inc_notes","exc_reason_cd","exc_notes","firm_id","active_flg","batch_dt",
                       "created_program_nm","created_user_nm","created_ts","modified_program_nm",
                       "modified_user_nm","modified_ts"]
        TableConfigLoader.__create_table_config(p_table_config_map, None, True, C_GLUE_S3_TABLE_BRDG_ENTITY_RELATION_PROP_KEY, column_list = column_list)

    @staticmethod
    def __prepare_dim_account_link(p_table_config_map):
        column_list = ['new_account_id','new_sleeve_id','new_account_uan_id','old_account_id','old_sleeve_id','old_account_uan_id','start_dt','end_dt',
                       'perf_level_flg','link_type_cd','firm_id','active_flg','batch_dt','created_program_nm','created_user_nm', 'created_ts']
        TableConfigLoader.__create_table_config(p_table_config_map, None, True, C_GLUE_S3_TABLE_DIM_ACCOUNT_LINK_PROP_KEY, column_list = column_list)

    @staticmethod
    def __prepare_dim_fund_sleeve_config(p_table_config_map):
        column_list = ["sor_sleeve_id","instrument_id","firm_id","active_flg","batch_dt","created_program_nm","created_user_nm","created_ts"]
        TableConfigLoader.__create_table_config(p_table_config_map, None, True, C_GLUE_S3_TABLE_DIM_FUND_SLEEVE_PROP_KEY, column_list = column_list)

    @staticmethod
    def __prepare_dim_targets_config(p_table_config_map):
        column_list = ["account_id","program_cd","program_num_id","foa_cd","instrument_id","target_alloc_prcnt","start_dt","end_dt","active_flg",
                       "batch_dt","created_program_nm","created_user_nm","created_ts"]
        TableConfigLoader.__create_table_config(p_table_config_map, None, True, C_GLUE_S3_TABLE_DIM_TARGETS_PROP_KEY, column_list = column_list)

    @staticmethod
    def __create_table_config(p_table_config_map, p_field_to_cast_map, p_include_stub, p_table_name, p_partitioned_rec=False, column_list = None):
        dim_class_choice_confg = ResolveChoiceConfig()
        dim_class_choice_confg.fields_to_cast_mapping = p_field_to_cast_map
        dim_class_tbl_config = TableConfig(dim_class_choice_confg, p_include_stub, p_partitioned_rec, column_list)
        JobContext.logger.info("Prepared table configuration for {} is {}".format(p_table_name, dim_class_tbl_config))
        p_table_config_map[JobContext.get_property(p_table_name)] = dim_class_tbl_config

    @staticmethod
    def __prepare_brdg_instrument_relation(p_table_config_map):
        TableConfigLoader.__create_table_config(p_table_config_map, None, True, C_GLUE_S3_TABLE_BRDG_INSTRU_REL_PROP_KEY)

    @staticmethod
    def __prepare_dtl_account_ref_config(p_table_config_map):
        column_list = ["account_id","account_ref_type_cd","account_ref_nm","active_flg","batch_dt","start_dt","end_dt",
                       "created_program_nm","created_user_nm","created_ts"]
        TableConfigLoader.__create_table_config(p_table_config_map, None, True, C_GLUE_S3_TABLE_DTL_ACCOUNT_REF_PROP_KEY, column_list = column_list)


    @staticmethod
    def __prepare_dtl_entity_ref(p_table_config_map):
        TableConfigLoader.__create_table_config(p_table_config_map, None, True, C_GLUE_S3_TABLE_DTL_ENTITY_REF_PROP_KEY)

    @staticmethod
    def __prepare_mtdt_batch_date(p_table_config_map):
        TableConfigLoader.__create_table_config(p_table_config_map, None, True, C_GLUE_S3_TABLE_MTDT_BATCH_DATE_PROP_KEY)


    @staticmethod
    def __prepare_mtdt_acc_id_gen_config(p_table_config_map):
        TableConfigLoader.__create_table_config(p_table_config_map, None, True, C_GLUE_S3_TABLE_MTDT_ACC_BEN_ID_GEN)

    @staticmethod
    def __prepare_dim_sub_group(p_table_config_map):
        column_list = ["account_group_id","sub_group_id","sub_group_type_cd","active_flg","batch_dt","created_program_nm","created_user_nm","created_ts"]
        TableConfigLoader.__create_table_config(p_table_config_map, None, True, C_GLUE_S3_TABLE_DIM_SUB_GROUP_PROP_KEY, column_list = column_list)

    @staticmethod
    def __prepare_wrk_tdr_delta_acc_targets(p_table_config_map):
        TableConfigLoader.__create_table_config(p_table_config_map, None, True, C_GLUE_S3_TABLE_DELTA_ACCOUNT_TARGETS_PROP_KEY)

    @staticmethod
    def __prepare_dim_sub_advisor(p_table_config_map):
        TableConfigLoader.__create_table_config(p_table_config_map, None, True, C_GLUE_S3_TABLE_DIM_SUB_ADVISOR_PROP_KEY)
