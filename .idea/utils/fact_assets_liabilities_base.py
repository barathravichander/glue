# This Script is used to data from stg to fact_assets_liabilities.
# --------------------------------------------------------------------------
#                   History
#  Date            Version No     Developer      Change
# -------          ----------     ---------      ------
# 06/08/2021       0.1            Uday V         If ow_account_nm is less than 8 characters loaded as None otherwise ow_account_nm.
# 06/09/2021       0.2            Uday V         we had filter condition for instrument_id is not null, removed it,
#                                                now it will load null instrument_id's also.
# 07/12/2021       0.3            Uday V         Added CAPS_SEC_BRD_ASSET_CLS to dim_class_type for generating class_type_id's
# 07/15/2021       0.4            Uday V         Now using sr_class_type_trim instead of sr_class_type,
#                                                zero before node_id from exception also removed.
# 08/042021        0.5            Uday V         Replaced sr_class_type_trim with sr_class_type, other jobs of
#                                                liabilities are effecting due to _derive_class_type_id changes
# 08/30/2021       0.6            Uday V         For jobs having @ accounts, not using account_id derivation method
#                                                (_derive_account_id), instead for each account_nm append along with
#                                                account_nm-3-1 as account_id and we will only load account_id in
#                                                fact_assets_liabilities table and accounts without @ will use
#                                                stub logic.
# 09/07/2021       0.7            Uday V         Changes at class_type_id derivation logic.
# 09/29/2021       0.8            Uday V         Added code fix load_fact_assets_liability job effected due to @ account
#                                                code change, observed at QA.
# --------------------------------------------------------------------------

from constant.common_constant import FACT_ASSETS_LIABILITIES_POS_BAL_FLG_POSITION, FACT_ASSETS_LIABILITIES_POS_BAL_FLG_BALANCE, \
    C_INSTRUMENT_REF_TYPE_CD_PW_SEC, C_CAPS_SEC_STYLE_CHILD_TYPE_CD, C_EMPTY_FILE_LOG_MSG, C_DEFAULT_DECIMAL_TYPE, C_UTF_NULL_CHAR, \
    C_ACCOUNT_REF_TYPE_CD_UAN, C_ACCOUNT_REF_TYPE_CD_OW, C_DEFAULT_START_DATE, C_DEFAULT_END_DATE, C_FIRM_ID, C_CREATED_USER_NM, \
    C_DTL_INSTRUMENT_REF_FIELDS, C_DIM_INSTRUMENT_COLUMN_LIST, C_OUTSIDE_ASSET_CLIENT_NUMBER, C_DI_INSTRUMENT_TYPE_CD_SEC, C_ACCOUNT_REF_TYPE_CD_BPS, \
    BPS_SOR_ACCOUNT_START_WITH, C_DATE_DTPE_CD_OPDT, C_INSTRUMENT_REF_TYPE_CD_CU, C_DCT_CHILD_CLASS_TYPE_CD_SEC_TYPE, C_DCT_CHILD_CLASS_VAL_ASSET, \
    C_FACT_CLASS_ENTITY_REL_ENTITY_TYPE_CD_BENCHMARK, C_FCE_ENTITY_TYPE_CD_SEC,C_DCT_CHILD_CLASS_TYPE_CD_CAPS_SEC_ASSET_CLS,C_CAPS_SEC_BRD_ASSET_CLS_CHILD_TYPE_CD, \
    C_CHILD_CLASS_VAL_LIABILITIES, C_CAPS_SEC_SUB_ASSET_CLS_TYPE_CD
from constant.property_constant import C_GLUE_S3_TABLE_DTL_INSTRUMENT_REF_PROP_KEY, C_GLUE_S3_TABLE_DIM_SOR_CAPS_CLASS_TYPE_REF, \
    C_GLUE_S3_TABLE_DIM_CLASS_TYPE, C_S3_FACT_ASSETS_LIABILITIES_OUTPUT_LOCATION, C_S3_DTL_ACCOUNT_REF_OUTPUT_LOCATION_PROP_KEY, \
    C_S3_DIM_ACCOUNT_OUTPUT_LOCATION_PROP_KEY, C_S3_DTL_INSTRUMENT_REF_OUTPUT_LOCATION_PROP_KEY, C_S3_DIM_INSTRUMENT_OUTPUT_LOC_PROP_KEY, \
    C_S3_FACT_ACC_DATE_OUTPUT_LOCATION_PROP_KEY, C_S3_FACT_ASSETS_LIABILITIES_EXCP_OUTPUT_LOCATION, C_S3_FACT_CLASS_ENTITY_OUTPUT_LOCATION
from pyspark.sql import functions as sf
from utils.GlueJobBase import *
from utils.common_derivations_util import derive_instrument_id, derive_account_id, derive_currency_cd, derive_src_sys_id, mtdt_instrument_id_gen
from utils.common_util import transform_boolean_clm, add_default_clm, add_audit_columns, trim_space, alias_ontology_to_column, isNull_check, \
    prepare_exception_clm_list, create_empty_dataframe, remove_unwanted_char
from utils.data_validations_util import prepare_validation_map, NotNullValidator, validate_rec_and_filter, NotEqualValidator
from utils.s3_operations_util import read_from_s3_catalog_convert_to_df, read_from_catalog_and_filter_inactive_rec, save_output_to_s3, \
    read_json_from_s3
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql.window import Window


class FactAssetLiabilities(GlueJobBase):

    C_REQUIRED_CLMS=["sr_uan_account_nm","sr_ow_account_nm","src_instrument_nm","sr_class_type","sr_effective_dt","sr_mv_val",
                     "src_loan_desc","sr_custodian_name","src_currency_cd","sr_product_type","sr_credit_card_num","sr_client_id","src_available_amt_val",
                     "sr_quantity","sr_cost_basis","sr_price","sr_current_price","sr_purchase_date","sr_custodian_short_nm","sr_account_type_cd",
                     "sr_refresh_dt","sr_last_refresh_status","sr_last_refresh_status_msg","sr_outside_account_type_desc","sr_outside_sub_account_type_desc",
                     "sr_cusip_number","sr_isin_id","sr_sedol_id","sr_symbol_id","security_type"]

    clms_present =[]
    C_SRC_DATE_FORMAT="yyyy-MM-dd"
    C_ADDITIONAL_SRC_DATE_FORMAT='MM/dd/yyyy'

    C_DEFAULT_CLM_MAP={
        "custodian_account_num":None,"comment_txt":None
    }

    FAC_ASSETS_LIABILITIES_CLMS=["account_id","asset_liability_flg","outside_flg","manual_flg","instrument_id","class_type_id","effective_dt","quantity_val",
                                 "mv_val", "available_amt_val","pos_bal_flg","purchase_price_val","purchase_dt","current_price_val","currency_cd","costbasis_val","loan_desc","product_type_desc",
                                 "credit_card_num","custodian_nm","custodian_short_nm","custodian_account_num","client_id","comment_txt","account_type_cd","outside_account_type_desc",
                                 "outside_sub_account_type_desc","refresh_dt","last_refresh_status","last_refresh_status_msg","security_type","active_flg","batch_dt","created_program_nm","created_user_nm","created_ts"]

    C_DTL_ACCOUNT_REF_DEFAULT_MAPPING = { 'start_dt':C_DEFAULT_START_DATE,'end_dt':C_DEFAULT_END_DATE}
    C_DIM_ACCOUNT_DEFAULT_MAPPING = { 'firm_id':C_FIRM_ID,'tax_status_id':None,
                                      'load_cache_flg':None,'fiscal_year_end':None}


    ###############################
    # This method will return list of column with default values in it
    ###############################
    C_DIM_INSTRUMENT_FIELDS_DEFAUT_MAPPING =  {"start_dt":C_DEFAULT_START_DATE,"end_dt":C_DEFAULT_END_DATE,'issue_dt': None, 'maturity_dt': None,
                                               'first_accrual_dt': None, 'first_coupon_dt': None, 'pre_refund_dt': None, 'instrument_desc': None,
                'instrument_name_1': None,'instrument_name_2': None, 'constituent_benchmark_id': None, 'constituent_benchmark_start_dt': None,
                'constituent_benchmark_end_dt': None,'constituent_benchmark_prct': None, 'qty_factor_val': None, 'interest_rt': None, 'coupon_freq_cd': None,
                'pending_payment_days': None, 'opt_call_put_flg': None,
                'opt_strike_price_amt': None, 'underlying_instrument_id': None, 'year_count_cd': None, 'day_count_cd': None, 'interest_rate_type_cd': None,
                "firm_id": C_FIRM_ID}
    C_DIM_ACCOUNT_OUTPUT_CLMS = ["account_id","sor_account_id","source_system_id","account_nm","currency_cd","tax_status_id",
                                 "load_cache_flg","fiscal_year_end","firm_id","active_flg","batch_dt","created_program_nm",
                                 "created_user_nm","created_ts","modified_program_nm","modified_user_nm","modified_ts"]


    C_DTL_ACCOUNT_REF_COLUMN_LIST = ["account_id", "account_ref_type_cd", "account_ref_nm", "active_flg", "batch_dt",
                                 "start_dt", "end_dt","created_program_nm", "created_user_nm", "created_ts"]


    C_FACT_ACC_DATE_COLUMN_LIST = ['account_id','entity_type_cd','entity_id','date_type_cd','date_type_dt',
                                   'perf_cont_flg','foa_cd','reason_cd','firm_id','program_ind','program_num_id','active_flg','batch_dt',
                                   'created_program_nm','created_user_nm','created_ts']
    C_FACT_ACC_DATE_DEFAULT_COL_LIST = {
        'entity_type_cd':None,
        'entity_id':None,
        "date_type_cd":C_DATE_DTPE_CD_OPDT,
        "perf_cont_flg": None,
        "foa_cd": None,
        "reason_cd": None,
        "program_ind": None,
        "program_num_id":None,
        'firm_id': C_FIRM_ID

        }
    C_FACT_CLASS_ENTITY_COL_LIST = ['entity_id','class_type_id','entity_type_cd','overriding_entity_type_cd',
                                    'overriding_entity_id','effective_from_dt','effective_to_dt','allocation_prcnt',
                                    'firm_id','active_flg','batch_dt','created_program_nm','created_user_nm','created_ts']

    C_FACT_ASSET_FIELD_TO_VALIDATE_AGAINST = {"class_type_id_validate": [NotNullValidator('sr_class_type', 'Supplied node_id is not present in the system :')]}

    ow_account_branch_clm = "ow_account_branch"
    ow_account_number_clm = "ow_account_number"
    universal_account_number_clm_nm="universal_account_number"
    c_client_num = C_OUTSIDE_ASSET_CLIENT_NUMBER
    # If new char came just put | and char ex=> "@|$"
    C_SPEICAL_REGEX_IN_ACCOUNT="&"
    default_instrument_ref_type_cd = C_INSTRUMENT_REF_TYPE_CD_CU
    C_MAC_ALLOWED_CHAR_IN_UAN = 10

    def _execute_job(self):

        self.logger.info(f"Starting {self._get_file_type()} to fact_asset_liabilities transformations")
        src_df = self._read_src_and_stage()
        if src_df is None:
            self.logger.info(C_EMPTY_FILE_LOG_MSG)
            return
        curr_pr_clms = src_df.schema.names

        # if source has not provided required column, then add them with None value, and keep track of it
        src_df = self.__prepare_df_as_required(curr_pr_clms, src_df)
        src_df = src_df.withColumn("sr_ow_account_nm", sf.regexp_replace(sf.col("sr_ow_account_nm"), "\\s+", ""))
        self.src_sys_id = derive_src_sys_id(self.c_client_num)
        self.currency_Cd = derive_currency_cd()

        created_progrm_nm = self._get_property("JOB_NAME")[:20]
        match_df1 = src_df.withColumn("created_program_nm", sf.lit(created_progrm_nm.upper()))
        created_prg_nm_1 = match_df1.select(sf.col("created_program_nm")).collect()[0][0]

        if created_prg_nm_1 == 'LOAD_FACT_ASSET_LIAB':
            src_df = self._derive_account_id(src_df)
        else:
            src_df = self._derive_at_the_rate_account_id(src_df)

        src_df = self._derive_instrument_id(src_df)
        src_df = self._derive_class_type_id(src_df)
        src_df = self._derive_currency_cd(src_df)

        src_df = src_df.withColumn("effective_dt",sf.coalesce(sf.to_date(sf.col("sr_effective_dt"), self.C_SRC_DATE_FORMAT), self.g_batch_date_clm))

        if "sr_purchase_date" in self.clms_present:
            self.logger.info("Using source purchase date column value")
            src_df = src_df.withColumn("purchase_dt",sf.coalesce(sf.to_date(sf.col("sr_purchase_date"), self.C_SRC_DATE_FORMAT),
                                                                sf.to_date(sf.col("sr_purchase_date"), self.C_ADDITIONAL_SRC_DATE_FORMAT), self.g_batch_date_clm))
        else:
            # set null if srouce has not passed it
            src_df = src_df.withColumn("purchase_dt",sf.lit(None))

        src_df = src_df.withColumnRenamed("sr_mv_val","mv_val") \
            .withColumnRenamed("src_loan_desc","loan_desc") \
            .withColumnRenamed("sr_custodian_name","custodian_nm") \
            .withColumnRenamed("sr_product_type","product_type_desc") \
            .withColumnRenamed("sr_client_id","client_id") \
            .withColumnRenamed("sr_credit_card_num","credit_card_num") \
            .withColumnRenamed("src_available_amt_val","available_amt_val") \
            .withColumnRenamed("sr_quantity","quantity_val") \
            .withColumnRenamed("sr_cost_basis","costbasis_val") \
            .withColumnRenamed("sr_price","purchase_price_val") \
            .withColumnRenamed("sr_current_price","current_price_val") \
            .withColumnRenamed("sr_custodian_short_nm","custodian_short_nm") \
            .withColumnRenamed("sr_refresh_dt","refresh_dt") \
            .withColumnRenamed("sr_last_refresh_status","last_refresh_status") \
            .withColumnRenamed("sr_last_refresh_status_msg","last_refresh_status_msg") \
            .withColumnRenamed("sr_outside_account_type_desc","outside_account_type_desc") \
            .withColumnRenamed("sr_account_type_cd","account_type_cd") \
            .withColumnRenamed("sr_outside_sub_account_type_desc","outside_sub_account_type_desc")



        default_clms_map = self.__prepare_default_mappings()

        src_df = add_default_clm(src_df,default_clms_map)
        src_df = add_audit_columns(src_df,self.g_batch_date_clm)
        src_df = validate_rec_and_filter(src_df, self.C_FACT_ASSET_FIELD_TO_VALIDATE_AGAINST,
                                           self._get_property(C_S3_FACT_ASSETS_LIABILITIES_EXCP_OUTPUT_LOCATION),
                                           prepare_exception_clm_list(self.FAC_ASSETS_LIABILITIES_CLMS))
        save_output_to_s3(src_df.select(*self.FAC_ASSETS_LIABILITIES_CLMS),"src", self._get_property(C_S3_FACT_ASSETS_LIABILITIES_OUTPUT_LOCATION))
        self.logger.info(f"Completed {self._get_file_type()} to fact_asset_liabilities transformations")

    """
        It could happen that Different sources has different default values, so first check if they have given some default value, 
        if not, then use common default values
        TODO:
    """
    def __prepare_default_mappings(self):
        default_clms_map = self._get_default_column_mapping()
        for k, v in self.C_DEFAULT_CLM_MAP.items():
            if k not in default_clms_map:
                default_clms_map[k] = v
        return default_clms_map

    def _derive_at_the_rate_account_id(self, src_df):
        # deriving the account_id for @ accounts by concatination of account_nm-3-1 manually and only loading
        # account_id to fact_assets_liabilities table only and for remaining accounts uses stub logic.
        clms = src_df.schema.names
        clms.append('account_id')

        src_df1 = src_df.filter(sf.col('sr_ow_account_nm').like("@%"))

        if src_df1 is None:
            JobContext.logger.info("ow_account with @ accounts are not present")
            src_df1 = create_empty_dataframe(clms)
        else:
            src_df1 = src_df1.withColumn("sr_ow_account_nm_length", sf.length(src_df1.sr_ow_account_nm))
            src_df1 = src_df1.withColumn("sr_ow_account_nm", sf.when(sf.col("sr_ow_account_nm_length") > 7, sf.col("sr_ow_account_nm")).otherwise(sf.lit(None)))
            src_df1 = src_df1.withColumn("account_nm", sf.when(isNull_check("sr_ow_account_nm"), sf.col("sr_uan_account_nm"))
                                         .otherwise(sf.col("sr_ow_account_nm"))) \
                .withColumn("account_id", sf.concat_ws('-', sf.col("account_nm"), sf.lit(self.src_sys_id), sf.lit(C_FIRM_ID)))
            src_df1.cache()

        src_df = src_df.filter(~sf.col('sr_ow_account_nm').like("@%"))

        if src_df is None:
            JobContext.logger.info("Normal accounts are not present")
            src_df = create_empty_dataframe(clms)
        else:
            src_df = self._derive_account_id(src_df)

        src_df = src_df.select(clms).union(src_df1.select(clms))
        return src_df

    def _derive_currency_cd(self, src_df):

        if "src_currency_cd" in self.clms_present:
            self.logger.info("Source have currency_cd column")
            src_df = src_df.withColumn("currency_cd", sf.when(isNull_check("src_currency_cd"), sf.lit(self.currency_Cd) ).otherwise(sf.col("src_currency_cd")))
        else:
            self.logger.info("Source does not have currency_cd column, adding default currency")
            src_df = src_df.withColumn("currency_cd",sf.lit(self.currency_Cd))
        return src_df

    """
        In derive_class_type_id method changes had made to Automated and Manual outside holdings job, added CAPS_SEC_BRD_ASSET_CLS
        now more records will generate at fact_assets_liabilities table, doesn't match with source count and for manual job 
        considering all four sor_caps_node_cd and for Auto only one sor_caps_node_cd.
    """

    def _derive_class_type_id(self, src_df):

        if "sr_class_type" in self.clms_present:
            self.logger.info("Source have class_type column, so deriving class_type_id value")
            dim_sor_caps_class_type_ref_df = read_from_catalog_and_filter_inactive_rec(self._get_property(C_GLUE_S3_TABLE_DIM_SOR_CAPS_CLASS_TYPE_REF))
            dim_sor_caps_class_type_ref_df=dim_sor_caps_class_type_ref_df.select('sor_caps_node_cd','caps_sec_style_desc','caps_sec_sub_asset_cls_desc','caps_sec_brd_ast_cls_desc')
            dim_class_type_df= read_from_catalog_and_filter_inactive_rec(self._get_property(C_GLUE_S3_TABLE_DIM_CLASS_TYPE))
            dim_class_type_df = dim_class_type_df.filter(sf.col('child_class_type_cd').isin(C_CAPS_SEC_STYLE_CHILD_TYPE_CD, C_CAPS_SEC_SUB_ASSET_CLS_TYPE_CD, C_CAPS_SEC_BRD_ASSET_CLS_CHILD_TYPE_CD))

            created_progrm_nm = self._get_property("JOB_NAME")[:20]
            match_df = dim_sor_caps_class_type_ref_df.withColumn("created_program_nm", sf.lit(created_progrm_nm.upper()))
            created_prg_nm = match_df.select(sf.col("created_program_nm")).collect()[0][0]

            if created_prg_nm == 'MANUAL_OUTSIDE_HOLDI':
                df1=dim_sor_caps_class_type_ref_df.withColumn("derv_sor_sec_type_node_cd",sf.split(sf.col('sor_caps_node_cd'),'_').getItem(0))
                df2=dim_sor_caps_class_type_ref_df.withColumn("derv_sor_sec_type_node_cd",sf.split(sf.col('sor_caps_node_cd'),'_').getItem(1))
                df3=dim_sor_caps_class_type_ref_df.withColumn("derv_sor_sec_type_node_cd",sf.split(sf.col('sor_caps_node_cd'),'_').getItem(2))
                df4=dim_sor_caps_class_type_ref_df.withColumn("derv_sor_sec_type_node_cd",sf.split(sf.col('sor_caps_node_cd'),'_').getItem(3))
                dim_sor_caps_class_type_ref_df = df1.union(df2).union(df3).union(df4)
            else:
                dim_sor_caps_class_type_ref_df=dim_sor_caps_class_type_ref_df.withColumn("derv_sor_sec_type_node_cd",sf.split(sf.col('sor_caps_node_cd'),'_').getItem(3))

            src_df = src_df.withColumn("sr_class_type", sf.col("sr_class_type").cast(IntegerType()).cast(StringType()))

            src_df = src_df.alias("src").join(dim_sor_caps_class_type_ref_df.alias("dim"), sf.col("src.sr_class_type") == sf.col("dim.derv_sor_sec_type_node_cd"),'left')\
                .select(src_df['*'], sf.col("caps_sec_style_desc"), sf.col("caps_sec_sub_asset_cls_desc"),sf.col("caps_sec_brd_ast_cls_desc"))

            src_df = src_df.withColumn("child_class_val_src", sf.when(sf.col("sr_class_type").isin(C_CHILD_CLASS_VAL_LIABILITIES), sf.col("caps_sec_sub_asset_cls_desc")).otherwise(sf.col("caps_sec_style_desc")))
            src_df = src_df.withColumn("dervd_child_class_type_cd", sf.when(sf.col("sr_class_type").isin(C_CHILD_CLASS_VAL_LIABILITIES), sf.lit(C_CAPS_SEC_SUB_ASSET_CLS_TYPE_CD))
                                       .otherwise(sf.lit(C_CAPS_SEC_STYLE_CHILD_TYPE_CD)))
            src_broad_asset_df = src_df

            src_df = src_df.alias("src").join(dim_class_type_df.alias("dim"), (sf.col("src.child_class_val_src") == sf.col("dim.child_class_val")) &
                                              (sf.col("dim.child_class_type_cd") == sf.col("src.dervd_child_class_type_cd")), "left") \
                .select(src_df['*'], dim_class_type_df.class_type_id)

            clms = src_df.schema.names

            src_broad_asset_df = src_broad_asset_df.join(dim_class_type_df.alias("dim"), (sf.col("caps_sec_brd_ast_cls_desc") == sf.col("dim.child_class_val")) &
                                                         (sf.col("dim.child_class_type_cd") == C_CAPS_SEC_BRD_ASSET_CLS_CHILD_TYPE_CD), "left") \
                .select(src_broad_asset_df['*'], dim_class_type_df.class_type_id)

            src_df = src_df.select(clms).union(src_broad_asset_df.select(clms))

            src_df = src_df.withColumn("class_type_id_validate", sf.col("class_type_id"))
        else:
            self.logger.info("Source does not have class_type column, so adding class_type_id with None value")
            src_df = src_df.withColumn("class_type_id",sf.lit(None)).withColumn("class_type_id_validate",sf.lit("dummy"))

        return src_df

    """
        At present we are not using this _derive_instrument_id method, we had this method at auto_holdings and manual_holdings jobs itself.
        Note that no records are loading to dim_instrument, dtl_instrument_ref and fact_calss_entity table.
    """

    def _derive_instrument_id(self, src_df):

        if "src_instrument_nm" in self.clms_present:
            self.logger.info("Source have security column, so deriving instrument_id value")
            dtl_instrument_df = read_from_s3_catalog_convert_to_df(self.C_DB_NAME,self._get_property(C_GLUE_S3_TABLE_DTL_INSTRUMENT_REF_PROP_KEY))
            dtl_instrument_df = transform_boolean_clm(dtl_instrument_df,"active_flg")
            joining_cond = (sf.col("src_instrument_nm") == sf.col("instrument_ref_nm"))
            src_df = derive_instrument_id(src_df, dtl_instrument_df, joining_cond, self.g_batch_date_clm,
                                          derived_instrument_id_alias="instrument_id", broadcast=False)
            src_df = src_df.withColumn("pos_bal_flg", sf.lit(FACT_ASSETS_LIABILITIES_POS_BAL_FLG_POSITION))

            clms = src_df.schema.names
            src_instrument_id_null_df = src_df.filter(sf.col("instrument_id").isNull() & (~isNull_check("src_instrument_nm"))).drop("instrument_id")

            new_instrument_df = mtdt_instrument_id_gen(self, src_instrument_id_null_df,in_df_security_col_nm = "src_instrument_nm", created_user_nm=C_CREATED_USER_NM)
            new_instrument_df.cache()

            src_df = new_instrument_df.select(clms).union(src_df.select(clms))

            new_instrument_df = new_instrument_df.select("src_instrument_nm","instrument_id").distinct() \
                .withColumnRenamed("src_instrument_nm","sor_instrument_id")

            new_instrument_df = new_instrument_df.withColumn("source_system_id", sf.lit(self.src_sys_id)) \
                .withColumn('currency_cd', sf.lit(self.currency_Cd)) \
                .withColumn('inst_len', sf.length(new_instrument_df.sor_instrument_id)) \
                .withColumn('instrument_type_cd', sf.lit(C_DI_INSTRUMENT_TYPE_CD_SEC)) \
                .withColumn('instrument_ref_nm',sf.col('sor_instrument_id'))

            new_instrument_df = self._populate_instrument_ref_type(new_instrument_df)

            new_instrument_df = add_default_clm(new_instrument_df , self.C_DIM_INSTRUMENT_FIELDS_DEFAUT_MAPPING)
            new_instrument_df = add_audit_columns(new_instrument_df,p_batch_date_clm=self.g_batch_date_clm,add_modification_clm=True)
            new_instrument_df.cache()

            save_output_to_s3(new_instrument_df.select(*C_DIM_INSTRUMENT_COLUMN_LIST), "target_df", self._get_property(C_S3_DIM_INSTRUMENT_OUTPUT_LOC_PROP_KEY))
            new_instrument_df = new_instrument_df.withColumnRenamed("modified_user_nm","modified_user") \
                .withColumnRenamed("modified_program_nm","modified_program")
            save_output_to_s3(new_instrument_df.select(*C_DTL_INSTRUMENT_REF_FIELDS),'dtl_instrument_ref',self._get_property(C_S3_DTL_INSTRUMENT_REF_OUTPUT_LOCATION_PROP_KEY))

            self._create_fact_class_entity(src_df)
        else:
            self.logger.info("Source does not have security column, so adding instrument_id with None value")
            src_df = src_df.withColumn("instrument_id",sf.lit(None)) \
                .withColumn("pos_bal_flg", sf.lit(FACT_ASSETS_LIABILITIES_POS_BAL_FLG_BALANCE))

        return src_df

    def _create_fact_class_entity(self, src_df):
        self.logger.info(f"Creating fact_class_entity")
        g_dim_cls_typ_df = read_from_catalog_and_filter_inactive_rec(self._get_property(C_GLUE_S3_TABLE_DIM_CLASS_TYPE),
                                                                     ["class_type_id","child_class_type_cd","child_class_val",
                                                                      "active_flg","created_ts"])
        g_dim_cls_typ_df = g_dim_cls_typ_df.where((sf.upper(sf.col("child_class_type_cd"))==C_DCT_CHILD_CLASS_TYPE_CD_SEC_TYPE)
                                                    & (sf.upper(sf.col("child_class_val"))==C_DCT_CHILD_CLASS_VAL_ASSET))
        class_type_id_df = g_dim_cls_typ_df.select("class_type_id").first()
        if class_type_id_df is None:
            raise Exception(f"Seed data not present in dim_class_type, required,child_class_type_cd={C_DCT_CHILD_CLASS_TYPE_CD_SEC_TYPE}, "
                            f"child_class_val={C_DCT_CHILD_CLASS_VAL_ASSET} ")

        fact_class_df = src_df.select(sf.col("instrument_id").alias("entity_id")).distinct()
        class_type_id = class_type_id_df.asDict()['class_type_id']
        self.logger.info("Class_type_id {}, found for custom blend flg".format(class_type_id))
        C_FACT_CLASS_ENTITY_DEFAULT_CLM_MAPPING = {'entity_type_cd': C_FCE_ENTITY_TYPE_CD_SEC, 'overriding_entity_type_cd': None,
                                                   'overriding_entity_id': None, 'effective_from_dt': C_DEFAULT_START_DATE,
                                                   'effective_to_dt': C_DEFAULT_END_DATE, 'allocation_prcnt': None, 'firm_id': C_FIRM_ID}


        fact_class_df = fact_class_df.withColumn("class_type_id", sf.lit(class_type_id))
        fact_class_df = add_default_clm(fact_class_df,C_FACT_CLASS_ENTITY_DEFAULT_CLM_MAPPING)
        fact_class_df = add_audit_columns(fact_class_df, self.g_batch_date_clm)
        save_output_to_s3(fact_class_df.select(self.C_FACT_CLASS_ENTITY_COL_LIST),"fact_class_entity",self._get_property(C_S3_FACT_CLASS_ENTITY_OUTPUT_LOCATION))

    def _populate_instrument_ref_type(self, src_df):
        self.logger.info(f"Populating instrument_ref_type_cd as {self.default_instrument_ref_type_cd}")
        src_df = src_df.withColumn("instrument_ref_type_cd", sf.lit(self.default_instrument_ref_type_cd ))
        return src_df

    def _derive_account_id(self, src_df):
        self.logger.info("First using own account to get account_id")

        clms = src_df.schema.names
        clms.append("account_id")
        uan_src_df = src_df

        # some source has UAN, some has OWN  some has both, so if source has only UAN derive only using UAN or OW
        if "sr_ow_account_nm" in self.clms_present:
            src_df = src_df.withColumn("sr_ow_account_nm_length", sf.length(src_df.sr_ow_account_nm))
            src_df = src_df.withColumn("sr_ow_account_nm", sf.when(sf.col("sr_ow_account_nm_length") > 7, sf.col("sr_ow_account_nm")).otherwise(sf.lit(None)))
            joining_cond = sf.col('src.sr_ow_account_nm') == sf.col('acc.sor_account_id')
            src_df = src_df.withColumn("sor_account_id", sf.col("sr_ow_account_nm"))
            src_df = derive_account_id(src_df, joining_cond).drop("sor_account_id")
            ow_src_df = src_df.filter(sf.col("account_id").isNotNull())
            uan_src_df = src_df.filter(sf.col("account_id").isNull())
        else:
            ow_src_df = create_empty_dataframe(clms)

        if "sr_uan_account_nm" in self.clms_present:
            uan_src_df = uan_src_df.drop("account_id")
            self.logger.info("Now using uan account to get account_id")
            uan_src_df = uan_src_df.withColumn("sor_account_id", sf.col("sr_ow_account_nm"))
            joining_cond = sf.col('src.sr_uan_account_nm') == sf.col('acc.sor_account_id')
            uan_src_df = derive_account_id(uan_src_df, joining_cond).drop("sor_account_id")

        src_df = uan_src_df.select(clms).union(ow_src_df.select(clms))

        src_df_with_new_acc = self.__create_account_stub(src_df)

        src_df = src_df_with_new_acc.select(clms).union(src_df.filter(sf.col("account_id").isNotNull()).select(clms))
        return src_df

    def __create_account_stub(self, src_df):
        self.logger.info("Creating stub records")
        src_acc_not_df = src_df.filter(sf.col("account_id").isNull())

        src_acc_not_df = src_acc_not_df.withColumn("account_nm", sf.when(isNull_check("sr_ow_account_nm"), sf.col("sr_uan_account_nm"))
                                                   .otherwise(sf.col("sr_ow_account_nm"))) \
            .withColumn("account_id", sf.concat_ws('-', sf.col("account_nm"), sf.lit(self.src_sys_id), sf.lit(C_FIRM_ID)))

        src_acc_not_df.cache()
        src_acc_not_df = src_acc_not_df.withColumn("sr_uan_account_nm", sf.col("sr_uan_account_nm").cast(StringType()))
        stub_src_df = src_acc_not_df.select("sr_ow_account_nm", "sr_uan_account_nm","account_nm","account_id").distinct()
        stack_exp = "stack(3,'{}',sr_uan_account_nm,'{}',sr_ow_account_nm,'{}',sr_ow_account_nm) as (account_ref_type_cd,account_ref_nm)" \
                                                 .format(C_ACCOUNT_REF_TYPE_CD_UAN, C_ACCOUNT_REF_TYPE_CD_OW,C_ACCOUNT_REF_TYPE_CD_BPS)

        base_acc_stub_src_df = stub_src_df.selectExpr("account_nm","account_id", stack_exp)

        base_acc_stub_src_df = base_acc_stub_src_df.filter(~isNull_check("account_ref_nm"))
        base_acc_stub_src_df = base_acc_stub_src_df.withColumn("account_ref_nm", sf.when(sf.col("account_ref_type_cd")==C_ACCOUNT_REF_TYPE_CD_BPS,
                                                                  sf.concat(sf.lit(BPS_SOR_ACCOUNT_START_WITH), sf.col("account_ref_nm"))).otherwise(sf.col("account_ref_nm")))
        dtl_account_df = add_default_clm(base_acc_stub_src_df, self.C_DTL_ACCOUNT_REF_DEFAULT_MAPPING)
        dtl_account_df = add_audit_columns(dtl_account_df, self.g_batch_date_clm)

        save_output_to_s3(dtl_account_df.select(*self.C_DTL_ACCOUNT_REF_COLUMN_LIST), "dtl_account_df",
                          self._get_property(C_S3_DTL_ACCOUNT_REF_OUTPUT_LOCATION_PROP_KEY))

        dim_acc_df = stub_src_df.withColumn("sor_account_id", sf.col("account_nm")) \
            .withColumn("currency_cd",sf.lit(self.currency_Cd)) \
            .withColumn( "source_system_id", sf.lit(self.src_sys_id))
        dim_acc_df = add_default_clm(dim_acc_df, self.C_DIM_ACCOUNT_DEFAULT_MAPPING)
        dim_acc_df = add_audit_columns(dim_acc_df, self.g_batch_date_clm, add_modification_clm=True)
        dim_acc_df.cache()
        save_output_to_s3(dim_acc_df.select(*self.C_DIM_ACCOUNT_OUTPUT_CLMS), "DIM_ACCOUNT",
                          self._get_property(C_S3_DIM_ACCOUNT_OUTPUT_LOCATION_PROP_KEY))

        fact_acc_date_df = add_default_clm(dim_acc_df, self.C_FACT_ACC_DATE_DEFAULT_COL_LIST)
        fact_acc_date_df = fact_acc_date_df.withColumn("date_type_dt", sf.lit(self.g_batch_date_clm))
        save_output_to_s3(fact_acc_date_df.select(*self.C_FACT_ACC_DATE_COLUMN_LIST), "account_present_df",
                          self._get_property(C_S3_FACT_ACC_DATE_OUTPUT_LOCATION_PROP_KEY))

        self.logger.info("stub records created")
        return src_acc_not_df


    def __prepare_df_as_required(self, curr_pr_clms, src_df):
        for rclm in self.C_REQUIRED_CLMS:
            if rclm not in curr_pr_clms:
                self.logger.info(f'{rclm} not present in source Dataframe, adding it with None value')
                src_df = src_df.withColumn(rclm, sf.lit(None))
            else:
                self.clms_present.append(rclm)
        return src_df

    def _read_source_file(self, input_loc):
        self.logger.info("Reading source as Json")
        src_df = read_json_from_s3(input_loc)

        if src_df is None:
            return None
        self.logger.info(f"Exploding json on {self.explode_on}")
        src_df = alias_ontology_to_column(src_df, self.ontology_to_field_map, explode_on=self.explode_on)
        return src_df

    def _read_and_validate(self, input_loc, exp_loc, output_stg_loc, uan_zero_removal=True):
        # Child class provide its own implementation
        src_df = self._read_source_file(input_loc)

        if src_df is None:
            return None

        src_df = trim_space(src_df, self.src_clms_used_in_fact_assets_la)
        src_df = remove_unwanted_char(src_df,C_UTF_NULL_CHAR)

        OW_ACC_VALIDATION= "ow_account_no_validation"

        # as different source can have different column name so, defined it at class level so that child can pass its column name

        src_df = src_df.withColumn("ow_account_no", sf.concat(sf.col(self.ow_account_branch_clm), sf.col(self.ow_account_number_clm))) \
            .withColumn(OW_ACC_VALIDATION, sf.when( isNull_check(self.ow_account_branch_clm)
                                                    | isNull_check(self.ow_account_number_clm), sf.lit(None)).otherwise(sf.lit("Dummy")))

        # in the beginning all json file had 10-12 column for validation, so created method to generate validation map,
        # now if we need to add new column just add in respective list
        clm_validation_map = prepare_validation_map(self.stg_clms, self.clms_for_validation, [],
                                                    src_date_format=self.C_SRC_DATE_FORMAT)
        account_clm_empty_exp_msg = f'Source column for OW account({self.ow_account_number_clm} and/or {self.ow_account_branch_clm}  are empty'
        not_allwoed_char_list = self.C_SPEICAL_REGEX_IN_ACCOUNT.split("|")
        self.logger.info(f"Using {not_allwoed_char_list} for account regex filtering ")

        src_df = src_df.withColumn("ow_acc_special_char", sf.when(sf.col("ow_account_no").rlike(self.C_SPEICAL_REGEX_IN_ACCOUNT),
                                                                  sf.col("ow_account_no")).otherwise(sf.lit("")))
        clm_validation_map["ow_acc_special_char"] = NotEqualValidator([""], f"Source OW account should not contain: {not_allwoed_char_list} chars: ")

        if uan_zero_removal is True:
            src_df = src_df.withColumn('acc_len',sf.length(self.universal_account_number_clm_nm))
            src_df = src_df.withColumn("universal_account_number", sf.when(sf.col("acc_len") == '11',
                                                                           sf.substring(sf.col(self.universal_account_number_clm_nm), 2, 11)).otherwise(sf.col(self.universal_account_number_clm_nm)))

        if self.universal_account_number_clm_nm in src_df.schema.names:
            self.logger.info("As source has UAN and OW both, exception will be raised only if both are empty")
            src_df = src_df.withColumn(OW_ACC_VALIDATION, sf.when(isNull_check(OW_ACC_VALIDATION) & isNull_check(self.universal_account_number_clm_nm),
                                                                  sf.lit(None)).otherwise(sf.lit("Dummy")))
            src_df = src_df.withColumn("uan_acc_special_char", sf.when(sf.col(self.universal_account_number_clm_nm).rlike(self.C_SPEICAL_REGEX_IN_ACCOUNT),
                                                                        sf.col(self.universal_account_number_clm_nm)).otherwise(sf.lit("")))

            clm_validation_map["uan_acc_special_char"] = NotEqualValidator([""], f"Source UAN account should not contain: {not_allwoed_char_list} chars: ")

            src_df = src_df.withColumn("uan_len", sf.when(sf.length(sf.col(self.universal_account_number_clm_nm)) > self.C_MAC_ALLOWED_CHAR_IN_UAN,
                                                          sf.lit(None)).otherwise(sf.lit("dummy")))

            clm_validation_map["uan_len"] = NotNullValidator( "uan_len",f"UAN account should not be more than 10 char")
            clm_validation_map.pop(self.universal_account_number_clm_nm)
            account_clm_empty_exp_msg = f'Source column for both OW account({self.ow_account_number_clm} and/or {self.ow_account_branch_clm}) and UAN account are empty'

        clm_validation_map[OW_ACC_VALIDATION] =  [NotNullValidator(OW_ACC_VALIDATION, account_clm_empty_exp_msg)]

        if isinstance(self.src_mv_clm, list):
            self.logger.info("")
            for clm in self.src_mv_clm:
                C_CLM_VALIDATED_NME=f"{clm}_validate"
                self.logger.info(f"Adding column for decimal validation :{clm}")
                clm_validation_map[C_CLM_VALIDATED_NME] = [NotNullValidator(C_CLM_VALIDATED_NME, f'Supplied amount column {clm} is not matched with excepted decimal format Decimal(26 10): ')]
                src_df = src_df.withColumn(C_CLM_VALIDATED_NME, sf.col(clm).cast(C_DEFAULT_DECIMAL_TYPE))
        else:
            self.logger.info(f"Market value column:{self.src_mv_clm}")
            C_CLM_VALIDATED_NME=f"{self.src_mv_clm}_validate"
            clm_validation_map[C_CLM_VALIDATED_NME] = [NotNullValidator(C_CLM_VALIDATED_NME, 'Supplied amount is not matched with excepted decimal format Decimal(26 10): ')]
            src_df = src_df.withColumn(C_CLM_VALIDATED_NME, sf.col(self.src_mv_clm).cast(C_DEFAULT_DECIMAL_TYPE))

        src_df = self._call_validator(src_df, clm_validation_map,exp_loc)

        src_df.cache()
        save_output_to_s3(src_df.select(*self.stg_clms),"src", output_stg_loc)

        return src_df

    def _call_validator(self,src_df, clm_validation_map,exp_loc):
        self.logger.info("called based validation logic")
        src_df = validate_rec_and_filter(src_df, clm_validation_map,exp_loc,prepare_exception_clm_list(self.stg_clms))
        return src_df

    def _read_src_and_stage(self):
        raise Exception("Implementation class need to override this....")



    """
        this can not be empty, as there are columns which have different value based on Asset,
         Liabilities and Autmated or manual, its esay to take it as default value, rather than putting condition
    """
    def _get_default_column_mapping(self):

        raise Exception("Implementation class need to override this....")