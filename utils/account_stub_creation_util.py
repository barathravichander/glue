import sys

import pyspark.sql.functions as sf

from constant.common_constant import *
from constant.property_constant import *
from utils.s3_operations_util import read_from_s3,save_output_to_s3
from utils.JobContext import *
from utils.common_util import add_audit_columns,add_default_clm
from utils.stub_creation_base_util import SourceDataProviderOutput
from utils.data_validations_util import validate_rec_and_filter,C_NOT_NULL_WITH_EMPTY_VALIDATOR_LIST,NotNullValidator


###############################
# Source provider base class
###############################
class BaseAccSrcDataProvider(object):

    def _get_initialized_obj(self):
        raise Exception(C_OPERATION_NOT_SUPPORTED)

    def _get_source_to_generic_clm_map(self):
        raise Exception(C_OPERATION_NOT_SUPPORTED)

    def _get_required_src_col_list(self):
        return ['*']

    def _common_transformation(self,p_src_df):
        p_src_df = p_src_df.select(*self._get_required_src_col_list()).distinct()

        for l_clm,l_new_clm in self._get_source_to_generic_clm_map().items():
            p_src_df = p_src_df.withColumnRenamed(l_clm,l_new_clm)
        return p_src_df

    def get_date_type_details(self, p_batch_date):
        return [("OPDT",p_batch_date)]

    def provideSrc(self,p_src_df):
        l_src_df = self._common_transformation(p_src_df)
        l_src_out = self._get_initialized_obj()
        l_src_out.src_df= l_src_df
        return l_src_out

    def get_acc_ref_nm_for_stub(self, p_src_df):
        p_src_df = p_src_df.withColumn("bps_acc_ref_nm", sf.col("sor_account_id"))
        p_src_df = p_src_df.withColumn("sor_account_id", sf.col("sor_account_id").substr(sf.lit(2), sf.length("sor_account_id") - 1))
        return p_src_df

    def get_acc_join_condition(self):
        return (sf.col('src.bps_acc_ref_nm') == sf.col('acc.account_ref_nm')) & (sf.col("account_ref_type_cd") == C_ACCOUNT_REF_TYPE_CD_BPS)

    def create_dtl_acc_ref_records(self,p_src_df):
        g_stack_exp = "stack(2,'{}',bps_acc_ref_nm,'{}',sor_account_id) as (account_ref_type_cd,account_ref_nm)" \
            .format(C_ACCOUNT_REF_TYPE_CD_BPS,C_ACCOUNT_REF_TYPE_CD_OW)
        p_src_df = p_src_df.selectExpr("account_id", g_stack_exp)
        return p_src_df


###############################
# Source provider class for Keytrans file
###############################
class KeytransAccSrcProviderData(BaseAccSrcDataProvider):
    def _get_initialized_obj(self):
        return SourceDataProviderOutput(None, C_KEY_TRANS_CLIENT_NUMBER)

    def _get_source_to_generic_clm_map(self):
        return {'cl_id':'sor_account_id'}

    def _get_required_src_col_list(self):
        return ['cl_id']

    def get_date_type_details(self, p_batch_date):
        return [("OPDT",p_batch_date),("BDPDT",p_batch_date)]

    def get_acc_ref_nm_for_stub(self, p_src_df):
        p_src_df = p_src_df.withColumn("bps_acc_ref_nm", sf.concat(sf.lit("5"),"sor_account_id"))
        return p_src_df

    def get_acc_join_condition(self):
        return (sf.col('src.sor_account_id') == sf.col('acc.account_ref_nm')) & (sf.col("account_ref_type_cd").isin(C_ACCOUNT_REF_TYPE_CD_OW, C_ACCOUNT_REF_TYPE_CD_BPS))

    def create_dtl_acc_ref_records(self,p_src_df):
        g_stack_exp = "stack(3,'{}',bps_acc_ref_nm,'{}',sor_account_id,'{}',sor_account_id) as (account_ref_type_cd,account_ref_nm)" \
            .format(C_ACCOUNT_REF_TYPE_CD_BPS,C_ACCOUNT_REF_TYPE_CD_OW,C_ACCOUNT_REF_TYPE_CD_KEYBANK)
        p_src_df = p_src_df.selectExpr("account_id", g_stack_exp)
        return p_src_df


###############################
# Source provider class for Keyrecon file
###############################
class KeyreconAccSrcProviderData(BaseAccSrcDataProvider):
    def _get_initialized_obj(self):
        return SourceDataProviderOutput(None, C_KEY_RECON_CLIENT_NUMBER)

    def _get_source_to_generic_clm_map(self):
        return {'cl_id':'sor_account_id'}

    def _get_required_src_col_list(self):
        return ['cl_id']

    def get_date_type_details(self, p_batch_date):
        return [("OPDT",p_batch_date),("BDPDT",p_batch_date)]

    def get_acc_ref_nm_for_stub(self, p_src_df):
        p_src_df = p_src_df.withColumn("bps_acc_ref_nm", sf.concat(sf.lit("5"),"sor_account_id"))
        return p_src_df

    def get_acc_join_condition(self):
        return (sf.col('src.sor_account_id') == sf.col('acc.account_ref_nm')) & (sf.col("account_ref_type_cd") == C_ACCOUNT_REF_TYPE_CD_KEYBANK)

    def create_dtl_acc_ref_records(self,p_src_df):
        g_stack_exp = "stack(3,'{}',bps_acc_ref_nm,'{}',sor_account_id,'{}',sor_account_id) as (account_ref_type_cd,account_ref_nm)" \
            .format(C_ACCOUNT_REF_TYPE_CD_BPS,C_ACCOUNT_REF_TYPE_CD_OW,C_ACCOUNT_REF_TYPE_CD_KEYBANK)
        p_src_df = p_src_df.selectExpr("account_id", g_stack_exp)
        return p_src_df


###############################
# Source provider class for 529plan file
###############################
class Five29PlanAccSrcProviderData(BaseAccSrcDataProvider):
    def _get_initialized_obj(self):
        return SourceDataProviderOutput(None, C_529_CLIENT_NUMBER)

    def _get_source_to_generic_clm_map(self):
        return {'account_nbr':'sor_account_id'}

    def _get_required_src_col_list(self):
        return ['account_nbr']

    def get_date_type_details(self, p_batch_date):
        return [("OPDT",p_batch_date),("BDPDT",p_batch_date)]

###############################
# Source provider class for Annuities file
###############################
class AnnuitiesAccSrcProviderData(BaseAccSrcDataProvider):
    def _get_initialized_obj(self):
        return SourceDataProviderOutput(None, C_ANNUITIES_CLIENT_NUMBER)

    def _get_source_to_generic_clm_map(self):
        return {'sor_account_id':'sor_account_id'}

    def _get_required_src_col_list(self):
        return ['sor_account_id']

    def get_date_type_details(self, p_batch_date):
        return [("OPDT",p_batch_date),("BDPDT",p_batch_date)]

###############################
# Source provider factory class which will be used in job to invoke corresponding process
###############################
class AccountStubSrcDataProviderFactory():
    file_type_to_impl_clas_ob_map = {"KEYTRANS": KeytransAccSrcProviderData(),
                                     "KEYRECON": KeyreconAccSrcProviderData(),
                                     "FIVE29_PLAN": Five29PlanAccSrcProviderData(),
                                     "ANNUITIES": AnnuitiesAccSrcProviderData()}

    @staticmethod
    def get(file_type):
        provider = AccountStubSrcDataProviderFactory.file_type_to_impl_clas_ob_map.get(file_type.upper())
        if provider is None:
            raise Exception("Passed file type is not supported".format(file_type))
        return provider

###############################
# Base class for Auxiliary table record insertion
###############################
class AccountStubAuxiliaryRecordCreator(object):

    ###############################
    # Constant declaration
    ###############################
    C_BRDG_ACC_RELATION_COL_LIST = ['account_id','entity_type_cd','entity_id','start_dt','end_dt','relation_type_cd',
                                    'target_alloc_prcnt','manual_override_flg','inc_reason_cd','inc_notes',
                                    'exc_reason_cd','exc_notes','firm_id','active_flg','batch_dt',
                                    'created_program_nm','created_user_nm','created_ts',
                                    'modified_program_nm','modified_user_nm','modified_ts']
    C_BRDG_ACC_RELATION_COL_LIST_TO_VALIDATE ={"account_id": C_NOT_NULL_WITH_EMPTY_VALIDATOR_LIST}
    C_BRDG_ACC_RELATION_DEFAULT_COL_LIST = {
        "end_dt": C_DEFAULT_END_DATE,
        "relation_type_cd": None,
        "target_alloc_prcnt": None,
        "manual_override_flg": None,
        "inc_reason_cd": None,
        "inc_notes": None,
        "exc_reason_cd": None,
        "exc_notes": None,
        "firm_id": C_FIRM_ID}

    def insert_rec(self,p_input_df,p_batch_date):
        JobContext.logger.info("Record Insertion process for brdg_account_relation table - Starts")
        C_BRDG_ACCOUNT_RELATION_OUTPUT_LOCATION = JobContext.get_property(C_S3_BRDG_ACC_RELATION_OUTPUT_LOCATION_PROP_KEY)
        C_BRDG_ACC_RELATION_OUTPUT_EXCP_LOCATION = JobContext.get_property(C_S3_BRDG_ACC_RELATION_EXCP_OUTPUT_LOCATION_PROP_KEY)

        l_entity_type_cd_df = JobContext.glue_context.spark_session.createDataFrame([("BU","1-Default-1"),
                                                                                     ("ADVISOR","1-Default-1"),
                                                                                     ("UBS_BPS","1-Default")],
                                                                                    ["entity_type_cd","entity_id"])
        p_input_df = p_input_df.crossJoin(sf.broadcast(l_entity_type_cd_df))


        ###############################
        # Adding Default column
        ###############################
        self.C_BRDG_ACC_RELATION_DEFAULT_COL_LIST["start_dt"] = p_batch_date
        p_input_df = add_default_clm(p_input_df, self.C_BRDG_ACC_RELATION_DEFAULT_COL_LIST)
        ###############################
        # Adding audit column
        ###############################
        p_input_df = add_audit_columns(p_input_df,p_batch_date,add_modification_clm=True)
        ###############################
        # Validating records
        ###############################
        p_input_df = self._update_clm(p_input_df)
        p_input_df = validate_rec_and_filter(p_input_df,
                                             self.C_BRDG_ACC_RELATION_COL_LIST_TO_VALIDATE,
                                             C_BRDG_ACC_RELATION_OUTPUT_EXCP_LOCATION)

        p_input_df = p_input_df.select(*self.C_BRDG_ACC_RELATION_COL_LIST)
        ###############################
        # Saving output to s3
        ###############################
        save_output_to_s3(p_input_df,"brdg_account_relation",C_BRDG_ACCOUNT_RELATION_OUTPUT_LOCATION)
        JobContext.logger.info("Record Insertion process for brdg_account_relation table - Ends")

    ###############################
    # This can be overriden by child, to implement source specific transformations
    ###############################
    def _update_clm(self, p_input_df):
        return p_input_df

###############################
# Class for Auxiliary table record insertion for keytrans file type, currenlty it does not have specific transformation, so not overriding any method
###############################
class KeytransAccountStubAuxiliaryRecordCreator(AccountStubAuxiliaryRecordCreator):
    pass


###############################
# Class for Auxiliary table record insertion for keyrecon file type
###############################
class KeyreconAccountStubAuxiliaryRecordCreator(AccountStubAuxiliaryRecordCreator):
    pass

###############################
# Class for Auxiliary table record insertion for keyrecon file type
###############################
class CommonAccountStubAuxiliaryRecordCreator(AccountStubAuxiliaryRecordCreator):
    pass

###############################
# Dummy Class for Auxiliary table record insertion if file type is not supported
###############################
class AccountStubAuxiliaryRecordCreatorDummy(AccountStubAuxiliaryRecordCreator):

    def insert_rec(self,p_input_df,p_batch_date):
        JobContext.logger.info("Method not supported for provided file type, hence skipping")

###############################
# Factory Class for Auxiliary table record insertion which will be used in job to invoke corresponding process
###############################
class AccountStubAuxiliaryRecordFactory():
    file_type_to_create_aux_tbl = {"KEYTRANS": KeytransAccountStubAuxiliaryRecordCreator(),
                                   "KEYRECON": KeyreconAccountStubAuxiliaryRecordCreator(),
                                   "FIVE29_PLAN": CommonAccountStubAuxiliaryRecordCreator(),
                                   "ANNUITIES": CommonAccountStubAuxiliaryRecordCreator()}

    @staticmethod
    def get(p_file_type):
        updater = AccountStubAuxiliaryRecordFactory.file_type_to_create_aux_tbl.get(p_file_type.upper())
        if updater is None:
            JobContext.logger.info("Passed File type {} not found using, skipping creating auxiliary table[s]".format(p_file_type))
            updater = AccountStubAuxiliaryRecordCreatorDummy()
        return updater
