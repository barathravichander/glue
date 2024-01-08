import sys

from utils.common_derivations_util import derive_instrument_id
from utils.s3_operations_util import *
from constant.common_constant import *
from constant.property_constant import *
from utils.stub_creation_base_util import BaseSourceDataProvider, BaseSrcStubTransformer, SourceDataProviderOutput, \
    InstrumentStubAuxiliaryRecordCreator


class InsuPositionSourceDataProvider(BaseSourceDataProvider):

    def _get_initialized_obj(self):
        return SourceDataProviderOutput(None, C_ANNUITIES_CLIENT_NUMBER)


    def _transform_to_general_df(self, p_src_df):
        p_src_df = super(InsuPositionSourceDataProvider, self)._transform_to_general_df(p_src_df)
        l_fac_set_src_trans = InsuPositionSrcStubTransformer()
        p_src_df = l_fac_set_src_trans.transform(p_src_df)
        return p_src_df

    def get_inst_ref_type_cd_list_for_filter(self):
        return [sf.lit(C_INSTRUMENT_REF_TYPE_CD_INSU)]


class InsuPositionSrcStubTransformer(BaseSrcStubTransformer):

    def _get_required_clm_for_stub(self):
        return ['src_instrument_ref_nm',"processing_dt"]

    def _get_source_to_generic_clm_map(self):
        return {"insu_ref_nm": 'src_instrument_ref_nm'}

    def _specific_transformation(self, p_src_df):
        JobContext.logger.info("Updating Data column to generic name")
        p_src_df = p_src_df.withColumn("processing_dt", sf.to_date(sf.lit(JobContext.batch_date), C_DATE_FORMAT))
        return p_src_df


class InsuPositionInstrumentStubDataUpdater(object):

    def update(self, p_input_df,  for_dtl_instrument  = False):
        p_input_df = p_input_df.withColumn("instrument_type_cd", sf.lit(C_DI_INSTRUMENT_TYPE_CD_SEC)) \
            .withColumn("instrument_ref_type_cd", sf.lit(C_INSTRUMENT_REF_TYPE_CD_INSU))
        return p_input_df


class InsuPositionStubAuxiliaryRecordCreator(InstrumentStubAuxiliaryRecordCreator):

    def _get_class_type_details(self, p_dct_df):
        l_df = p_dct_df.select('class_type_id') \
            .where((sf.col('child_class_type_cd') == C_DCT_CHILD_CLASS_TYPE_CD_SEC_TYPE)
                   & (sf.col('child_class_val') == C_DCT_CHILD_CLASS_VAL_IA)
                   & (sf.col('entity_type_cd') == C_DCT_ENTITY_TYPE_CD_SECURITY) & (sf.col('active_flg')))
        return l_df

    def _select_source(self, p_input_df, p_src_df):
        return p_src_df

    def _check_if_record_exists(self, p_src_df):
        JobContext.logger.info("Checking if record exists in Fact class entity - Starts")
        l_class_type_list = p_src_df.select(sf.collect_set("class_type_id")).collect()[0][0]
        JobContext.logger.info(f"Checking if record exists in Fact class entity l_class_type_list - {l_class_type_list}")
        l_fce_df = read_from_catalog_and_filter_inactive_rec(JobContext.get_property(C_S3_FACT_CLASS_ENTITY_TBL_NME),
                                                             ['entity_id','class_type_id','entity_type_cd','active_flg'])
        l_fce_df = l_fce_df.filter(sf.col("entity_type_cd") == C_FCE_ENTITY_TYPE_CD_SEC) \
            .filter(sf.col("class_type_id").isin(l_class_type_list)).select('entity_id').distinct()

        l_dir_df = read_from_s3_catalog_convert_to_df(JobContext.get_property(C_GLUE_S3_DB_NAME_PROP_KEY),
                                                      JobContext.get_property(C_GLUE_S3_TABLE_DTL_INSTRUMENT_REF_PROP_KEY))

        l_dir_df = transform_boolean_clm(l_dir_df,"active_flg")

        l_filter_cond = [sf.lit(C_INSTRUMENT_REF_TYPE_CD_INSU)]
        l_join_cond = sf.col("src_instrument_ref_nm") == sf.col("instrument_ref_nm")
        p_src_df = derive_instrument_id(p_src_df,l_dir_df,l_join_cond,None,l_filter_cond,"processing_dt")

        p_src_df = p_src_df.join(sf.broadcast(l_fce_df),p_src_df["instrument_id"]==l_fce_df["entity_id"],"left") \
            .select(p_src_df["*"],l_fce_df["entity_id"]).where(sf.col("entity_id").isNull()).drop("entity_id","processing_dt")
        JobContext.logger.info("Checking if record exists in Fact class entity - Ends")
        return p_src_df

    def _update_clm(self, p_input_df):
        p_input_df = p_input_df.withColumnRenamed('instrument_id','entity_id')
        return p_input_df
