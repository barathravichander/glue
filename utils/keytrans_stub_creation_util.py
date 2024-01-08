import sys

from utils.s3_operations_util import *
from constant.common_constant import *
from utils.stub_creation_base_util import BaseSourceDataProvider, BaseSrcStubTransformer, SourceDataProviderOutput, \
    InstrumentStubAuxiliaryRecordCreator


###############################
#  Implementation class of keytrans source file for Instrument Stub creation logic
###############################
class KeytransSourceDataProvider(BaseSourceDataProvider):

    def _get_initialized_obj(self):
        return SourceDataProviderOutput(None, C_KEY_TRANS_CLIENT_NUMBER)

    def _transform_to_general_df(self, p_src_df):

        l_fac_set_src_trans = KeytransSrcStubTransformer()
        p_src_df = l_fac_set_src_trans.transform(p_src_df)
        return p_src_df

    def get_inst_ref_type_cd_list_for_filter(self):
        return [sf.lit('CU')]


class KeytransSrcStubTransformer(BaseSrcStubTransformer):

    def _get_source_to_generic_clm_map(self):
        return {"symbol": 'src_instrument_ref_nm'}

    def _get_required_clm_for_stub(self):
        return ['src_instrument_ref_nm', 'processing_dt']

    def _specific_transformation(self, p_src_df):
        JobContext.logger.info("Updating Data column to generic name")
        p_src_df = p_src_df.withColumn("processing_dt", sf.to_date(sf.lit(JobContext.batch_date), C_DATE_FORMAT))
        return p_src_df

class KeytransInstrumentStubDataUpdater(object):

    def update(self, p_input_df,  for_dtl_instrument  = False):
        p_input_df = p_input_df.withColumn("instrument_type_cd", sf.lit('SEC'))\
                               .withColumn("instrument_ref_type_cd", sf.lit('CU'))
        return p_input_df


class KeyTransInstrumentStubAuxiliaryRecordCreator(InstrumentStubAuxiliaryRecordCreator):

    def _update_clm(self, p_input_df):
        p_input_df = p_input_df.withColumnRenamed('instrument_id','entity_id')
        return p_input_df