import sys

import pyspark.sql.functions as sf
from pyspark.sql.types import *

from constant.property_constant import *
from utils.JobContext import *
from utils.common_util import *
from utils.data_validations_util import *
from utils.s3_operations_util import *
from constant.common_constant import *
from utils.stub_creation_base_util import BaseSourceDataProvider, BaseSrcStubTransformer, SourceDataProviderOutput, \
    InstrumentStubAuxiliaryRecordCreator


###############################
#  Implementation class of keytrans source file for Instrument Stub creation logic
###############################
class KeyreconSourceDataProvider(BaseSourceDataProvider):

    def _get_initialized_obj(self):
        return SourceDataProviderOutput(None, C_KEY_RECON_CLIENT_NUMBER)

    def provide(self, p_src_df):
        g_keybank_symbol_df = p_src_df.select("symbol").distinct()

        g_transformer=KeyreconSrcStubTransformer()
        g_keybank_symbol_df = g_transformer.transform(g_keybank_symbol_df)
        l_src_data_pro_output = self._get_initialized_obj()
        l_src_data_pro_output.src_df = g_keybank_symbol_df
        return l_src_data_pro_output

    def get_inst_ref_type_cd_list_for_filter(self):
        return [sf.lit('CU')]


class KeyreconSrcStubTransformer(BaseSrcStubTransformer):

    def _get_source_to_generic_clm_map(self):
        return {"symbol": 'src_instrument_ref_nm'}

    def _get_required_clm_for_stub(self):
        return ['src_instrument_ref_nm', 'processing_dt']

    def transform(self, p_src_df):
        p_src_df = self.common_transformation(p_src_df)
        p_src_df = self._specific_transformation(p_src_df)

        clms = self._get_required_clm_for_stub()
        JobContext.logger.info("Updated Data column to generic name Base")
        return p_src_df.select(*clms).distinct()

    def _specific_transformation(self, p_src_df):
        JobContext.logger.info("Updating Data column to generic name")
        p_src_df = p_src_df.withColumn("processing_dt", sf.to_date(sf.lit(JobContext.batch_date), C_DATE_FORMAT))
        return p_src_df


class KeyreconInstrumentStubDataUpdater(object):

    def update(self, p_input_df,  for_dtl_instrument  = True):
        p_input_df = p_input_df.withColumn("instrument_type_cd", sf.lit('SEC')) \
            .withColumn("instrument_ref_type_cd", sf.lit('CU'))
        return p_input_df


class KeyReconInstrumentStubAuxiliaryRecordCreator(InstrumentStubAuxiliaryRecordCreator):

    def _update_clm(self, p_input_df):
        p_input_df = p_input_df.withColumnRenamed('instrument_id', 'entity_id')
        return p_input_df