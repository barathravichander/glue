from utils.common_util import  auto_str
from constant.common_constant import C_SOR_BEN_INSTR_REF_TYPE_CD
from utils.data_validations_util import *
from constant.property_constant import *
from utils.keytrans_stub_creation_util import KeytransInstrumentStubDataUpdater, KeytransSourceDataProvider, \
    KeyTransInstrumentStubAuxiliaryRecordCreator
from utils.stub_creation_base_util import BaseSrcStubTransformer, BaseSourceDataProvider, BaseInstrumentStubDataUpdater, \
    InstrumentStubAuxiliaryRecordCreator, SourceDataProviderOutput
from utils.keyrecon_stub_creation_util import KeyreconInstrumentStubDataUpdater, KeyreconSourceDataProvider, \
    KeyReconInstrumentStubAuxiliaryRecordCreator
from utils.dim_instrument_stub_creation_util import DimInstrumentStubDataUpdater, DimInstrumentSourceDataProvider, \
    DimInstrumentStubAuxiliaryRecordCreator
from utils.annuities_stub_creation_util import AnnuitiesInstrumentStubDataUpdater, AnnuitiesSourceDataProvider, \
    AnnuitiesInstStubAuxiliaryRecordCreator, AnnuitiesFundSubSourceDataProvider, AnnuitiesFundSubSourceDataUpdater, \
    AnnuitiesSubFundInstStubAuxiliaryRecordCreator
from utils.insu_position_stub_creation_util import InsuPositionInstrumentStubDataUpdater, InsuPositionSourceDataProvider, \
    InsuPositionStubAuxiliaryRecordCreator


################################################################
# Pojo Class to hold Input data
################################################################

@auto_str
class SourceDataProviderInput:
    input_location = ""
    batch_date = None
    run_validation = True
    g_glue_context = None
    src_exception_s3_output_loc = ""
    stg_output_loc = ""

    def __init__(self):
        l_file_type= JobContext.file_being_processed.lower()

        self.input_location = JobContext.get_property(C_GLUE_S3_INSTRUMENT_SRC_TABLE_PROP_KEY.format(l_file_type))

        # as this will be coming from job property as string to checking with 'true'
        self.run_validation = JobContext.get_property(C_RUN_VALIDATION_GL_PROP_KEY).lower() == "true"
        self.src_exception_s3_output_loc = JobContext.get_property(C_S3_STUB_INSTRUMENT_SRC_STG_EXCP_OUTPUT_LOC_PROP_KEY.format(l_file_type))
        self.stg_output_loc = JobContext.get_property(C_S3_STUB_INSTRUMENT_SRC_STG_OUTPUT_LOC_PROP_KEY.format(l_file_type))

        self.g_glue_context = JobContext.glue_context
        self.batch_date = JobContext.batch_date


def validate_not_null(p_attr, p_attr_name):
    if p_attr is None:
        raise Exception("Variable {} is None or not passed".format(p_attr_name))


###############################
#  This Child class for FACT_SET file
###############################
class FactSetSrcStubTransformer(BaseSrcStubTransformer):

    def _get_required_clm_for_stub(self):
        return ['src_instrument_ref_nm', 'factset_id', 'processing_dt', 'INDEX_DESCRIPTION']

    def _get_source_to_generic_clm_map(self):
        return {"ubs_id": 'src_instrument_ref_nm'}

    def _specific_transformation(self, p_src_df):
        JobContext.logger.info("Updating Data column to generic name")
        p_src_df = p_src_df.withColumn("processing_dt", sf.to_date(sf.lit(JobContext.batch_date), C_DATE_FORMAT))
        return p_src_df


class RimesSrcStubTransformer(BaseSrcStubTransformer):

    def _get_required_clm_for_stub(self):
        return ['src_instrument_ref_nm', 'Index reporting currency', 'processing_dt' ,'Long name','Description']

    def _get_source_to_generic_clm_map(self):
        return {"Benchmark code": 'src_instrument_ref_nm'}

    def _specific_transformation(self, p_src_df):
        JobContext.logger.info("Updating Data column to generic name")
        p_src_df = p_src_df.withColumn("processing_dt", sf.to_date(sf.lit(JobContext.batch_date), C_DATE_FORMAT))
        return p_src_df


class FactSetSourceDataProvider(BaseSourceDataProvider):

    def _get_initialized_obj(self):
        return SourceDataProviderOutput(None, C_FAC_SET_CLIENT_NUMBER)

    def _transform_to_general_df(self, p_src_df):

        l_fac_set_src_trans = FactSetSrcStubTransformer()
        p_src_df = l_fac_set_src_trans.transform(p_src_df)
        return p_src_df

    def get_inst_ref_type_cd_list_for_filter(self):
        return [sf.lit(C_SOR_BEN_INSTR_REF_TYPE_CD),sf.lit('FSET_BEN')]



class RimesSourceDataProvider(BaseSourceDataProvider):

    def _get_initialized_obj(self):
        return SourceDataProviderOutput(None, C_RIMES_CLIENT_NUMBER)


    def _transform_to_general_df(self, p_src_df):
        p_src_df = super(RimesSourceDataProvider, self)._transform_to_general_df(p_src_df)
        l_fac_set_src_trans = RimesSrcStubTransformer()
        p_src_df = l_fac_set_src_trans.transform(p_src_df)
        return p_src_df

    def get_inst_ref_type_cd_list_for_filter(self):
        return [sf.lit('RIMES_BEN')]


# Factory class to get the Dataframe Provider Object for respective file type
class InstrumentStubSrcDataProviderFactory():
    file_type_to_impl_clas_ob_map = {"FACT_SET": FactSetSourceDataProvider(),
                                     "RIMES": RimesSourceDataProvider(),
                                     "KEYTRANS": KeytransSourceDataProvider(),
                                     "KEYRECON": KeyreconSourceDataProvider(),
                                     "H2B": DimInstrumentSourceDataProvider(),
                                     "ANNUITIES": AnnuitiesSourceDataProvider(),
                                     "ANNUITIES_SUB_FUND": AnnuitiesFundSubSourceDataProvider(),
                                     "INSU_POSITION":InsuPositionSourceDataProvider()}
    @staticmethod
    def get(file_type):
        provider = InstrumentStubSrcDataProviderFactory.file_type_to_impl_clas_ob_map.get(file_type.upper())
        if provider is None:
            raise Exception("Passed file type is not supported".format(file_type))
        return provider


class FactSetInstrumentStubDataUpdater(BaseInstrumentStubDataUpdater):

    def update(self, p_input_df, for_dtl_instrument = True):
        p_input_df = p_input_df.withColumn('instrument_type_cd',sf.lit('BEN'))
        p_input_df.cache()
        df_with_primary_key =p_input_df.drop("factset_id").withColumn("instrument_ref_type_cd", sf.lit(C_SOR_BEN_INSTR_REF_TYPE_CD))

        if for_dtl_instrument:
            df_with_alternet_key = p_input_df.drop("src_instrument_ref_nm").withColumnRenamed("factset_id", "src_instrument_ref_nm") \
                .withColumn("instrument_ref_type_cd",   sf.lit("FSET_BEN"))
            df = df_with_alternet_key.union(df_with_primary_key)
        else:
            df = df_with_primary_key
        df = df.withColumn('instrument_desc', sf.col('INDEX_DESCRIPTION')) \
            .withColumn('instrument_name_1', sf.col('INDEX_DESCRIPTION'))
        return df


class RimesInstrumentStubDataUpdater(BaseInstrumentStubDataUpdater):

    def update(self, p_input_df,for_dtl_instrument = True ):
        p_input_df = p_input_df.drop('currency_cd').withColumnRenamed("Index reporting currency", "currency_cd") \
            .withColumn("instrument_ref_type_cd", sf.lit('RIMES_BEN')) \
            .withColumn('instrument_type_cd',sf.lit('BEN')) \
            .withColumn('instrument_name_1', sf.col('Description')) \
            .withColumn('instrument_desc', sf.col('Long name'))
        return p_input_df


# Factory class to get the Dataframe Provider Object for respective file type
class InstrumentStubDataUpdaterFactory():
    file_type_to_impl_clas_ob_map = {"FACT_SET": FactSetInstrumentStubDataUpdater(),
                                     "RIMES": RimesInstrumentStubDataUpdater(),
                                     "KEYTRANS": KeytransInstrumentStubDataUpdater(),
                                     "KEYRECON": KeyreconInstrumentStubDataUpdater(),
                                     "H2B": DimInstrumentStubDataUpdater(),
                                     "ANNUITIES": AnnuitiesInstrumentStubDataUpdater(),
                                     "ANNUITIES_SUB_FUND":AnnuitiesFundSubSourceDataUpdater(),
                                     "INSU_POSITION": InsuPositionInstrumentStubDataUpdater()}

    @staticmethod
    def get(p_file_type):
        updater = InstrumentStubDataUpdaterFactory.file_type_to_impl_clas_ob_map.get(p_file_type.upper())
        if updater is None:
            JobContext.logger.info("Passed File type {} not found using, skipping  updating".format(p_file_type))
            updater = BaseInstrumentStubDataUpdater()
        return updater


class InstrumentStubAuxiliaryRecordCreatorDummy(InstrumentStubAuxiliaryRecordCreator):

    def insert_rec(self, p_input_df, p_batch_date, p_src_df):
        pass


class InstrumentStubAuxiliaryRecordFactory():
    file_type_to_create_aux_tbl = {"KEYTRANS": KeyTransInstrumentStubAuxiliaryRecordCreator(),
                                   "KEYRECON": KeyReconInstrumentStubAuxiliaryRecordCreator(),
                                   "H2B": DimInstrumentStubAuxiliaryRecordCreator(),
                                   "ANNUITIES": AnnuitiesInstStubAuxiliaryRecordCreator(),
                                   "ANNUITIES_SUB_FUND": AnnuitiesSubFundInstStubAuxiliaryRecordCreator(),
                                   "INSU_POSITION": InsuPositionStubAuxiliaryRecordCreator()}

    @staticmethod
    def get(p_file_type):
        updater = InstrumentStubAuxiliaryRecordFactory.file_type_to_create_aux_tbl.get(p_file_type.upper())
        if updater is None:
            JobContext.logger.info("Passed File type {} not found using, skipping creating auxiliary table[s]".format(p_file_type))
            updater = InstrumentStubAuxiliaryRecordCreatorDummy()
        return updater