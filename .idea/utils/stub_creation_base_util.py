from constant.common_constant import *
from constant.property_constant import *
from pyspark.sql import functions as sf
from pyspark.sql.types import *
from constant.common_constant import *
from utils.JobContext import JobContext
from awsglue.dynamicframe import DynamicFrame
from utils.common_util import *
from utils.data_validations_util import *
from utils.s3_operations_util import *

###############################
# This script is for Insturment stub creation related Base classes
# Base classes were moved out to a separate script to avoid cyclic dependency issue
###############################

class SourceDataProviderOutput:
    src_df = None
    client_nbr = None

    def __init__(self, p_src_df, p_client_nbr):
        self.src_df = p_src_df
        self.client_nbr = p_client_nbr


class BaseSrcStubTransformer(object):

    ###############################
    #  This method will supply default map by child classes, which base will use to rename the column to make it generic
    ###############################
    def _get_source_to_generic_clm_map(self):
        raise Exception(C_OPERATION_NOT_SUPPORTED)

    ###############################
    # This  Main funtion of class, it does following things
    # 1- Save the validated output if staging output location is supplied
    # 2- Perform common and specifi transformation (For more details pleae refere respectiive method level comments)
    ###############################
    def transform(self, p_src_df):
        JobContext.logger.info("Updating Data column to generic name Base with Pojo input as: {} ".format(str(p_src_df)))

        p_src_df = self.common_transformation(p_src_df)
        p_src_df = self._specific_transformation(p_src_df)

        clms = self._get_required_clm_for_stub()
        JobContext.logger.info("Updated Data column to generic name Base")
        return p_src_df.select(*clms).distinct()

    ###############################
    #  This method will use supplied map and use withColumnRenamed to change the column name to generic name
    ###############################
    def common_transformation(self, p_src_df):
        JobContext.logger.info("Performing generic transaformation")
        src_to_gen_clmmap = self._get_source_to_generic_clm_map()
        for l_clm, generic_name in src_to_gen_clmmap.items():
            p_src_df = p_src_df.withColumnRenamed(l_clm, generic_name)

        JobContext.logger.info("Generic transaformation completed")
        return p_src_df

    ###############################
    #  Base class will not perform any specific transformation,
    #  this fucntion should be override by child class and perform specific transformation, like adding new column or concateting two Columns
    ###############################
    def _specific_transformation(self, p_src_df):
        return p_src_df

###############################
#  This Base class for proivder class
###############################
class BaseSourceDataProvider(object):

    ###############################
    #  Main method of this class, this will perform following activity (For more detail about activity please refer respective method level comments)
    #  3 - Transforme it into generic column name
    ###############################
    def provide(self, p_src_df):
        l_transformed_df = self._transform_to_general_df(p_src_df)
        l_src_data_pro_output = self._get_initialized_obj()
        l_src_data_pro_output.src_df = l_transformed_df
        return l_src_data_pro_output

    ###############################
    #   This will return SourceDataProviderOutput, child class will instantiate it and add specific data in that
    ###############################
    def _get_initialized_obj(self):
        raise Exception(C_OPERATION_NOT_SUPPORTED)


    def _transform_to_general_df(self, p_src_df):
        # if len(p_src_data_pro_input.stg_output_loc) != 0:
        #     JobContext.logger.info("Saving Data ...{}".format(p_src_data_pro_input.stg_output_loc))
        #     self._save_validated_rec(p_src_df, p_src_data_pro_input, self._get_stg_file_clm_list())
        # else:
        #     JobContext.logger.info("Source file does not staged as output location in not supplied")
        return p_src_df

    def get_inst_ref_type_cd_list_for_filter(self):
        raise Exception("Implementation class need to override this....")


class BaseInstrumentStubDataUpdater(object):

    def update(self, df, for_dtl_instrument  = True):
        return df


class InstrumentStubAuxiliaryRecordCreator(object):
    ###############################
    # Constant Declaration
    ###############################
    C_FACT_CLASS_ENTITY_COL_LIST = ['entity_id','class_type_id','entity_type_cd','overriding_entity_type_cd',
                                    'overriding_entity_id','effective_from_dt','effective_to_dt','allocation_prcnt',
                                    'firm_id','active_flg','batch_dt','created_program_nm','created_user_nm','created_ts']
    C_FACT_CLASS_ENTITY_DEFAULT_COL_LIST= {'entity_type_cd':'SECURITY','overriding_entity_type_cd':None,
                                           'allocation_prcnt':None,'overriding_entity_id':None,'firm_id':1}
    C_DIM_CLASS_TYPE_REQUIRED_COL_LIST = ['child_class_type_cd','child_class_val','entity_type_cd','class_type_id',
                                          'active_flg']
    C_FACT_CLASS_ENTITY_FIELD_TO_VALIDATE_AGAINST = {'entity_id': C_NOT_NULL_WITH_EMPTY_VALIDATOR_LIST,
                                                     'class_type_id': C_NOT_NULL_WITH_EMPTY_VALIDATOR_LIST,
                                                     'entity_type_cd': C_NOT_NULL_WITH_EMPTY_VALIDATOR_LIST}

    def _get_class_type_details(self, p_dct_df):
        l_df = p_dct_df.select('class_type_id') \
            .where((((sf.col('child_class_type_cd')=='SEC_TYPE') & (sf.col('child_class_val')=='Equity') & (sf.col('entity_type_cd')=='SECURITY'))
                    |((sf.col('child_class_type_cd')=='SEC_CLASS') & (sf.col('child_class_val')=='Other') & (sf.col('entity_type_cd')=='SECURITY'))
                    |((sf.col('child_class_type_cd')=='SEC_ASSET_CLASS') & (sf.col('child_class_val')=='Other') & (sf.col('entity_type_cd')=='SECURITY'))
                    |((sf.col('child_class_type_cd')=='SEC_ASSET_CATEGORY') & (sf.col('child_class_val')=='Other') & (sf.col('entity_type_cd')=='SECURITY'))
                    |((sf.col('child_class_type_cd')=='SEC_MKT_GROUP') & (sf.col('child_class_val')=='Other') & (sf.col('entity_type_cd')=='SECURITY')))
                   & (sf.col('active_flg')))
        return l_df

    def _check_if_record_exists(self, p_input_df):
        return p_input_df

    def _select_source(self, p_input_df, p_src_df):
        return p_input_df

    def insert_rec(self, p_input_df, p_batch_date, p_src_df):
        JobContext.logger.info("Record insertion for Fact class entity - Starts")
        p_input_df = self._select_source(p_input_df, p_src_df)
        C_DB_NAME = JobContext.get_property(C_GLUE_S3_DB_NAME_PROP_KEY)
        C_DIM_CLASS_TYPE_TBL = JobContext.get_property(C_GLUE_S3_TABLE_DIM_CLASS_TYPE)
        C_FACT_CLASS_ENTITY_S3_LOCATION_NEW_REC_INVAILD = JobContext.get_property(C_S3_FACT_CLASS_ENTITY_EXCP_OUTPUT_LOCATION)
        ###############################
        # For Fact_class_entity table
        ###############################
        l_dim_class_type_df = read_from_s3_catalog_convert_to_df(C_DB_NAME,C_DIM_CLASS_TYPE_TBL,
                                                                 self.C_DIM_CLASS_TYPE_REQUIRED_COL_LIST,None)

        l_dim_class_type_df = transform_boolean_clm(l_dim_class_type_df, "active_flg")

        ###############################
        # Deriving records based on class_type_id values
        ###############################
        l_dim_class_type_df = self._get_class_type_details(l_dim_class_type_df)
        p_input_df = self._join_src_with_dim_class_type(p_input_df,l_dim_class_type_df)


        p_input_df = self._check_if_record_exists(p_input_df)
        p_input_df = p_input_df.withColumn('effective_from_dt',sf.to_date(sf.lit(C_DEFAULT_START_DATE))) \
            .withColumn('effective_to_dt',sf.to_date(sf.lit(C_DEFAULT_END_DATE)))
        ###############################
        # Adding Default column
        ###############################
        p_input_df = add_default_clm(p_input_df,self.C_FACT_CLASS_ENTITY_DEFAULT_COL_LIST)
        ###############################
        # Adding Audit column
        ###############################
        p_input_df = add_audit_columns(p_input_df,p_batch_date,add_modification_clm=False)
        p_input_df = self._update_clm(p_input_df)
        p_input_df = p_input_df.select(*self.C_FACT_CLASS_ENTITY_COL_LIST)

        ###############################
        # Validating records
        ###############################
        p_input_df = validate_rec_and_filter(p_input_df,
                                             self.C_FACT_CLASS_ENTITY_FIELD_TO_VALIDATE_AGAINST,
                                             C_FACT_CLASS_ENTITY_S3_LOCATION_NEW_REC_INVAILD)
        ###############################
        # Saving output to s3
        ###############################
        save_output_to_s3(p_input_df,"fact_class_entity",JobContext.get_property(C_S3_FACT_CLASS_ENTITY_OUTPUT_LOCATION))
        JobContext.logger.info("Record insertion for Fact class entity - Ends")

    def _join_src_with_dim_class_type(self, p_input_df, l_dim_class_type_df):
        JobContext.logger.info("Using cross join")
        p_input_df = p_input_df.crossJoin(sf.broadcast(l_dim_class_type_df))
        return p_input_df

    def _update_clm(self, p_input_df):
        return p_input_df

class InstrumentStubAuxiliaryRecordCreatorDummy(InstrumentStubAuxiliaryRecordCreator):

    def insert_rec(self, p_input_df, p_batch_date, p_src_df):
        pass