from constant.common_constant import C_DEFAULT_DECIMAL_TYPE, C_EMPTY_FILE_LOG_MSG
from constant.property_constant import C_GLUE_S3_TABLE_DIM_SOR_CAPS_CLASS_TYPE_REF
from pyspark.sql import functions as sf
from utils.JobContext import JobContext
from pyspark.sql.window import Window
from utils.common_derivations_util import derive_class_type_id_from_dim_cls_typ_and_src, \
    C_DEFAULT_DIM_CLASS_TYPE_ALIAS_NAME, derive_entity_id_from_acc_grp_id_and_src, C_DEFAULT_DIM_ACCOUNT_GRP_ALIAS_NAME
from utils.common_util import when_condition_prepare, create_empty_dataframe, trim_space, prepare_exception_clm_list
from utils.s3_operations_util import read_from_catalog_and_filter_inactive_rec

from utils.s3_operations_util import read_from_s3, save_output_to_s3
from utils.data_validations_util import *

C_DATE_FORMAT = 'MM/dd/yyyy'
C_NOT_NULL_WITH_EMPTY_DATE_FORMAT_VALIDATOR_LIST = [NotNullValidator(), EqualValidator(''), DateFormatValidator(C_DATE_FORMAT)]

C_PMP_COMP_PERF_CLMS = ['cl_id', 'his_type', 'his_class', 'his_code', 'his_date', 'value1', 'value2', 'frag_id',
                        'filler1']

C_PMP_COMP_PERF_ROR_CLMS = ['cl_id', 'his_type', 'his_class', 'his_code', 'his_date', 'value1', 'value2']

C_PERF_COMP_ROR_CLM_NMS = ['cl_id', 'ror_date', 'ror_class', 'net_fees_ror', 'gross_fees_ror', 'net_fees_u_value',
                           'gross_fees_u_value', 'filler1', 'frag_id', 'ror_type']



C_PERF_COMP_ROR_REQ_CLM_NMS = ['cl_id', 'ror_date', 'ror_class', 'net_fees_ror', 'gross_fees_ror']
C_PERF_COMP_ROR_VALIDATION_MAP = {"cl_id": C_NOT_NULL_WITH_EMPTY_VALIDATOR_LIST,
                                  "ror_date": C_NOT_NULL_WITH_EMPTY_DATE_FORMAT_VALIDATOR_LIST,
                                  "ror_class":C_NOT_NULL_WITH_EMPTY_VALIDATOR_LIST,
                                  }

C_SRC_CLSS_TYPE_TO_DIM_CLASS_TYPE_MAP={'EQUITIES': 'EQUITY', 'FIXED': 'FIXED INCOME'}

C_PERF_COMP_ROR_VALIDATION_MAP = {"cl_id": C_NOT_NULL_WITH_EMPTY_VALIDATOR_LIST,
                                  "ror_date": C_NOT_NULL_WITH_EMPTY_DATE_FORMAT_VALIDATOR_LIST,
                                  "ror_class":C_NOT_NULL_WITH_EMPTY_VALIDATOR_LIST,
                                  }



C_PMP_COMP_PERF_ROR_VALIDATION_MAP = {"cl_id": C_NOT_NULL_WITH_EMPTY_VALIDATOR_LIST,
                                      "his_type": C_NOT_NULL_WITH_EMPTY_VALIDATOR_LIST,
                                      "his_class":C_NOT_NULL_WITH_EMPTY_VALIDATOR_LIST,
                                      "his_date": C_NOT_NULL_WITH_EMPTY_DATE_FORMAT_VALIDATOR_LIST
                                      }

C_PMP_COMP_PERF_ROR_VALIDATION_MAP = {"cl_id": C_NOT_NULL_WITH_EMPTY_VALIDATOR_LIST,
                                      "his_type": C_NOT_NULL_WITH_EMPTY_VALIDATOR_LIST,
                                      "his_class":C_NOT_NULL_WITH_EMPTY_VALIDATOR_LIST,
                                      "his_date": C_NOT_NULL_WITH_EMPTY_DATE_FORMAT_VALIDATOR_LIST
                                      }

C_FACT_ROR_HIST_CLMS_NAME = ['account_id', 'sleeve_id', 'instrument_id', 'class_type_id', 'entity_type_cd',
                             'entity_id', 'ror_freq', 'period_start_dt', 'period_end_dt', 'currency_cd', 'adv_flg',
                             'beg_mv_val', 'end_mv_val', 'beg_ai_val', 'end_ai_val', 'cf_in_val', 'cf_out_val',
                             'income_amt', 'net_wtd_cf_val', 'gross_wtd_cf_val', 'fee_tot_val', 'gof_ror_val',
                             'nof_ror_val', 'batch_dt', 'active_flg', 'created_program_nm', 'created_user_nm',
                             'created_ts']



def pivot_perf_amt_value(clm_map_fill_with_zero, clm_to_tgt_name_map, his_code_clm_values,
                         pivot_grp_clm, pmp_comp_perf_df):
    JobContext.logger.info("Pivoting pmp_comp_perf records")
    pmp_comp_perf_df = pmp_comp_perf_df.withColumn("value1", sf.col("value1").cast(C_DEFAULT_DECIMAL_TYPE)) \
        .withColumn("value2", sf.col("value2").cast(C_DEFAULT_DECIMAL_TYPE))
    pmp_comp_perf_df = pmp_comp_perf_df.groupBy(pivot_grp_clm) \
        .pivot("his_type", his_code_clm_values) \
        .agg(sf.sum("value1"), sf.sum("value2"))

    for key, value in clm_to_tgt_name_map.items():
        pmp_comp_perf_df = pmp_comp_perf_df.withColumnRenamed(key, value)

    # Set values to default, incase if its null so that addition or negation should not be set to null
    pmp_comp_perf_df = pmp_comp_perf_df.na.fill(clm_map_fill_with_zero)
    JobContext.logger.info("Pivoted pmp_comp_perf records")
    return pmp_comp_perf_df



def derive_period_end_date( perf_comp_ror_df):
    JobContext.logger.info("Derive period end date")
    window = Window. \
        partitionBy('cl_id', 'ror_class'). \
        orderBy(sf.col('period_start_dt').asc())

    # Adding period_end_dt using next record's ror_date/period_start_dt, adding record_seq to duplicate first new records of each window

    perf_comp_ror_df = perf_comp_ror_df.withColumn("period_end_dt", sf.lead('period_start_dt').over(window)) \
        .withColumn("o_gross_fees_ror", sf.lead('gross_fees_ror').over(window)) \
        .withColumn("o_net_fees_ror", sf.lead('net_fees_ror').over(window)) \
        .withColumn('record_seq', sf.row_number().over(window))


    # dummy DF which will help to duplicate first records, it has rec_type_indicator, which will identify one record after duplicate, to set its period_end_date
    duplicate_dummy_df = JobContext.glue_context.spark_session.createDataFrame([(1, 'original'), (1, 'duplicate')],
                                                                               ['record_seq', 'rec_type_indicator'])

    perf_comp_ror_df = perf_comp_ror_df.join(duplicate_dummy_df,
                                             duplicate_dummy_df.record_seq == perf_comp_ror_df.record_seq, 'left') \
        .select(perf_comp_ror_df['*'], duplicate_dummy_df.rec_type_indicator)

    # chance the period_end_dt for one of duplicated record
    perf_comp_ror_df = perf_comp_ror_df.withColumn('period_end_dt',
                                                   sf.when(sf.col('rec_type_indicator') == 'duplicate',
                                                           sf.col('period_start_dt')).otherwise(sf.col('period_end_dt')))
    perf_comp_ror_df = perf_comp_ror_df.withColumn('gross_fees_ror',
                                                   sf.when(sf.col('rec_type_indicator') == 'duplicate',
                                                           sf.col('gross_fees_ror')).otherwise(sf.col('o_gross_fees_ror')))
    perf_comp_ror_df = perf_comp_ror_df.withColumn('net_fees_ror',
                                                   sf.when(sf.col('rec_type_indicator') == 'duplicate',
                                                           sf.col('net_fees_ror')).otherwise(sf.col('o_net_fees_ror')))

    # As ror_dt is mandatory field, so only last record will have null value, which we need to filter
    perf_comp_ror_df = perf_comp_ror_df.filter(sf.col('period_end_dt').isNotNull())
    JobContext.logger.info("Derived period end date")
    return perf_comp_ror_df



def transform_ror_class_and_start_dt(perf_comp_ror_df):

    JobContext.logger.info("Transforming Look on dim_sor_class_type to validate ror_class")
    ror_clas_clm_trans_map = C_SRC_CLSS_TYPE_TO_DIM_CLASS_TYPE_MAP.copy()
    ror_clas_clm_trans_map[None]= sf.col('ror_class')
    perf_comp_ror_df = perf_comp_ror_df.withColumn('ror_class', sf.upper(sf.col('ror_class'))) \
        .withColumn('period_start_dt', sf.to_date(sf.col('ror_date'), C_DATE_FORMAT)) \
        .withColumn('ror_class', when_condition_prepare(ror_clas_clm_trans_map, 'ror_class'))

    JobContext.logger.info("Transforming Look on dim_sor_class_type to validate ror_class")
    return perf_comp_ror_df


def derive_class_type_id( perf_comp_ror_df, child_class_type_cd_val ='CAPS_SEC_BRD_ASSET_CLS'):

    # as we will use alias to join, creating column FQDN(table_name.column_name) with alias
    dim_child_class_val_clm_nm = "{dim_class_tbl_name}.child_class_val".format(dim_class_tbl_name=C_DEFAULT_DIM_CLASS_TYPE_ALIAS_NAME)
    child_class_type_cd_clm_nm = "{dim_class_tbl_name}.child_class_type_cd".format(dim_class_tbl_name=C_DEFAULT_DIM_CLASS_TYPE_ALIAS_NAME)

    dim_cls_typ_join_cond = [(sf.col('src.ror_class') == sf.col(dim_child_class_val_clm_nm)) & (sf.col(child_class_type_cd_clm_nm) == child_class_type_cd_val)]
    perf_comp_ror_df = perf_comp_ror_df.alias('src')
    perf_comp_ror_df = derive_class_type_id_from_dim_cls_typ_and_src(perf_comp_ror_df, dim_cls_typ_join_cond)

    return perf_comp_ror_df

def derive_entity_id( perf_comp_ror_df, child_class_type_cd_val ='COMPOSITE'):

    # as we will use alias to join, creating column FQDN(table_name.column_name) with alias
    dim_child_class_val_clm_nm = "{dim_grp_acc_tbl_name}.sor_account_group_id".format( dim_grp_acc_tbl_name=C_DEFAULT_DIM_ACCOUNT_GRP_ALIAS_NAME)
    child_class_type_cd_clm_nm = "{dim_grp_acc_tbl_name}.account_group_type_cd".format(dim_grp_acc_tbl_name=C_DEFAULT_DIM_ACCOUNT_GRP_ALIAS_NAME)

    perf_comp_ror_df = perf_comp_ror_df.alias('src')

    dim_grp_account_typ_join_cond = [
        (sf.col('src.cl_id') == sf.col(dim_child_class_val_clm_nm)) &
        (sf.upper(sf.col(child_class_type_cd_clm_nm)) == child_class_type_cd_val)
    ]

    perf_comp_ror_df = derive_entity_id_from_acc_grp_id_and_src(perf_comp_ror_df,
                                                                join_condition=dim_grp_account_typ_join_cond)
    return perf_comp_ror_df


def read_conv_ror_files_and_Validate_them(pmp_perf_loc, perf_ror_loc, pmp_perf_stg_exception, perf_ror_stg_exception):

    JobContext.logger.info("Reading and validaring pmp_comp_perf and perf_comp_ror files")
    pmp_comp_perf_df = read_from_s3(JobContext.get_property(pmp_perf_loc),
                                    p_with_header=False, p_sep='|')
    perf_comp_ror_df = read_from_s3(JobContext.get_property(perf_ror_loc),
                                    p_with_header=False, p_sep='|')

    if perf_comp_ror_df is None:
        JobContext.logger.info("Performance ror file is empty or not found")
        return (None, None);

    if pmp_comp_perf_df is None:
        JobContext.logger.info("Pmp Composite performance file is empty")
        pmp_comp_perf_df = create_empty_dataframe(C_PMP_COMP_PERF_CLMS)

    # Chnage Column names
    pmp_comp_perf_df = pmp_comp_perf_df.toDF(*C_PMP_COMP_PERF_CLMS)
    perf_comp_ror_df = perf_comp_ror_df.toDF(*C_PERF_COMP_ROR_CLM_NMS)


    # TODO: we need to check if it increase performance
    # current_partitions = pmp_comp_perf_df.getNumPartitions()
    # pmp_comp_perf_df = pmp_comp_perf_df.repartition(current_partitions, sf.col('cl_id'), sf.col('his_date'), sf.col('ror_class'))

    # Validate source files
    perf_comp_ror_df = trim_space(perf_comp_ror_df, C_PERF_COMP_ROR_CLM_NMS)
    perf_comp_ror_df = validate_rec_and_filter(perf_comp_ror_df, C_PERF_COMP_ROR_VALIDATION_MAP,
                                               JobContext.get_property(perf_ror_stg_exception),
                                               prepare_exception_clm_list(C_PERF_COMP_ROR_CLM_NMS))

    pmp_comp_perf_df = trim_space(pmp_comp_perf_df, C_PMP_COMP_PERF_CLMS)
    pmp_comp_perf_df = validate_rec_and_filter(pmp_comp_perf_df, C_PMP_COMP_PERF_ROR_VALIDATION_MAP,
                                               JobContext.get_property(pmp_perf_stg_exception),
                                               prepare_exception_clm_list(C_PMP_COMP_PERF_CLMS))
    his_class_clm_trans_map = C_SRC_CLSS_TYPE_TO_DIM_CLASS_TYPE_MAP.copy()
    his_class_clm_trans_map[None]= sf.col('his_class')
    pmp_comp_perf_df = pmp_comp_perf_df.withColumn('his_date',
                                                   sf.to_date(sf.col('his_date'), C_DATE_FORMAT)).withColumn(
        'his_class', sf.upper(sf.col('his_class'))).withColumn('his_class', when_condition_prepare(his_class_clm_trans_map, 'his_class'))
    JobContext.logger.info("Complted reading and validating pmp_comp_perf and perf_comp_ror files")
    return perf_comp_ror_df,pmp_comp_perf_df




def derive_ror_freq( target_df):
    target_df = target_df.withColumn('ror_freq',
                                     sf.when(sf.col('his_code') == 'M', 'MLY').when(sf.col('his_code') == 'D',
                                                                                    'DLY')
                                     .when(sf.col('his_code') == 'E', 'EOM').otherwise('MLY'))
    return target_df