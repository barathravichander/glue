from constant.property_constant import *
from utils.common_derivations_util import get_property_from_dim_tbl
from pyspark.sql import functions as sf
from pyspark.sql.types import *

from utils.common_util import create_empty_dataframe
from utils.data_validations_util import *
from utils.s3_operations_util import  save_output_to_s3,read_from_s3
from pyspark.context import *
from utils.JobContext import JobContext
from constant.common_constant import *
from utils.common_util import lower_case
from pyspark.sql import functions as sf


class Type9ExcludeCustomProcessor(object):

    def execute_account_type_9_exclusion(self, src_df):
        JobContext.logger.info("Executing Account type 9 filter exclude logic")
        # then check for configured property
        prop_val = get_property_from_dim_tbl(C_TYPE_9_EXCLUDE_PROPERTY_DESC, False)

        # check for condition
        if lower_case(prop_val) in TRUE_VALUES:
            JobContext.logger.info("type9 logic will be executed")
            src_df = src_df.filter(~sf.col('account_type_id').eqNullSafe(9))
        else:
            JobContext.logger.info("Account type 9 filter logic skipped")
        return src_df


class MultiCurrencyCustomProcessor(object):

    def execute_multi_currency_logic(self):
        JobContext.logger.info("Checking if Multi currency logic needs to execute")
        execute_mulit_currecny_logic=False
        # then check for configured property
        prop_val = get_property_from_dim_tbl(C_MULTI_CURRENCY_SUPPORT_PROPERTY_DESC, False)
        # check for condition
        if lower_case(prop_val) in TRUE_VALUES:
            JobContext.logger.info("Multi currency logic will be executed")
            execute_mulit_currecny_logic = True
        else:
            JobContext.logger.info("Multi currency logic will be skipped")
        return execute_mulit_currecny_logic




class OptionalSecurityCustomProcessor(object):

    def exclude_optional_securities(self, src_df, security_adp_col_name):

        JobContext.logger.info("Inside the optional_security logic...")
        prop_val = get_property_from_dim_tbl(C_OPTIONAL_SECURITY_PROPERTY_DESC, excp_reqd=False)

        if prop_val in TRUE_VALUES:
            JobContext.logger.info("Executing the custom logic")
            src_df=self.__adp_exclude(src_df, security_adp_col_name)
            return src_df
        else:
            JobContext.logger.info("Not executing the custom logic")
            return src_df

    def __adp_exclude(self, l_source_data_df, l_security_adp_col_name):

        JobContext.logger.info("Executing the security exclude logic")

        stg_purchase_sales_trans_df=read_from_s3(JobContext.get_property(C_S3_H2A_OUTPUT_LOCATION_PROP_KEY))
        stg_book_keeping_trans_df=read_from_s3(JobContext.get_property(C_S3_STG_BOOK_KEEPING_TRANS_OUTPUT_LOCATION_PROP_KEY))
        stg_dividend_trans_df=read_from_s3(JobContext.get_property(C_S3_B2D_OUTPUT_LOCATION_PROP_KEY))
        stg_money_trans_df=read_from_s3(JobContext.get_property(C_S3_B2E_OUTPUT_LOCATION_PROP_KEY))
        stg_holding_df=read_from_s3(JobContext.get_property(C_S3_HOA_STG_HOLDING_OUTPUT_LOCATION_PROP_KEY))

        if stg_purchase_sales_trans_df is not None:
            union_df=stg_purchase_sales_trans_df.select('security_adp_nbr')
        else:
            stg_purchase_sales_trans_df=create_empty_dataframe(['security_adp_nbr'])
            union_df=stg_purchase_sales_trans_df

        if stg_book_keeping_trans_df is not None:
            union_df=union_df.union(stg_book_keeping_trans_df.select('security_adp_nbr'))
        else:
            stg_book_keeping_trans_df=create_empty_dataframe(['security_adp_nbr'])
            union_df=union_df.union(stg_book_keeping_trans_df)

        if stg_dividend_trans_df is not None:
            union_df=union_df.union(stg_dividend_trans_df.select('security_adp_nbr'))
        else:
            stg_dividend_trans_df=create_empty_dataframe(['security_adp_nbr'])
            union_df=union_df.union(stg_dividend_trans_df)

        if stg_money_trans_df is not None:
            union_df=union_df.union(stg_money_trans_df.select('security_adp_nbr'))
        else:
            stg_money_trans_df=create_empty_dataframe(['security_adp_nbr'])
            union_df=union_df.union(stg_money_trans_df)

        if stg_holding_df is not None:
            union_df=union_df.union(stg_money_trans_df.select('security_adp_nbr'))
        else:
            stg_holding_df=create_empty_dataframe(['security_adp_nbr'])
            union_df=union_df.union(stg_holding_df)

        if union_df is not None:
            union_df=union_df.distinct()
            JobContext.logger.info("Joining the source DF and Union DF")
            c_join_condition_lookup = l_source_data_df[l_security_adp_col_name] == union_df.security_adp_nbr
            l_source_data_df=l_source_data_df.join(union_df,c_join_condition_lookup, 'left')
            l_source_data_df=l_source_data_df.filter((~l_source_data_df[l_security_adp_col_name].startswith('8')) | (l_source_data_df.security_adp_nbr.isNotNull())).drop('security_adp_nbr')
            JobContext.logger.info("Executed the adp exclude logic successfully..!")
            return l_source_data_df
        else:
            JobContext.logger.info("Union DF is empty, so no join operation will be performed..")
            return l_source_data_df

class TrailerTransTypIDFlgCustomProcessor(object):

    def trailer_trans_type_id_flg_exclusion(self, src_df=None):
        JobContext.logger.info("Executing trailer trans type ID flag exclude logic")
        # then check for configured property
        prop_val = get_property_from_dim_tbl(C_TRAILER_TRANS_TYPE_ID_FLG_DESC, False)

        # check for condition
        if lower_case(prop_val) in TRUE_VALUES:
            JobContext.logger.info("Trailer trans type ID flag logic will be executed")
        else:
            JobContext.logger.info("Trailer trans type ID flag logic skipped")
        return src_df

class ClassificationDervFlgCustomProcessor(object):

    def classification_derv_flg_exclusion(self, src_df=None):
        JobContext.logger.info("Executing classification derv flag exclude logic")
        # then check for configured property
        prop_val = get_property_from_dim_tbl(C_CLASSIFICATION_DERV_FLG_DESC, False)

        # check for condition
        if lower_case(prop_val) in TRUE_VALUES:
            JobContext.logger.info("Classification derv flag logic will be executed")
        else:
            JobContext.logger.info("Classification derv flag logic skipped")
        return src_df

class TrailerEffDtFlgCustomProcessor(object):

    def trailer_eff_dt_flg_exclusion(self, src_df=None):
        JobContext.logger.info("Executing trailer effective date flag exclude logic")
        # then check for configured property
        prop_val = get_property_from_dim_tbl(C_TRAILER_EFFECTIVE_DT_FLG_DESC, False)

        # check for condition
        if lower_case(prop_val) in TRUE_VALUES:
            JobContext.logger.info("Trailer effective date logic will be executed")
        else:
            JobContext.logger.info("Trailer effective date flag logic skipped")
        return src_df



class AdjustBackDatedTransCustomProcessor(object):

    def adjust_back_dated_trans(self, src_df=None):
        JobContext.logger.info("Executing back dated transaction adjustment logic")
        # then check for configured property
        """"
        TRAN_BACK_DT_B4_CONV_FLG - Y/N	
        Y - Current behavior. Posts on the actual date of the transaction
        N - UBS behavior. Posts on "CONV_DT+1BUSINESS DAY"

        """
        prop_val = get_property_from_dim_tbl(C_BACK_DATED_TRANS_PROPERTY_DESC, False)

        # check for condition
        if lower_case(prop_val) in C_DIM_PROPERTY_VALUE_DESC_N:
            JobContext.logger.info("Executed back dated transaction adjustment logic")
            return True
        else:
            JobContext.logger.info("Skipping back dated transaction adjustment logic")
            return False


class CancelTransCustomProcessor(object):

    def cancel_trans_processing(self, src_df=None):
        JobContext.logger.info("Processing cancel transaction logic")
        # then check for configured property
        prop_val = get_property_from_dim_tbl(C_CANCEL_TRANS_PROPERTY_DESC, False)
        """
        TRAN_CANCEL_NEW_MTCH_FLG - Y/N
        Y - Current behavior. Checks for matching NEW transaction.
        N - UBS behavior. Does not look for a match and processes all cancels.

        """

        # check for condition
        if lower_case(prop_val) in C_DIM_PROPERTY_VALUE_DESC_N:
            JobContext.logger.info("Processing cancel transaction logic is enabled")
            return True
        else:
            JobContext.logger.info("Processing of cancel transaction logic is disabled")
            return False
