#################################################
# This script will load to tool_cpos_acc_perf_ror
#################################################
from constant.property_constant import *
from utils.GlueJobBase import *
from utils.s3_operations_util import *
from utils.common_derivations_util import *
from utils.common_util import *
from pyspark.sql.types import *
from pyspark.sql import functions as sf, Window
from utils.data_validations_util import *
from utils.s3_operations_util import save_output_to_s3, read_from_s3_catalog_convert_to_df

class AccPerfDailyRorExtract(GlueJobBase):
#TODO: Made changes to nettwr to handle only 31 chars instead of 32 --> Barath R/Rajalakshmi
    C_CLM_NME_TO_DATA_POSITION_MAP = {"as_of_date": [1,10],"uan": [11,10],
			"performance_data_status": [21,1],"ow_wire_code": [22,3],"ow_base": [25,7],
            "sleeve_id": [32,4],"foa": [36,6],"faid":[42,6],"amount_value_at_eod":[48,25],
            "performance_status_flag": [73,1],"perflevelcd": [74,1],"period_type": [75,3],
            "rtnperstartdt": [78,10],"rtnperenddt": [88,10],"tot_assetstartamt":[98,31],
            "tot_assetendamt":[129,31],"tot_capinflow":[160,31],"tot_capoutflow":[191,31],
            "tot_taxcharges":[222,31],"tot_transcharges":[253,31],"tot_progfees":[284,31],
            "tot_depfee":[315,31],"tot_othcharges":[346,31],"grosstwr":[377,31],"nettwr":[408,31]}
			
    C_TARGET_CLMS = ['as_of_date','uan','performance_data_status','ow_wire_code','ow_base','sleeve_id','foa','faid','amount_value_at_eod',
                     'performance_status_flag','perflevelcd','period_type','rtnperstartdt','rtnperenddt','tot_assetstartamt','tot_assetendamt',
                     'tot_capinflow','tot_capoutflow','tot_taxcharges','tot_transcharges','tot_progfees','tot_depfee','tot_othcharges','grosstwr',
					 'nettwr','active_flg','batch_dt','created_program_nm','created_user_nm','created_ts']

    def _get_logger_name(self):
        return 'ConvAccPerfDailyRorExtract'

    def _get_file_type(self):
        return 'AccPerfDaily'

    def _execute_job(self):

        self.logger.info("Job DailyRorExtract is started..")
        ###############################
        # read source from s3 location
        ###############################
        src_df = read_fixed_len_file_skip_header_footer(C_S3_DAILY_ROR_EXTRACT_INPUT_LOCATION_PROP_KEY)

        ###########################
        # Checking if file is empty
        ###########################
        if src_df==None:
            self.logger.info(C_EMPTY_FILE_LOG_MSG)
            return

        #############################################
        #  Parsing fixed length data and returning DF
        #############################################
        src_df = parse_fixed_len_data_with_start_len_skip_client_nbr_rcd_typ(src_df, self.C_CLM_NME_TO_DATA_POSITION_MAP)
        src_df = trim_space(src_df,self.C_CLM_NME_TO_DATA_POSITION_MAP)
        src_df = self.comma_remove(src_df, self.C_CLM_NME_TO_DATA_POSITION_MAP)
        src_df = add_audit_columns(src_df, self.g_batch_date_clm)

        ###############################
        # Save output to S3
        ###############################
        save_output_to_s3(src_df.select(self.C_TARGET_CLMS), 'AccPerfDaily', self._get_property(C_S3_DAILY_ROR_EXTRACT_OUTPUT_LOCATION))

        self.logger.info("Job  is completed....")

    def comma_remove(self, df, columns_to_consider):
        for clm in columns_to_consider:
            df = df.withColumn(clm, sf.regexp_replace(sf.col(clm), ",", ""))
        return df


def main():
    job = AccPerfDailyRorExtract()
    job.execute()

if __name__ == '__main__':
    main()
