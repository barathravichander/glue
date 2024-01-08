import pyspark.sql.functions as sf
from constant.common_constant import C_OPERATION_NOT_SUPPORTED, C_EMPTY_FILE_LOG_MSG
from constant.property_constant import C_FILE_TYPE_BEING_PROCESSED_GL_PROP_KEY, \
    C_GLUE_S3_TABLE_DTL_INSTRUMENT_REF_PROP_KEY, C_S3_WRK_MULT_INSTRUMENT_FILTER_OUTPUT_PROP_KEY
from utils.GlueJobBase import GlueJobBase
from utils.JobConfigLoader import JobConfigLoader
from utils.JobContext import JobContext
from utils.common_derivations_util import derive_instrument_id
from utils.s3_operations_util import read_from_s3_catalog_convert_to_df, save_output_to_s3
from utils.common_util import transform_boolean_clm

###############################
#  This is the base class for
###############################

class GlueWithAccSecStubJob(GlueJobBase):

    src_df = None
    C_ACCOUNT_STUB_JOB_PARAM = "ACCOUNT_STUB"
    C_INSTRUMENT_STUB_JOB_PARAM = "INSTRUMENT_STUB"

    def _get_job_parameter_option_names(self):
        l_prop_list = super(GlueWithAccSecStubJob, self)._get_job_parameter_option_names()
        l_addtional_prop_list = [self.C_ACCOUNT_STUB_JOB_PARAM, self.C_INSTRUMENT_STUB_JOB_PARAM]
        l_prop_list = l_prop_list + l_addtional_prop_list   
        return l_prop_list


    def _execute_job(self):

        account_stub = self._get_property(self.C_ACCOUNT_STUB_JOB_PARAM)
        instrument_stub = self._get_property(self.C_INSTRUMENT_STUB_JOB_PARAM)

        if (account_stub.lower() == "y") and (instrument_stub.lower() == "y"):
            JobContext.job_config_map = JobConfigLoader.account_instrument_stub("acc_inst_stub")

        elif (account_stub.lower() == "y") and (instrument_stub.lower() == "n" ):
            JobContext.job_config_map = JobConfigLoader.account_instrument_stub("acc_stub")

        elif (account_stub.lower() == "n") and (instrument_stub.lower() == "y"):
            JobContext.job_config_map = JobConfigLoader.account_instrument_stub("inst_stub")

        elif (account_stub.lower() == "n") and (instrument_stub.lower() == "n"):
            JobContext.job_config_map = JobConfigLoader.account_instrument_stub("no_stub")

        self._job_start()

        src_df_for_stub_jobs = self._read_src_and_stage()
        if src_df_for_stub_jobs==None:
            JobContext.logger.info(C_EMPTY_FILE_LOG_MSG)
            return

        src_df_for_stub_jobs.cache()
        job_config = JobContext.job_config_map.get(type(self).__name__)

        self.logger.info("Found Job configuration {}".format(job_config))
        # Filetype would be used by stub flow to handle the source specific scenarios
        JobContext.glue_job_prop[C_FILE_TYPE_BEING_PROCESSED_GL_PROP_KEY] = self._get_file_type()

        if job_config.instrument_stub_job:
            self._execute_instrument_stub_job(src_df_for_stub_jobs, job_config.instrument_stub_job)

        if job_config.account_stub_job:
            self._execute_account_stub(src_df_for_stub_jobs, job_config.account_stub_job)

        if job_config.security_ref_list is not None:
            self.__derive_instruments(src_df_for_stub_jobs, job_config.security_ref_list)

        # for big file we can split the job in two part, 1- Read validate source file and creation of Stub job
        # 2- Source to target transformation
        if job_config.run_main_job:
            self._execute_main_job()
        else:
            JobContext.logger.info("Main job execution is skipped")

        self._job_end()

    def _execute_account_stub(self, src_df_for_stub_jobs, stub_creation_obj):
        self.logger.info("Starting Account stub Job execution")
        self._copy_attr(stub_creation_obj)
        stub_creation_obj.src_df = src_df_for_stub_jobs
        stub_creation_obj._execute_job()
        self.logger.info("Account stub Job executed")


    def _execute_instrument_stub_job(self, src_df_for_stub_jobs, stub_creation_obj):
        self.logger.info("Starting Instrument stub Job execution")
        self._copy_attr(stub_creation_obj)
        stub_creation_obj.src_df = src_df_for_stub_jobs
        stub_creation_obj._execute_job()
        self.logger.info("Instrument stub Job executed")

    def __derive_instruments(self, src_df_for_stub_jobs, security_ref_list):
        dtl_inst_df = read_from_s3_catalog_convert_to_df(self.C_DB_NAME,
                                                         self._get_property(C_GLUE_S3_TABLE_DTL_INSTRUMENT_REF_PROP_KEY),
                                                         ["instrument_id","instrument_ref_nm","instrument_ref_type_cd",
                                                          "start_dt","end_dt","active_flg","created_ts"])
        dtl_inst_df = transform_boolean_clm(dtl_inst_df,"active_flg")
        if isinstance(security_ref_list["security_ref_nm"], list):
            l_ref_cond = None
            l_typ_cond = None
            for (clm,val) in zip(security_ref_list["security_ref_nm"],security_ref_list["ref_type_cd"]):
                if l_ref_cond is None:
                    l_ref_cond = sf.when((sf.col(clm).isNotNull()) & (sf.col(clm) != ""),sf.col(clm))
                    l_typ_cond = sf.when((sf.col(clm).isNotNull()) & (sf.col(clm) != ""),val)
                else:
                    l_ref_cond = l_ref_cond.when((sf.col(clm).isNotNull()) & (sf.col(clm) != ""),sf.col(clm))
                    l_typ_cond = l_typ_cond.when((sf.col(clm).isNotNull()) & (sf.col(clm) != ""),val)

            src_df_for_stub_jobs = src_df_for_stub_jobs.withColumn("ref_nm",l_ref_cond).withColumn("type_cd",l_typ_cond)

            instrument_df = src_df_for_stub_jobs.select("ref_nm","type_cd").distinct()
            join_cond = (dtl_inst_df["instrument_ref_nm"] == instrument_df["ref_nm"]) \
                        & (dtl_inst_df["instrument_ref_type_cd"] == instrument_df["type_cd"])
        else:
            instrument_df = src_df_for_stub_jobs.select(security_ref_list["security_ref_nm"]).distinct()
            join_cond = dtl_inst_df["instrument_ref_nm"] == instrument_df[security_ref_list["security_ref_nm"]]

        filter_cond = security_ref_list["ref_type_cd"]
        instrument_df = derive_instrument_id(instrument_df,dtl_inst_df,join_cond,self.batch_date,filter_cond)
        save_output_to_s3(instrument_df.select("instrument_id"),"inst_df",
                          self._get_property(C_S3_WRK_MULT_INSTRUMENT_FILTER_OUTPUT_PROP_KEY))

    ###############################
    #  This method will read the source from s3, validate it and return it
    ###############################
    def _read_src_and_stage(self):
        pass

    ###############################
    #  This method will  be override by child, it will be called before starting job
    ###############################
    def _job_start(self):
        JobContext.logger.info("Starting job ")

    ###############################
    #  This method will  be override by child, it will be called at the end of job
    ###############################
    def _job_end(self):
        JobContext.logger.info("Ending job ")

    ###############################
    #  This method  perform main transformation
    ###############################
    def _execute_main_job(self):
        raise Exception(C_OPERATION_NOT_SUPPORTED)

    def _copy_attr(self, other_object):
        other_object.file_type = self.file_type
        other_object.run_validation = self.run_validation
        other_object.batch_date = self.batch_date
        other_object.redshift_tmp_dir = self.redshift_tmp_dir
        other_object.logger = self.logger
        other_object.sc = self.sc
        other_object.C_DB_NAME = self.C_DB_NAME
        other_object.C_RED_SHIFT_DB = self.C_RED_SHIFT_DB
        other_object.g_batch_date_clm= self.g_batch_date_clm
        other_object.spark = self.spark
        other_object.job = self.job