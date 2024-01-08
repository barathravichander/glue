import pyspark.sql.functions as sf
from jobs.common_stub_account_creation import AccountStubCreation
from jobs.common_stub_instrument_creation import InstrumentStubCreation
from utils.JobContext import JobContext
from utils.config_classes import GlueJobConfig
from constant.common_constant import *

class JobConfigLoader(object):

    """
        Below load method is used for jobs having fixed account and instrument stubs, always stubs will run. Here no need to pass job parameters
        explicitly for running account and instrument stubs.
        Note: Jobs are divided between two methods 1) load 2) account_instrument_stub, while added a new job in this,
        please make sure in which method your job suits.
    """
    @staticmethod
    def load():
        JobContext.logger.info("Loading Job Configuration")
        job_cls_nme_to_job_config = {}
        JobConfigLoader.__fact_set_fact_bench_mark_ror(job_cls_nme_to_job_config)
        JobConfigLoader.__rimes_fact_benchark_ror(job_cls_nme_to_job_config)
        JobConfigLoader.__ben_master_prospect_brdg_instrument_relation(job_cls_nme_to_job_config)
        JobConfigLoader.__prospectus_ben_brdg_instrument_relation(job_cls_nme_to_job_config)
        JobConfigLoader.__annuities_sub_fund_fact_annuities_mv(job_cls_nme_to_job_config)
        JobConfigLoader.__insurance_position_stub(job_cls_nme_to_job_config)
        JobContext.logger.info("Job Configuration loaded")
        return job_cls_nme_to_job_config

    """
        Below account_instrument_stub method is used for jobs having variable account and instrument stubs. Here need to 
        pass job parameters explicitly for running account and instrument stubs. For example need account stub for a job,
        need to pass --ACCOUNT_STUB as Y at Glue job parameters, similarly --INSTRUMENT_STUB as Y for instrument stub,
        If not required pass as N.
    """
    @staticmethod
    def account_instrument_stub(val):
        JobContext.logger.info("Loading Job Configuration")
        job_cls_nme_to_job_config = {}
        JobConfigLoader.__keyrecon_fact_sor_holding(job_cls_nme_to_job_config, val)
        JobConfigLoader.__day_zero_keyrecon_fact_holding(job_cls_nme_to_job_config, val)
        JobConfigLoader.__five29_stg_factsorholding(job_cls_nme_to_job_config, val)
        JobConfigLoader.__day_zero_five29_stg_stub_fact_holding(job_cls_nme_to_job_config, val)
        JobConfigLoader.__keytrans_stub_creation(job_cls_nme_to_job_config, val)
        JobConfigLoader.__annuities_fact_sor_holding(job_cls_nme_to_job_config, val)
        JobConfigLoader.__day_zero_annuities_stg_stub_fact_holding(job_cls_nme_to_job_config, val)

        JobContext.logger.info("Job Configuration loaded")
        return job_cls_nme_to_job_config

    """
        Below instrument_stub method is used for jobs having only variable instrument stubs. Here need to 
        pass job parameters explicitly for running instrument stubs. For example need instrument stub for a job,
        need to pass --INSTRUMENT_STUB as Y for instrument stub,
        If not required pass as N.
    """

    @staticmethod
    def only_instrument_stub(val):
        JobContext.logger.info("Loading Job Configuration")
        job_cls_nme_to_job_config = {}
        JobConfigLoader.__dim_instrument_stub(job_cls_nme_to_job_config, val)
        JobConfigLoader.__caps_fact_class_entity(job_cls_nme_to_job_config, val)
        JobContext.logger.info("Job Configuration loaded")
        return job_cls_nme_to_job_config


    """
        Below are load methods and please remember to add your new methods according to category, we had two categories.
        1) load method 2) account_instrument_stub method.
    """
    @staticmethod
    def __fact_set_fact_bench_mark_ror(job_cls_nme_to_job_config):
        instrument_stub_job = InstrumentStubCreation()
        job_config = GlueJobConfig(run_main_job=True, instrument_stub_job=instrument_stub_job)
        job_cls_nme_to_job_config["FactsetFactBenchmark"] = job_config

    @staticmethod
    def __rimes_fact_benchark_ror(job_cls_nme_to_job_config):
        instrument_stub_job = InstrumentStubCreation()
        job_config = GlueJobConfig(run_main_job=True, instrument_stub_job=instrument_stub_job)
        job_cls_nme_to_job_config["RimesFactBenchmarkRorJob"] = job_config

    @staticmethod
    def __annuities_sub_fund_fact_annuities_mv(job_cls_nme_to_job_config):
        job_config = GlueJobConfig(run_main_job=True, account_stub_job=None, instrument_stub_job=InstrumentStubCreation())
        job_cls_nme_to_job_config["AnnutiesSubFundFactAnnuitiesMV"] = job_config


    @staticmethod
    def __ben_master_prospect_brdg_instrument_relation(job_cls_nme_to_job_config):
        from jobs.conv_ben_mstar_prospect_security_stub import BenMasterProspect_SecurityStub
        job_config = GlueJobConfig(run_main_job=True, instrument_stub_job=BenMasterProspect_SecurityStub())
        job_cls_nme_to_job_config["BenMasterProspect_BrdgInstrumentRelation"] = job_config

    @staticmethod
    def __prospectus_ben_brdg_instrument_relation(job_cls_nme_to_job_config):
        from jobs.prospectus_ben_stub_dim_dtl_instrument_ref import ProspectusBenStubDimDtlInstrumentRef
        job_config = GlueJobConfig(instrument_stub_job=ProspectusBenStubDimDtlInstrumentRef(), run_main_job=True)
        job_cls_nme_to_job_config["ProspectBenBrdgInstrumentRelation"] = job_config

    @staticmethod
    def __insurance_position_stub(job_cls_nme_to_job_config):
        job_config = GlueJobConfig(run_main_job=False, instrument_stub_job=InstrumentStubCreation())
        job_cls_nme_to_job_config["InstrumentPositionStgStub"] = job_config

    """
        Below are account_instrument_stub methods and please remember to add your new methods according to category.
    """
    @staticmethod
    def __keyrecon_fact_sor_holding(job_cls_nme_to_job_config, val):

        instrument_stub_job = None
        account_stub_job = None
        if val == "acc_inst_stub":
            instrument_stub_job = InstrumentStubCreation()
            account_stub_job = AccountStubCreation()
            JobContext.logger.info("Processing Account and Instrument Stub")
        elif val == "acc_stub":
            account_stub_job = AccountStubCreation()
            JobContext.logger.info("Processing Account Stub")
        elif val == "inst_stub":
            instrument_stub_job = InstrumentStubCreation()
            JobContext.logger.info("Processing Instrument Stub")

        job_config = GlueJobConfig(run_main_job=True, instrument_stub_job=instrument_stub_job,
                                   account_stub_job=account_stub_job)
        job_cls_nme_to_job_config["KeyReconFactSorHolding"] = job_config

    @staticmethod
    def __day_zero_keyrecon_fact_holding(job_cls_nme_to_job_config, val):

        instrument_stub_job = None
        account_stub_job = None
        print("val: ",val)
        if val == "acc_inst_stub":
            instrument_stub_job = InstrumentStubCreation()
            account_stub_job = AccountStubCreation()
            JobContext.logger.info("Processing Account and Instrument Stub")
        elif val == "acc_stub":
            account_stub_job = AccountStubCreation()
            JobContext.logger.info("Processing Account Stub")
        elif val == "inst_stub":
            instrument_stub_job = InstrumentStubCreation()
            JobContext.logger.info("Processing Instrument Stub")

        job_config = GlueJobConfig(run_main_job=True, instrument_stub_job=instrument_stub_job,
                                   account_stub_job=account_stub_job)
        job_cls_nme_to_job_config["Day0KeyReconFactHolding"] = job_config

    @staticmethod
    def __five29_stg_factsorholding( job_cls_nme_to_job_config, val):
        from jobs.Five29_security_stub import SecurityStub529

        instrument_stub_job = None
        account_stub_job = None
        if val == "acc_inst_stub":
            instrument_stub_job = SecurityStub529()
            account_stub_job = AccountStubCreation()
            JobContext.logger.info("Processing Account and Instrument Stub")
        elif val == "acc_stub":
            account_stub_job = AccountStubCreation()
            JobContext.logger.info("Processing Account Stub")
        elif val == "inst_stub":
            instrument_stub_job = SecurityStub529()
            JobContext.logger.info("Processing Instrument Stub")

        security_ref_list = {"security_ref_nm": ["adp_nbr","cusip_nbr"],"ref_type_cd":[sf.lit("OT"),sf.lit("CU")]}
        job_config = GlueJobConfig(run_main_job= True, instrument_stub_job=instrument_stub_job, account_stub_job=account_stub_job,
                                   security_ref_list=security_ref_list)
        job_cls_nme_to_job_config["Five29_Stg_Stub_FactSorHolding"] = job_config

    @staticmethod
    def __day_zero_five29_stg_stub_fact_holding( job_cls_nme_to_job_config, val):
        from jobs.Five29_security_stub import SecurityStub529
        instrument_stub_job = None
        account_stub_job = None
        print("val: ",val)
        if val == "acc_inst_stub":
            instrument_stub_job = SecurityStub529()
            account_stub_job = AccountStubCreation()
            JobContext.logger.info("Processing Account and Instrument Stub")
        elif val == "acc_stub":
            account_stub_job = AccountStubCreation()
            JobContext.logger.info("Processing Account Stub")
        elif val == "inst_stub":
            instrument_stub_job = SecurityStub529()
            JobContext.logger.info("Processing Instrument Stub")

        security_ref_list = {"security_ref_nm": ["adp_nbr","cusip_nbr"],"ref_type_cd":[sf.lit("OT"),sf.lit("CU")]}
        job_config = GlueJobConfig(run_main_job= True, instrument_stub_job=instrument_stub_job, account_stub_job=account_stub_job,
                                   security_ref_list=security_ref_list)
        job_cls_nme_to_job_config["Day0_Five29_Stg_Stub_FactHolding"] = job_config

    @staticmethod
    def __dim_instrument_stub(job_cls_nme_to_job_config, val):

        instrument_stub_job = None
        if val == "inst_stub":
            instrument_stub_job = InstrumentStubCreation()
            JobContext.logger.info("Processing Instrument Stub")

        security_ref_list = {"security_ref_nm": "security_adp_nbr","ref_type_cd":[sf.lit("OT")]}
        job_config = GlueJobConfig(instrument_stub_job=instrument_stub_job, run_main_job=False, security_ref_list=security_ref_list)
        job_cls_nme_to_job_config["DimInstrumentStub"] = job_config

    @staticmethod
    def __caps_fact_class_entity(job_cls_nme_to_job_config, val):
        from jobs.caps_security_stub import CapsSecurityStub
        instrument_stub_job = None
        if val == "inst_stub":
            instrument_stub_job = CapsSecurityStub()
            JobContext.logger.info("Processing Instrument Stub")

        job_config = GlueJobConfig(instrument_stub_job=instrument_stub_job, run_main_job=True)
        job_cls_nme_to_job_config["CapsFactClassEntity"] = job_config

    @staticmethod
    def __keytrans_stub_creation(job_cls_nme_to_job_config, val):
        instrument_stub_job = None
        account_stub_job = None
        if val == "acc_inst_stub":
            instrument_stub_job = InstrumentStubCreation()
            account_stub_job = AccountStubCreation()
        elif val == "acc_stub":
            account_stub_job = AccountStubCreation()
        elif val == "inst_stub":
            instrument_stub_job = InstrumentStubCreation()

        security_ref_list = {"security_ref_nm": "symbol", "ref_type_cd":[sf.lit("CU")]}
        job_config = GlueJobConfig(run_main_job=False, instrument_stub_job=instrument_stub_job,
                                   account_stub_job=account_stub_job, security_ref_list= security_ref_list)
        job_cls_nme_to_job_config["KeytransStubCreation"] = job_config

    @staticmethod
    def __annuities_fact_sor_holding(job_cls_nme_to_job_config, val):
        instrument_stub_job = None
        account_stub_job = None
        if val == "acc_inst_stub":
            instrument_stub_job = InstrumentStubCreation()
            account_stub_job = AccountStubCreation()
        elif val == "acc_stub":
            account_stub_job = AccountStubCreation()
        elif val == "inst_stub":
            instrument_stub_job = InstrumentStubCreation()

        security_ref_list = {"security_ref_nm": "src_instrument_nm","ref_type_cd":[sf.lit(C_INSTRUMENT_REF_TYPE_CD_INSU)]}
        from jobs.annuities_security_stub import AnnutiesSecurityStub
        job_config = GlueJobConfig(run_main_job=True, account_stub_job=account_stub_job, instrument_stub_job=instrument_stub_job,
                                   security_ref_list=security_ref_list)
        job_cls_nme_to_job_config["Annuties529FactSorHolding"] = job_config

    @staticmethod
    def __day_zero_annuities_stg_stub_fact_holding(job_cls_nme_to_job_config, val):
        instrument_stub_job = None
        account_stub_job = None
        if val == "acc_inst_stub":
            instrument_stub_job = InstrumentStubCreation()
            account_stub_job = AccountStubCreation()
            print("Both acc and instrument stub")
        elif val == "acc_stub":
            account_stub_job = AccountStubCreation()
        elif val == "inst_stub":
            instrument_stub_job = InstrumentStubCreation()

        security_ref_list = {"security_ref_nm": "src_instrument_nm","ref_type_cd":[sf.lit(C_INSTRUMENT_REF_TYPE_CD_INSU)]}
        from jobs.annuities_security_stub import AnnutiesSecurityStub
        job_config = GlueJobConfig(run_main_job=True, account_stub_job=account_stub_job, instrument_stub_job=instrument_stub_job,
                                   security_ref_list=security_ref_list)
        job_cls_nme_to_job_config["Day0Annuties529FactHolding"] = job_config
