###############################
#  Base class for all job type, New job should be extending it and override following methods

#  _execute_job(): This method will have Job Specific transformation and validations
#  _get_job_parameter_option_names(): Base class provide default values, but this could be overriden by child class to supply its own parameter list
# _get_logger_name(): This name would be used to Create Logger object
# _validate_job_param: This method will validate the job parameters, Child can override this and perform other validation
#                       History
# Date            Version No    Developer         Change
# -------         ------        ---------         ------
# 03/23/2022       0.1          Barath R           Changed Glue logger name to PRU
# --------------------------------------------------------------------------
###############################
import configparser
import logging
import sys

from awsglue.utils import getResolvedOptions
from constant.property_constant import C_GLUE_S3_DB_NAME_PROP_KEY, C_REDSHIFT_DB_NAME_PROP_KEY, C_PROPERTY_LIST
from pyshell_utils.PyshellJobContext import PyShellJobContext, JobContextInitializer
from pyshell_utils.common_pyshell_util import formatted_batch_date


config = configparser.RawConfigParser()


class PyshellJobBase(object):

    batch_date = ""
    redshift_tmp_dir = ""
    logger = ""
    sc = None
    prop_path = ""
    C_DB_NAME = ""
    C_RDS_DB = ""
    g_batch_date_clm = None

    def execute(self):

        self.initilize_logger()
        args = self.__resolve_job_parameters()
        # set encode to UTF-8, as default is ANSI, which fails with some of special char.
        current_default_ecoding = sys.getdefaultencoding()

        self.logger.info("Current default encoding is {}".format(current_default_ecoding))
        # reload(sys)
        # sys.setdefaultencoding('utf-8')
        # self.logger.info("Changed default encoding to UTF-8 ")

        prop_path = args.get('PROP_PATH')
        self._validate_job_param(args)
        JobContextInitializer.initializer(C_PROPERTY_LIST, args, self.logger,
                                          self.batch_date, prop_path)

        self.C_DB_NAME = self._get_property(C_GLUE_S3_DB_NAME_PROP_KEY)
        self.C_RDS_DB = self._get_property(C_REDSHIFT_DB_NAME_PROP_KEY)

        #  This method would be implementated by Child Job
        self._execute_job()

    ###############################
    #  Child class could override this and perform other job parameter validations
    ###############################
    def _validate_job_param(self, args):
        self.batch_date = args.get('BATCH_DATE')
        self.logger.info("batch date is : {}".format(self.batch_date))
        self.g_batch_date_clm = formatted_batch_date(self.batch_date)
        self.logger.info("formatted batch date is : {}".format(self.g_batch_date_clm))

    ###############################
    #  Child job must override this and perform its transformation
    ###############################
    def _execute_job(self):
        raise Exception("Implementation class need to override this....")

    def _mandatory_job_params(self):
        return ['ENV', 'PROP_PATH']

    def _optional_job_params(self):
        from datetime import date
        today = str(date.today())
        return {'COLUMN_LIST': None, 'CLIENT_NAME': 'UBS', 'BATCH_DATE': today}

    def _get_logger_name(self):
        raise Exception("Implementation class need to override this....")

    def _get_property(self, prop_key):
        return PyShellJobContext.get_property(prop_key)

    def initilize_logger(self):
        ##################################
        #  Log configuration
        ##################################
        self.logger = logging.getLogger("Glue_Job_" + self._get_logger_name())
        self.logger = logging.getLogger("Glue_PRU_Pyshell_Job_")
        self.logger .setLevel(logging.INFO)
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger .addHandler(handler)

    def __resolve_job_parameters(self):
        mandatory_args = self._mandatory_job_params()
        optional_args = self._optional_job_params()
        args = self.get_glue_args(mandatory_args, optional_args)
        self.logger.info("args : *********  {}  **********".format(args))

        return args

    def get_glue_args(self, mandatory_fields, default_optional_args):

        given_optional_fields_key = list(set([i[2:] for i in sys.argv]).intersection([i for i in default_optional_args]))
        args = getResolvedOptions(sys.argv, mandatory_fields+given_optional_fields_key)
        default_optional_args.update(args)
        self.logger.info("default_optional_args : *********  {}  **********".format(default_optional_args))
        return default_optional_args



