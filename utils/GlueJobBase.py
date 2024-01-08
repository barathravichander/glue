import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from utils.JobContext import *
from utils.TableConfigLoader import TableConfigLoader
from utils.common_util import format_batch_date
from utils.schema_file_parser_util import SchemaCache
from utils.stub_creation_util import *
from constant.common_constant import *
from utils.schema_file_parser_util import SchemaCache
from constant.property_constant import *
from pyspark.context import SparkContext
from pyspark.sql.functions import *

###############################
#  Base class for all job type, New job should be extending it and override following methods

#  _execute_job(): This method will have Job Specific transformation and validations
#  _get_job_parameter_option_names(): Base class provide default values, but this could be overriden by child class to supply its own parameter list
# _get_logger_name(): This name would be used to Create Logger object
# _validate_job_param: This method will validate the job parameters, Child can override this and perform other validation
###############################
class GlueJobBase(object):

    file_type = ""
    run_validation = ""
    batch_date = ""
    redshift_tmp_dir = ""
    logger = ""
    sc = None
    C_DB_NAME = ""
    C_RED_SHIFT_DB = ""
    g_batch_date_clm= None

    def execute(self):

        self.__initilize_spark_and_glue_context()

        args = self.__resolve_job_parameters()

        self.initilize_logger()
		# set encode to UTF-8, as default is ANSI, which fails with some of special char.
        current_default_ecoding = sys.getdefaultencoding()
        self.logger.info("Current default encoding is {}".format(current_default_ecoding))
        # reload(sys)
        # sys.setdefaultencoding('utf-8')
        # self.logger.info("Changed default encoding to UTF-8 ")
        self.file_type = self._get_file_type()
        JobContextInitializer.initializer(self.glueContext, C_PROPERTY_LIST, args, self.logger, self.file_type, self.batch_date)

        self.C_DB_NAME = self._get_property(C_GLUE_S3_DB_NAME_PROP_KEY)
        self.C_RED_SHIFT_DB = self._get_property(C_GLUE_REDSHIFT_DB_NAME_PROP_KEY)

        self._validate_job_param(args)

        JobContext.batch_date_clm = self.g_batch_date_clm

        JobContext.batch_date = self.batch_date
        self.load_configuration()
        #  This method would be implemented by Child Job
        self._cache_schema()

        self._execute_job()

    def _cache_schema(self):
        SchemaCache.cache()

    ###############################
    #  Child class could override this and perform other job parameter validations
    ###############################
    def _validate_job_param(self, args):
        self.batch_date = args.get('BATCH_DATE')
        self.g_batch_date_clm = format_batch_date(self.batch_date)

    ###############################
    #  Child job must override this and perform its transformation
    ###############################
    def _execute_job(self):
        raise Exception("Implementation class need to override this....")

    def _get_job_parameter_option_names(self):
        return ['TempDir', 'JOB_NAME', 'BATCH_DATE','ENV']

    def _get_logger_name(self):
        raise Exception("Implementation class need to override this....")

    def _get_property(self, prop_key):
        return JobContext.get_property(prop_key)

    # This could have been handle using attribute assignment, but using method, so that its should not be skipped
    def _get_file_type(self):
        raise Exception("Implementation class need to override this....")

    def initilize_logger(self):
        self.Logger = self.spark._jvm.org.apache.log4j.Logger
        # Adds Glue_Job prefix for all logger name so that Splunk can index Glue specific logs
        self.logger = self.Logger.getLogger("Glue_Job_" + self._get_logger_name() )

    def load_configuration(self):
        self.logger.info("Loading configuration..")
        JobContext.table_config_map = TableConfigLoader.load()
        JobContext.logger.info("All configuration loaded..")

    def __resolve_job_parameters(self):
        args = getResolvedOptions(sys.argv, self._get_job_parameter_option_names())
        self.job.init(args['JOB_NAME'], args)
        self.file_type = args.get(C_FILE_TYPE_BEING_PROCESSED_GL_PROP_KEY)
        self.redshift_tmp_dir = args["TempDir"]
        return args

    def __initilize_spark_and_glue_context(self):
        self.sc = SparkContext()
        self.glueContext = GlueContext(self.sc)
        self.spark = self.glueContext.spark_session
        self.job = Job(self.glueContext)