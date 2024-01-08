import configparser
from constant.property_constant import C_ENVIRONMENT_GL_PROP_KEY


##############################
# This class will hold the all data which will be needed in various steps of Job executions
##############################

class JobContext(object):
    config = configparser.RawConfigParser()
    glue_context = None
    glue_job_prop = None
    logger = None
    job_property = None
    file_being_processed = None
    batch_date = None
    batch_date_clm = None
    table_config_map = {}
    job_config_map = {}

    @classmethod
    def get_property(clss, p_property_key, p_required=False, p_default_value=None):
        try:
            l_env =JobContext.glue_job_prop.get(C_ENVIRONMENT_GL_PROP_KEY)
            clss.logger.info("Selected working environment - {}".format(l_env))
            l_env_prop_list = dict(JobContext.config.items(l_env))

            prop_val = JobContext.config.get('property', p_property_key)
            prop_val = prop_val.format(l_env_prop_list)
        except:
            clss.logger.info("Property {} not found in property file, searching in Job Parameter".format(p_property_key))
            prop_val = JobContext.glue_job_prop.get(p_property_key)

        if p_required & (prop_val is None):
            raise Exception("Property {} not found in configuration file and job property".format(p_property_key))

        if prop_val is None:
            prop_val = p_default_value

        clss.logger.info("Property {} is returning for key {}".format(prop_val, p_property_key))
        return prop_val


class JobContextInitializer(object):

    ###############################
    #  Method to iniliaze the Job Conext
    ###############################
    @staticmethod
    def initializer(p_glue_context, prop_location, p_glue_job_prop, p_logger, p_file_being_processed, p_batch_date):
        p_logger.info(
            "Started Initailizing JobContext with Job parameters {} and properties {}, file being processed:{}, batch date: {}".format(
                p_glue_job_prop, prop_location, p_file_being_processed, p_batch_date))

        for pro in prop_location:
            JobContext.config.read(pro)
        p_logger.info("All property files are read successfully")

        JobContext.glue_context = p_glue_context
        JobContext.glue_job_prop = p_glue_job_prop
        JobContext.logger = p_logger
        JobContext.file_being_processed = p_file_being_processed
        JobContext.batch_date = p_batch_date
