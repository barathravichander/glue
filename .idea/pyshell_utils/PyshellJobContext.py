import configparser
from constant.property_constant import *
import boto3


##############################
# This class will hold the all data which will be needed in various steps of Job executions
##############################


class PyShellJobContext(object):

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
    bucket_name = None


    @classmethod
    def get_property(clss, p_property_key, p_required=False, p_default_value=None):
        try:
            l_env = PyShellJobContext.glue_job_prop.get(C_ENVIRONMENT_GL_PROP_KEY)
            clss.logger.info("Selected working environment - {}".format(l_env))
            l_env_prop_list = dict(PyShellJobContext.config.items(l_env))

            print ("properties list is : ", l_env_prop_list)

            prop_val = PyShellJobContext.config.get('property', p_property_key)

            prop_val = prop_val.format(l_env_prop_list)
        except Exception as e:
            clss.logger.error("Union Error - {}".format(str(e)))
            clss.logger.info("Property {} not found in property file, searching in Job Parameter".format(p_property_key))
            prop_val = PyShellJobContext.glue_job_prop.get(p_property_key)

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
    def initializer(prop_location, p_glue_job_prop, p_logger, p_batch_date, p_prop_path):
        p_logger.info(
            "Started Initailizing JobContext with Job parameters {} and properties {}, batch date: {}".format(
                p_glue_job_prop, prop_location, p_batch_date))

        client = boto3.client('s3')
        prop_path = p_prop_path

        ''' Deriving the bucket name '''
        PyShellJobContext.bucket_name = prop_path.split('//')[1].split('/')[0]
        l_bucket = PyShellJobContext.bucket_name

        p_logger.info("Provided bucket name is {}".format(l_bucket))

        prop_path = prop_path.split(l_bucket)[1][1:]

        p_logger.info("Provided properties file path is {}".format(prop_path))

        for prop in prop_location:

            l_prop_path = prop_path+prop

            p_logger.info("File path along with property {} is {}".format(prop, l_prop_path))

            result = client.get_object(Bucket=l_bucket, Key=l_prop_path)
            l_prop_file = result["Body"].read().decode('utf-8-sig').splitlines()
            l_tmp_path = "/tmp/"+prop

            p_logger.info("Writing configuration file in temp location - {}".format(l_tmp_path))
            with open(l_tmp_path, 'w', encoding='utf-8') as filehandle:
                filehandle.writelines("%s\n" % line for line in l_prop_file)
            l_prop_file = []

        for pro in prop_location:
            print (pro)
            PyShellJobContext.config.read(pro)
        p_logger.info("All property files are read successfully")

        PyShellJobContext.glue_job_prop = p_glue_job_prop
        PyShellJobContext.logger = p_logger
        PyShellJobContext.batch_date = p_batch_date

