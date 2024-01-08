
###############################
# This Script will hold various RedShift related functions like Read, write etc.
###############################

from awsglue.dynamicframe import DynamicFrame
from constant.property_constant import *
from utils.JobContext import JobContext


###############################
# Function to read Redshift tables, this is capable to filter rows and fetch only given fields
###############################
def insert_into_redshift( p_df_to_insert, p_table_name):
    l_glue_context = JobContext.glue_context
    JobContext.logger.info("Inserting data into redshift table {}".format(p_table_name))
    l_dynamic_frame = DynamicFrame.fromDF(p_df_to_insert, l_glue_context,
                                          "df_to_save")
    l_glue_context.write_dynamic_frame.from_catalog(frame=l_dynamic_frame,
                                                    database= JobContext.get_property(C_GLUE_REDSHIFT_DB_NAME_PROP_KEY),
                                                    table_name=p_table_name,
                                                    redshift_tmp_dir=JobContext.get_property("TempDir"),
                                                    transformation_ctx="transformation_ctx")
    JobContext.logger.info("Inserted data into redshift table")

###############################
# Function to read Redshift tables and return Dataframe
###############################
def read_redshift_table(p_glue_database, p_table_name, p_temp_dir):
    JobContext.logger.info("Reading Redshift {} table from database {}".format(p_table_name, p_glue_database))
    l_glue_context = JobContext.glue_context
    l_df = l_glue_context.create_dynamic_frame.from_catalog(database=JobContext.get_property(C_GLUE_REDSHIFT_DB_NAME_PROP_KEY), table_name=p_table_name,
                                                            redshift_tmp_dir=p_temp_dir,
                                                            transformation_ctx="datasource0").toDF()
    JobContext.logger.info("Read Redshift table {}".format(p_table_name))
    return l_df

###############################
# Function to truncate and load Redshift tables
###############################
def insert_into_redshift_from_jdbc_conf(p_df_to_insert, p_table_name, p_preaction = None):
    l_glue_context = JobContext.glue_context
    JobContext.logger.info("Inserting data into Redshift table {}".format(p_table_name))
    l_dynamic_frame = DynamicFrame.fromDF(p_df_to_insert, l_glue_context,"df_to_save")

    l_redshift_db_name = JobContext.get_property(C_REDSHIFT_DB_NAME_PROP_KEY)
    l_redshift_connection_name = JobContext.get_property(C_REDSHIFT_CONNECTION_NAME_PROP_KEY)

    JobContext.logger.info("redshift database name -> {}".format(l_redshift_connection_name))
    JobContext.logger.info("redshift connection name -> {}".format(l_redshift_connection_name))

    if p_preaction is not None:
        l_glue_context.write_dynamic_frame.from_jdbc_conf(frame = l_dynamic_frame,
                                                          catalog_connection = l_redshift_connection_name,
                                                          connection_options = {"preactions":p_preaction,
                                                                                "dbtable": p_table_name,
                                                                                "database": l_redshift_db_name},
                                                          redshift_tmp_dir = JobContext.get_property("TempDir"),
                                                          transformation_ctx = "transformation_ctx")
    else:
        l_glue_context.write_dynamic_frame.from_jdbc_conf(frame = l_dynamic_frame,
                                                          catalog_connection = l_redshift_connection_name,
                                                          connection_options = {"dbtable": p_table_name,
                                                                                "database": l_redshift_db_name},
                                                          redshift_tmp_dir = JobContext.get_property("TempDir"),
                                                          transformation_ctx = "transformation_ctx")
    JobContext.logger.info("Inserted data into Redshift table")