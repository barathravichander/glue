from constant.common_constant import *
from constant.property_constant import C_S3_LOOKUP_TABLE_CONFIG_LOCATION_PROP_KEY
import pyspark.sql.functions as sf
from utils.JobContext import JobContext
from utils.schema_resolver_util import SchemaPreparator
from pyspark.sql.types import IntegerType


class SchemaCache(object):

    schema_df = None;
    table_nme_to_spark_schema = {}

    @classmethod
    def cache(clss):

        schema_loc = JobContext.get_property(C_S3_LOOKUP_TABLE_CONFIG_LOCATION_PROP_KEY)
        JobContext.logger.info("Parsing Schema file {}".format(schema_loc))

        clss.schema_df = JobContext.glue_context.spark_session.read.csv(schema_loc, header=True, sep=C_SCHEMA_FILE_SEP)
        clss.schema_df = clss.schema_df.withColumn(C_SCHEMA_ORDER_CLM_NAME,sf.col(C_SCHEMA_ORDER_CLM_NAME).cast(IntegerType()))
        clss.schema_df.repartition(1).cache()

        JobContext.logger.info("Parsed Schema file {}".format(schema_loc))

    @classmethod
    def get_spark_schema(clss, table_name):
        JobContext.logger.info("Returning Schema for table{}".format(table_name))
        spark_schema = clss.table_nme_to_spark_schema.get(table_name)

        if spark_schema is None:
            JobContext.logger.info("Schema for table not found in cache preparing it")

            raw_schema = clss.schema_df.filter(sf.col(C_SCHEMA_TABLE_COLUMN_NAME) == table_name)\
                .orderBy(clss.schema_df[C_SCHEMA_ORDER_CLM_NAME]).collect()
            spark_schema = SchemaPreparator.prepare(raw_schema)
            clss.table_nme_to_spark_schema[table_name] = spark_schema

            JobContext.logger.info("Schema for table prepared and cached")

        JobContext.logger.info("Returned Schema for table{}".format(table_name))
        return spark_schema;


