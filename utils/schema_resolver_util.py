from constant.common_constant import C_SCHEMA_DATA_TYPE_CLM_NAME
from pyspark.sql.types import *

import re

from utils.JobContext import JobContext


################################
# Class to prepare schema
####################
class SchemaPreparator(object):

    #############################
    # This methods take schema list created from schema file and return new_schema compatiable with Spark
    #############################
    @staticmethod
    def prepare(schema_list):
        JobContext.logger.info("Creating schema using {}".format(str(schema_list)))
        new_Schema = StructType()
        for row in schema_list:
            resolve_data_type = SchemaDataTypeResolver.resolve(str(row[C_SCHEMA_DATA_TYPE_CLM_NAME]))
            JobContext.logger.info(
                "Given type {}, resolved type is {}".format(row[C_SCHEMA_DATA_TYPE_CLM_NAME], resolve_data_type))
            new_Schema.add(row.column, resolve_data_type)

        return new_Schema


################################
# Class to Resolve Redshift data type to Spark data type
####################
class SchemaDataTypeResolver(object):
    C_STRING_PATTERN = ['Charater varying', 'NCHAR', 'TEXT']
    C_NUMER_DATA_TYPE_PATTERN = ['NUMERIC']

    C_BIGINT_DATE_TYPE_PATTERN = ['bigint', 'int8']
    C_INT_DATA_TYPE_PATTERN = ["integer", 'int4', 'int']
    C_SMALL_INT_DATA_TYPE_PATTERN = ['int2']

    C_FLOAT_DATA_TYPE_PATTERN = ["FLOAT4"]
    C_DOUBLE_INT_DATA_TYPE_PATTERN = ["FLOAT8", "FLOAT"]

    C_PRECISION_KEY = "precision"
    C_SCALE_KEY = "scale"

    @staticmethod
    def resolve(given_type):

        # resolving String first, because is common
        if SchemaDataTypeResolver.__resolve_with_string_type(given_type):
            return StringType()

        # resolving Number type like number(10,1), number(10,1)
        matched_decimal_type = SchemaDataTypeResolver.__resolve_With_big_decimal_type(given_type)
        if matched_decimal_type:
            return matched_decimal_type

        if SchemaDataTypeResolver.check_parttern(SchemaDataTypeResolver.C_BIGINT_DATE_TYPE_PATTERN, given_type):
            return LongType()

        if SchemaDataTypeResolver.check_parttern(SchemaDataTypeResolver.C_SMALL_INT_DATA_TYPE_PATTERN, given_type):
            return ShortType()

        # Resolving int in the last as pattern has int, which will match with int8, which should be LongType on IntergerType
        if SchemaDataTypeResolver.check_parttern(SchemaDataTypeResolver.C_INT_DATA_TYPE_PATTERN, given_type):
            return IntegerType()

        if SchemaDataTypeResolver.check_parttern(SchemaDataTypeResolver.C_FLOAT_DATA_TYPE_PATTERN, given_type):
            return FloatType()

        # Resolving int in the last as pattern has int, which will match with int8, which should be LongType on IntergerType
        if SchemaDataTypeResolver.check_parttern(SchemaDataTypeResolver.C_DOUBLE_INT_DATA_TYPE_PATTERN, given_type):
            return DoubleType()
        # if we dont know it, will consider it as String
        return StringType()

    @staticmethod
    def __resolve_with_string_type(given_type):
        return SchemaDataTypeResolver.check_parttern(SchemaDataTypeResolver.C_STRING_PATTERN, given_type)

    @staticmethod
    def __get_precision_scale(given_type):
        r1 = re.findall(r"\d+", given_type)
        return {SchemaDataTypeResolver.C_PRECISION_KEY: r1[0], SchemaDataTypeResolver.C_SCALE_KEY: r1[1]}

    @staticmethod
    def __resolve_With_big_decimal_type(given_type):
        schema_macthed = SchemaDataTypeResolver.check_parttern(SchemaDataTypeResolver.C_NUMER_DATA_TYPE_PATTERN,
                                                               given_type)
        if schema_macthed:
            precision_scale_map = SchemaDataTypeResolver.__get_precision_scale(given_type)
            precision = precision_scale_map.get(SchemaDataTypeResolver.C_PRECISION_KEY)
            scale = precision_scale_map.get(SchemaDataTypeResolver.C_SCALE_KEY)

            # if schema matched will try to find suitable candidate
            if scale == 0:
                return SchemaDataTypeResolver.__resolve_integer_type(precision, scale)
            else:
                # we could have gone for Double/Float, but as
                return DecimalType(int(precision), int(scale))


    @staticmethod
    def __resolve_integer_type(precision, scale):
        if precision < 5:
            return ShortType()
        elif precision < 10:
            return IntegerType()
        elif precision < 19:
            return LongType()
        else:
            return DecimalType(int(precision), int(scale))

    @staticmethod
    def check_parttern(string_pattern, given_type):
        for ptrn in string_pattern:
            if re.search(ptrn, given_type, re.I):
                return True
        return False
