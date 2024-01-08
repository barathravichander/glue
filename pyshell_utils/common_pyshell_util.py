import datetime
import re
from constant.common_constant import C_BATCH_DATE_FORMAT
from pyshell_utils.PyshellJobContext import *


def formatted_batch_date(p_batch_dt, format="%Y%m%d"):
    try:
        return datetime.datetime.strptime(p_batch_dt, C_BATCH_DATE_FORMAT).strftime(format)
    except ValueError:
        raise ValueError("Incorrect data format, should be YYYY-MM-DD")


def get_partition_column():
    return "pm_batch_dt_" + formatted_batch_date(PyShellJobContext.batch_date)



