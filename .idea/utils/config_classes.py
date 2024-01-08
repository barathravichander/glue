from utils.common_util import auto_str

@auto_str
class TableConfig(object):
    resolve_choice = None
    include_stub = False
    partitioned_tbl = False
    column_list = None

    def __init__(self, resolve_choice, include_stub=False, partitioned_tbl=False, column_list = None):
        self.include_stub = include_stub
        self.resolve_choice = resolve_choice
        self.partitioned_tbl = partitioned_tbl
        self.column_list = column_list

@auto_str
class ResolveChoiceConfig(object):
    fields_to_cast_mapping = {}

@auto_str
class GlueJobConfig(object):
    instrument_stub_job = None
    account_stub_job = None
    run_main_job = True
    security_ref_list = None

    def __init__(self, run_main_job=True,  instrument_stub_job=None, account_stub_job= None, security_ref_list = None):
        self.run_main_job = run_main_job
        self.instrument_stub_job = instrument_stub_job
        self.account_stub_job = account_stub_job
        self.security_ref_list = security_ref_list



