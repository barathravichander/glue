import boto3
from airflow import DAG
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.utils.task_group import TaskGroup

####################################
# Get SSM Parameters from Paramstore
####################################
ssm = boto3.client('ssm')
env = Variable.get("af_env")

#Fetching iam role
ssm_iam_role_key = ssm.get_parameter(Name= '/br/unipru'+env+'/af_iam_role')
iam_role = ssm_iam_role_key['Parameter']['Value']
print("Fetched ar_path: {} ".format(iam_role))

#Fetching aws region
ssm_region_name_key = ssm.get_parameter(Name= '/br/unipru'+env+'/af_region_nm')
region_name = ssm_region_name_key['Parameter']['Value']
print("Fetched ar_path: {} ".format(region_name))

#Fetching aurora username
ssm_ar_usernm_key = ssm.get_parameter(Name= '/br/unipru'+env+'/af_ar_username')
ar_usernm = ssm_ar_usernm_key['Parameter']['Value']            
print("Fetched ar_usernm: {} ".format(ar_usernm))

#Fetching s3 bucket name
ssm_s3_bucket_nm_key = ssm.get_parameter(Name= '/br/unipru'+env+'/af_s3_bucket_nm')
s3_bucket_nm = ssm_s3_bucket_nm_key['Parameter']['Value']            
print("Fetched ar_usernm: {} ".format(s3_bucket_nm))

#Fetching aurora shell script path
ssm_ar_path_key = ssm.get_parameter(Name= '/br/unipru'+env+'/af_ar_path')
ar_path = ssm_ar_path_key['Parameter']['Value']
print("Fetched ar_path: {} ".format(ar_path))

#Fetching glue shell script path
ssm_gl_path_key = ssm.get_parameter(Name= '/br/unipru'+env+'/af_gl_path')
gl_path = ssm_gl_path_key['Parameter']['Value']
print("Fetched gl_path: {} ".format(gl_path))

#ToDo: Parameterize af_ssh_conn using ssm. Right now, challenge with .pem key
#ssm_af_ssh_conn_key = ssm.get_parameter(Name= '/br/uniprudev/af_ssh_conn', WithDecryption=True)
#af_ssh_conn = ssm_af_ssh_conn_key['Parameter']['Value']            
#print("Fetched ar_usernm: {} ".format(af_ssh_conn))

#tags = af_ssh_conn['Parameter']['Tags']

##############################
# Default Args
##############################
default_args = {
	'owner': 'BR-UNIPRU-DEV',
	'start_date': datetime.now() - timedelta(days=1),
	'retry_delay': timedelta(minutes=5),
	'email_on_failure': False
}

###########################
# Getting AF Env Variables
###########################
batch_date = Variable.get("af_batch_date")
client_nbr = Variable.get("af_client_nbr")	
aws_region = Variable.get("af_aws_region")
ldate = Variable.get("af_ldate")
this_run = Variable.get("af_this_run")
firm_code = Variable.get("af_firm_code")
business_date = Variable.get("af_business_date")

####################
# Invoking the DAG
####################
with DAG(dag_id = 'PRLITE_CTERA_BATCH_TEMPLTE', default_args = default_args, description='PRLITE_CTERA BATCH TEMPLATE', schedule_interval = None, catchup=False) as dag:

    start = DummyOperator(task_id="start")
    
    BEGIN_NOTIFICATION = DummyOperator(task_id="BEGIN_NOTIFICATION")
    
    AR_PRE_EXTRACT_CLEANUP = SSHOperator(
                                task_id = 'AR_PRE_EXTRACT_CLEANUP',
                                ssh_conn_id = 'af_ssh_conn',
                                command=f"bash {ar_path}runner.sh {ar_usernm} {ldate}_{this_run} extract_cleanup.sh {firm_code}"                             
                                )
    
    AR_PRE_BATCH_BKP = SSHOperator(
                                task_id = 'AR_PRE_BATCH_BKP',
                                ssh_conn_id = 'af_ssh_conn',
                                command=f"bash {ar_path}runner.sh {ar_usernm} {ldate}_{this_run} daily_batch_backup.sh {firm_code} {business_date}"
                             )
    
    FILES_RETEN_LOGS_CLEANUP = DummyOperator(task_id="FILE_RETEN_LOGS_CLEANUP")
    
    FILES_UNZIP = DummyOperator(task_id="FILES_UNZIP")
    
    AR_CLEANUP_S3_DATA = SSHOperator(
                                task_id = 'AR_CLEANUP_S3_DATA',
                                ssh_conn_id = 'af_ssh_conn',
                                command=f"bash {ar_path}runner.sh {ar_usernm} {ldate}_{this_run} daily_cleanup_s3_data.sh {firm_code}"
                             )

    with TaskGroup("SECURITY_STAGING") as SECURITY_STAGING:

        Start_Stg = DummyOperator(task_id="Start_Stg")
        End_Stg = DummyOperator(task_id="End_Stg")

        AR_S3_COPY_MSA = S3KeySensor(
                        task_id='AR_S3_COPY_MSA',
                        bucket_key='pr_ubsfi/incoming/today/msa/*',
                        wildcard_match=True,
                        bucket_name=f{af_s3_bucket_nm},
                        timeout=18*60*60,
                        poke_interval=30,
                        dag=dag
                        )
        GL_DAILY_TRUNC_STAGE_SEC_INFO = SSHOperator(
                                        task_id = 'GL_DAILY_TRUNC_STAGE_SEC_INFO',
                                        ssh_conn_id = 'af_ssh_conn',
                                        command=f"bash {gl_path}util_db_truncate.sh STG_SEC_INFO {env} {aws_region}"
                                     )

        GL_DAILY_MSA_STG_SEC_INFO = SSHOperator(
                                        task_id = 'GL_DAILY_MSA_STG_SEC_INFO',
                                        ssh_conn_id = 'af_ssh_conn',
                                        command=f"bash {gl_path}msa_stg_sec_info.sh {business_date} {env} MSA {client_nbr} {aws_region}"
                                     )

        Start_Stg >> AR_S3_COPY_MSA >> GL_DAILY_TRUNC_STAGE_SEC_INFO >> GL_DAILY_MSA_STG_SEC_INFO >> End_Stg

        AR_S3_COPY_MSB = S3KeySensor(
                        task_id='AR_S3_COPY_MSB',
                        bucket_key='pr_ubsfi/incoming/today/msa/*',
                        wildcard_match=True,
                        bucket_name=f{af_s3_bucket_nm},
                        timeout=18*60*60,
                        poke_interval=30,
                        dag=dag
                        )

        GL_DAILY_TRUNC_STAGE_SEC_DESC = SSHOperator(
                                        task_id = 'GL_DAILY_TRUNC_STAGE_SEC_DESC',
                                        ssh_conn_id = 'af_ssh_conn',
                                        command=f"bash {gl_path}util_db_truncate.sh STG_SEC_DESC {env} {aws_region}"
                                     )

        GL_DAILY_MSB_STG_SEC_DESC = SSHOperator(
                                        task_id = 'GL_DAILY_MSB_STG_SEC_DESC',
                                        ssh_conn_id = 'af_ssh_conn',
                                        command=f"bash {gl_path}msb_stg_sec_desc.sh {business_date} {env} MSB {client_nbr} {aws_region}"
                                     )

        Start_Stg >> AR_S3_COPY_MSB >> GL_DAILY_TRUNC_STAGE_SEC_DESC >> GL_DAILY_MSB_STG_SEC_DESC >> End_Stg

        AR_S3_COPY_MSC = S3KeySensor(
                        task_id='AR_S3_COPY_MSC',
                        bucket_key='pr_ubsfi/incoming/today/msa/*',
                        wildcard_match=True,
                        bucket_name=f{af_s3_bucket_nm},
                        timeout=18*60*60,
                        poke_interval=30,
                        dag=dag
                        )

        GL_DAILY_TRUNC_STAGE_SEC_REF = SSHOperator(
                                        task_id = 'GL_DAILY_TRUNC_STAGE_SEC_REF',
                                        ssh_conn_id = 'af_ssh_conn',
                                        command=f"bash {gl_path}util_db_truncate.sh STG_SEC_REF {env} {aws_region}"
                                     )

        GL_DAILY_MSC_STG_SEC_REF = SSHOperator(
                                        task_id = 'GL_DAILY_MSC_STG_SEC_REF',
                                        ssh_conn_id = 'af_ssh_conn',
                                        command=f"bash {gl_path}msc_stg_sec_ref.sh {business_date} {env} MSC {client_nbr} {aws_region}"
                                     )

        Start_Stg >> AR_S3_COPY_MSC >> GL_DAILY_TRUNC_STAGE_SEC_REF >> GL_DAILY_MSC_STG_SEC_REF >> End_Stg

    PRE_STAGING_CHECKPOINT = DummyOperator(task_id="PRE_STAGING_CHECKPOINT",trigger_rule='one_success')
    
    PRLITE_CTERA_SECURITY_STAGING = DummyOperator(task_id="PRLITE_CTERA_SECURITY_STAGING",trigger_rule='one_success')
    
    PRLITE_CTERA_TRANSACTION_STAGING = DummyOperator(task_id="PRLITE_CTERA_TRANSACTION_STAGING",trigger_rule='one_success')



    GL_DAILY_SEC_DTL_INSTR_REF = SSHOperator(
                                        task_id = 'GL_DAILY_SEC_DTL_INSTR_REF',
                                        ssh_conn_id = 'af_ssh_conn',
                                        command=f"bash {gl_path}sec_dtl_instrument_ref.sh {batch_date} {env} {aws_region}"
                                    )

    GL_DAILY_UCA_EXCHG_DIM_INSTR = SSHOperator(
                                        task_id = 'GL_DAILY_UCA_EXCHG_DIM_INSTR',
                                        ssh_conn_id = 'af_ssh_conn',
                                        command=f"bash {gl_path}uca_fact_price.sh {batch_date} {env} {aws_region}"
                                    )

    GL_DAILY_SEC_DIM_INSTR = SSHOperator(
                                        task_id = 'GL_DAILY_SEC_DIM_INSTR',
                                        ssh_conn_id = 'af_ssh_conn',
                                        command=f"bash {gl_path}sec_dim_instrument.sh {batch_date} {env} {aws_region}",
                                        trigger_rule='one_success'
                                    )

    GL_DAILY_SEC_FACT_CLS_ENTITY = SSHOperator(
                                        task_id = 'GL_DAILY_SEC_FACT_CLS_ENTITY',
                                        ssh_conn_id = 'af_ssh_conn',
                                        command=f"bash {gl_path}sec_fact_class_entity.sh {batch_date} {env} {aws_region}",
                                        trigger_rule='none_failed'
                                    )
    
    GL_DAILY_AF_FACT_PRICE = SSHOperator(
                                        task_id = 'GL_DAILY_AF_FACT_PRICE',
                                        ssh_conn_id = 'af_ssh_conn',
                                        command=f"bash {gl_path}af_fact_price.sh {batch_date} {env} {aws_region}",
                                        trigger_rule='none_failed'
                                    )

    GL_DAILY_DIM_CORP_ACTION_DECL = SSHOperator(
                                        task_id = 'GL_DAILY_DIM_CORP_ACTION_DECL',
                                        ssh_conn_id = 'af_ssh_conn',
                                        command=f"bash {gl_path}msf_msk_dim_corp_action_decl.sh {batch_date} {env} {aws_region}",
                                        trigger_rule='none_failed'
                                    )

    GL_DAILY_PR_FACT_PRICE = SSHOperator(
                                        task_id = 'GL_DAILY_PR_FACT_PRICE',
                                        ssh_conn_id = 'af_ssh_conn',
                                        command=f"bash {gl_path}pr_fact_price.sh {batch_date} {env} {aws_region}",
                                        trigger_rule='none_failed'
                                    )

    GL_DAILY_SEC_INSTRU_OPTIONS = SSHOperator(
                                        task_id = 'GL_DAILY_SEC_INSTRU_OPTIONS',
                                        ssh_conn_id = 'af_ssh_conn',
                                        command=f"bash {gl_path}sec_dim_instrument_msl.sh {batch_date} {env} {aws_region}",
                                        trigger_rule='none_failed'
                                    )

    GL_DAILY_HPA_STUB_DIM_INSTRU = SSHOperator(
                                        task_id = 'GL_DAILY_HPA_STUB_DIM_INSTRU',
                                        ssh_conn_id = 'af_ssh_conn',
                                        command=f"bash {gl_path}hpa_security_stub.sh {batch_date} {env} {aws_region}",
                                        trigger_rule='none_failed'
                                    )

    AR_DAILY_VACUUM_STG_OTHER = SSHOperator(
                                        task_id = 'AR_DAILY_VACUUM_STG_OTHER',
                                        ssh_conn_id = 'af_ssh_conn',
                                        command=f"bash {ar_path}runner.sh {ar_usernm} {ldate}_{this_run} daily_db_maint_stg_othr.sh {firm_code}"
                                    )

    GL_DAILY_NRB_DIM_ACCOUNT_FACT_ACCOUNT_DATE = SSHOperator(
                                        task_id = 'GL_DAILY_NRB_DIM_ACCOUNT_FACT_ACCOUNT_DATE',
                                        ssh_conn_id = 'af_ssh_conn',
                                        command=f"bash {gl_path}nrb_dim_account_fact_account_date.sh {batch_date} {env} {aws_region}"
                                    )

    GL_DAILY_STUB_DIM_ACC = SSHOperator(
                                        task_id = 'GL_DAILY_STUB_DIM_ACC',
                                        ssh_conn_id = 'af_ssh_conn',
                                        command=f"bash {gl_path}stub_account_creation.sh {batch_date} {env} {aws_region}"
                                    )

    GL_DAILY_TRUNC_WRK_MULT_INSTRUMENT_FILTER = SSHOperator(
                                        task_id = 'GL_DAILY_TRUNC_WRK_MULT_INSTRUMENT_FILTER',
                                        ssh_conn_id = 'af_ssh_conn',
                                        command=f"bash {gl_path}util_db_truncate.sh WRK_MULT_INSTRUMENT_FILTER {env} {aws_region}",
                                        trigger_rule='one_success'
                                    )

    GL_DAILY_STUB_DIM_INSTR = SSHOperator(
                                        task_id = 'GL_DAILY_STUB_DIM_INSTR',
                                        ssh_conn_id = 'af_ssh_conn',
                                        command=f"bash {gl_path}stub_dim_instrument.sh WRK_MULT_INSTRUMENT_FILTER {env} {aws_region}"
                                    )

    AR_DAILY_VACUUM_STG_TRAILER = SSHOperator(
                                        task_id = 'AR_DAILY_VACUUM_STG_TRAILER',
                                        ssh_conn_id = 'af_ssh_conn',
                                        command=f"bash {ar_path}runner.sh {ar_usernm} {ldate}_{this_run} daily_db_maint_stg_trailer.sh {firm_code}"
                                    )

    GL_DAILY_QF_FACT_PRICE = SSHOperator(
                                        task_id = 'GL_DAILY_QF_FACT_PRICE',
                                        ssh_conn_id = 'af_ssh_conn',
                                        command=f"bash {gl_path}qf_fact_price.sh {batch_date} {env} {aws_region}"
                                    )

    GL_DAILY_SEC_DTL_FI_SCHEDULE = SSHOperator(
                                        task_id = 'GL_DAILY_SEC_DTL_FI_SCHEDULE',
                                        ssh_conn_id = 'af_ssh_conn',
                                        command=f"bash {gl_path}sec_dtl_fi_schedule.sh {batch_date} {env} {aws_region}"
                                    )

    AR_DAILY_BUILD_WRK_TABLE = SSHOperator(
                                        task_id = 'AR_DAILY_BUILD_WRK_TABLE',
                                        ssh_conn_id = 'af_ssh_conn',
                                        command=f"bash {ar_path}runner.sh {ar_usernm} {ldate}_{this_run} daily_build_wrk_tables.sh",
                                        trigger_rule='one_success'
                                    )

    end = DummyOperator(task_id="end")

    ######################################################
    # Create Job Dependencies
    ######################################################
    # initial jobs
    start >> BEGIN_NOTIFICATION >> AR_PRE_EXTRACT_CLEANUP >> AR_PRE_BATCH_BKP >> PRE_STAGING_CHECKPOINT 
    
    BEGIN_NOTIFICATION >> FILES_RETEN_LOGS_CLEANUP >> FILES_UNZIP >> PRE_STAGING_CHECKPOINT
    
    BEGIN_NOTIFICATION >> AR_CLEANUP_S3_DATA >> SECURITY_STAGING >> PRE_STAGING_CHECKPOINT

    PRE_STAGING_CHECKPOINT >> PRLITE_CTERA_SECURITY_STAGING >> GL_DAILY_SEC_DTL_INSTR_REF >> GL_DAILY_SEC_DIM_INSTR
    PRLITE_CTERA_SECURITY_STAGING >> GL_DAILY_UCA_EXCHG_DIM_INSTR >> GL_DAILY_SEC_DIM_INSTR
    
    GL_DAILY_SEC_DIM_INSTR >> GL_DAILY_SEC_FACT_CLS_ENTITY >> GL_DAILY_QF_FACT_PRICE >> AR_DAILY_BUILD_WRK_TABLE
    GL_DAILY_SEC_DIM_INSTR >> GL_DAILY_AF_FACT_PRICE >> AR_DAILY_BUILD_WRK_TABLE
    GL_DAILY_SEC_DIM_INSTR >> GL_DAILY_DIM_CORP_ACTION_DECL >> GL_DAILY_SEC_DTL_FI_SCHEDULE >> AR_DAILY_BUILD_WRK_TABLE
    GL_DAILY_SEC_DIM_INSTR >> GL_DAILY_PR_FACT_PRICE >> AR_DAILY_BUILD_WRK_TABLE
    GL_DAILY_SEC_DIM_INSTR >> GL_DAILY_SEC_INSTRU_OPTIONS >> GL_DAILY_TRUNC_WRK_MULT_INSTRUMENT_FILTER >> GL_DAILY_STUB_DIM_INSTR >> AR_DAILY_BUILD_WRK_TABLE
    GL_DAILY_SEC_DIM_INSTR >> GL_DAILY_HPA_STUB_DIM_INSTRU >> GL_DAILY_TRUNC_WRK_MULT_INSTRUMENT_FILTER
    
    PRE_STAGING_CHECKPOINT >> PRLITE_CTERA_TRANSACTION_STAGING >> AR_DAILY_VACUUM_STG_OTHER >> GL_DAILY_NRB_DIM_ACCOUNT_FACT_ACCOUNT_DATE >> GL_DAILY_STUB_DIM_ACC >> AR_DAILY_BUILD_WRK_TABLE
    PRE_STAGING_CHECKPOINT >> AR_DAILY_VACUUM_STG_TRAILER >> AR_DAILY_BUILD_WRK_TABLE >> end
