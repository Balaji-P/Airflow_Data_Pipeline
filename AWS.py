####Importing 
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from datetime import datetime
from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.decorators import task
import sys 
sys.path.append('/opt/airflow/includes')
#import queries
from emp_dim_insert_update import join_and_detect_new_or_changed_rows
from queries import INSERT_INTO_DWH_EMP_DIM
from queries import UPDATE_DWH_EMP_DIM

###This to check if data will be updated or not 
def check_update_result(**context):
    result = context['ti'].xcom_pull(task_ids='join_and_detect_new_or_changed_rows', key='ids_to_update')
    if len(result)>0:
        return 'snowflake_update'
    else:  
        return 'snowflake_insert_task'


###creating DAG  [get_from_finance , get_from_hr]
with DAG("Project" , start_date=datetime(2023,5,12), schedule='@yearly' , catchup=False) as Dag :
 
    get_from_finance = SqlToS3Operator(
        task_id="Finance",
        sql_conn_id="Postgres",
        aws_conn_id="AMAZON",
        query="select * from finance.emp_sal" , 
        s3_bucket="staging.emp.data",
        s3_key="habiba_emp_data.csv",
        replace=True)
        
    get_from_hr = SqlToS3Operator(
        task_id="Details",
        sql_conn_id="Postgres",
        aws_conn_id="AMAZON",
        query="select * from hr.emp_details" ,
        s3_bucket="staging.emp.data",
        s3_key="habiba_hr_sal.csv",
        replace=True)
        
    ######  creating variables
    Joined_tables=join_and_detect_new_or_changed_rows()
    ##check_if_updated=check_update_result()
    #######
    
    ###Branching_Operator
    what_task = BranchPythonOperator(
        task_id='what_task',
        python_callable=check_update_result
    )



    ##insertion to Target
    snowflake_insert_task = SnowflakeOperator(
        task_id='snowflake_insert_task',
        sql=INSERT_INTO_DWH_EMP_DIM('{{ ti.xcom_pull(task_ids="join_and_detect_new_or_changed_rows", key="rows_to_insert") }}'),
        snowflake_conn_id='Snowflake'
    )
    ##updating to Target
    snowflake_update = SnowflakeOperator(
        task_id='snowflake_update',
        sql=UPDATE_DWH_EMP_DIM('{{ ti.xcom_pull(task_ids="join_and_detect_new_or_changed_rows", key="ids_to_update") }}'),
        snowflake_conn_id='Snowflake'
    )
           
    ##Creating another insert with same code but differ id to check flow
    snowflake_insert2 = SnowflakeOperator(
        task_id='snowflake_insert2',
        sql=INSERT_INTO_DWH_EMP_DIM('{{ ti.xcom_pull(task_ids="join_and_detect_new_or_changed_rows", key="rows_to_insert") }}'),
        snowflake_conn_id='Snowflake'
    )
    
    ### Creating Tassk to make sure the DAG is ended
    Done = BashOperator(task_id='Done', bash_command='echo Done')
    
    [get_from_finance,get_from_hr ] >> Joined_tables >> what_task
    what_task >> snowflake_insert_task >> Done
    what_task >> snowflake_update >> snowflake_insert2 >> Done
     