from airflow import DAG 
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import time 
import boto3

SUBJECT = 'book'

def get_emr_client():
    return boto3.client('emr', region_name='us-east-1')

def _start_emr_cluster(**kwargs):
    emr_client = get_emr_client()

    response = emr_client.run_job_flow(
        Name=f'0002_creation_{SUBJECT}_gold',
        ReleaseLabel='emr-7.1.0',
        Instances={
            'InstanceGroups': [
                {
                    'Name': "MasterNode",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': 'm4.xlarge',
                    'InstanceCount': 1,
                }
            ],
            
        },
        Applications=[{'Name': 'Spark'}],
        LogUri='s3://nome_do_bucket/0005_logs/', 
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='EMR_DefaultRole'
    )

    cluster_id = response['JobFlowId']

    print(f"Cluster criado com sucesso: {cluster_id} .")

    kwargs['ti'].xcom_push(key='cluster_id', value=cluster_id)
    

def _add_step_job(**kwargs):
    emr_client = get_emr_client()

    cluster_id = kwargs['ti'].xcom_pull(key='cluster_id')
    script_path = 's3://nome_do_bucket/0004_codes/scripts_gold/cd_sales_book.py'

    response = emr_client.add_job_flow_steps(
        JobFlowId=cluster_id,
        Steps=[
            {
                'Name': f'creation_{SUBJECT}_gold',
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ['spark-submit', '--deploy-mode', 'client', script_path],
                }
            },
        ]
    )

    step_job_id = response['StepIds'][0]  

    kwargs['ti'].xcom_push(key='step_job_id', value=step_job_id)
    
def _wait_step_job(**kwargs):
    emr_client = get_emr_client()
    cluster_id = kwargs['ti'].xcom_pull(key='cluster_id')
    step_id = kwargs['ti'].xcom_pull(key='step_job_id')

    while True:
        response = emr_client.describe_step(ClusterId=cluster_id, StepId=step_id)
        state = response['Step']['Status']['State']

        if state in ['PENDING', 'RUNNING']:
            print(f'Executando step job...Estado: {state}')
            time.sleep(7)
        elif state == 'COMPLETED':
            print(f'Execução do step job finalizou!Estado: {state}')
            break
        else:
            raise Exception(f'O Step Job falhou com estado: {state}')
        
def _terminate_emr_cluster(**kwargs):
    emr_client = get_emr_client()
    cluster_id = kwargs['ti'].xcom_pull(key='cluster_id')
    emr_client.terminate_job_flows(JobFlowIds=[cluster_id])
    
with DAG(
    dag_id='creation_book_gold',
    tags=['dataLake_aws'],
    start_date=datetime(2024,4,22),
    schedule_interval=None,
    catchup=False
) as dag:
    
    start_execution = DummyOperator(
        task_id='start_execution'
    )

    start_emr_cluster = PythonOperator(
        task_id='start_emr_cluster',
        python_callable=_start_emr_cluster
    )

    add_step_job = PythonOperator(
        task_id='add_step_job',
        python_callable=_add_step_job
    )

    wait_step_job = PythonOperator(
        task_id='wait_step_job',
        python_callable=_wait_step_job
    )

    terminate_emr_cluster = PythonOperator(
        task_id='terminate_emr_cluster',
        python_callable=_terminate_emr_cluster
    )
       
    finish_execution = DummyOperator(
        task_id='finish_execution'
    )

    start_execution >> start_emr_cluster >> add_step_job >> wait_step_job >>  terminate_emr_cluster >> finish_execution