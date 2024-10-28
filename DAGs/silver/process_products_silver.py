from airflow import DAG 
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import time 
import boto3

SUBJECT = 'products'

def get_emr_client():
    return boto3.client('emr', region_name='us-east-1')

# Criação do cluster EMR
def _start_emr_cluster(**kwargs):
    emr_client = get_emr_client()

    response = emr_client.run_job_flow(
        Name=f'0001_process_{SUBJECT}_silver',
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
        ServiceRole='EMR_DefaultRole',
        BootstrapActions=[  # Adicionando o script de bootstrap para instalação do boto3
            {
                'Name': 'Install boto3',
                'ScriptBootstrapAction': {
                    'Path': 's3://nome_do_bucket/0004_codes/scripts_bash/install_boto3.sh'
                }
            }
        ]
    )

    cluster_id = response['JobFlowId']

    print(f"Cluster criado com sucesso: {cluster_id} .")

    kwargs['ti'].xcom_push(key='cluster_id', value=cluster_id)
    

# adiciona um step job ao cluster, que é responsável pelo processo do script da camada silver
def _add_step_job(**kwargs):
    emr_client = get_emr_client()

    cluster_id = kwargs['ti'].xcom_pull(key='cluster_id')
    script_path = 's3://nome_do_bucket/0004_codes/scripts_silver/cd_products_process.py'

    response = emr_client.add_job_flow_steps(
        JobFlowId=cluster_id,
        Steps=[
            {
                'Name': f'process_{SUBJECT}_silver',
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

# etapa que monitora e aguarda a finalização do step job    
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

# finalização do Cluster        
def _terminate_emr_cluster(**kwargs):
    emr_client = get_emr_client()
    cluster_id = kwargs['ti'].xcom_pull(key='cluster_id')
    emr_client.terminate_job_flows(JobFlowIds=[cluster_id])
    
with DAG(
    dag_id='process_products_silver',
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
