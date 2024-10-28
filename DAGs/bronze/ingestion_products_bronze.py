from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from sqlalchemy import create_engine
from configparser import ConfigParser
import pandas as pd 
import boto3
import os

SUBJECT = 'products'
REF = datetime.now().strftime('%Y%m%d')
TS_PROC = datetime.now().strftime('%Y%m%d%H%M%S')

# Acessando o banco de dados local, nesse caso o mysql
def _create_engine():
	config = ConfigParser()
	config_dir = os.getcwd() + '/config/config.cfg'
	config.read(config_dir)
	user = config.get('DB', 'user')
	password = config.get('DB', 'password')
	host = config.get('DB', 'host')
	database = config.get('DB', 'database')
	
	connection_string = f'mysql+mysqlconnector://{user}:{password}@{host}/{database}'
	
	engine = create_engine(connection_string)
	
	return engine.connect()

# extraindo a tabela da variável SUBJECT, nesse caso 'products'
def _extract_data(**kwargs):
	filename = f'{SUBJECT}_{REF}_{TS_PROC}'
	query = f'SELECT * FROM {SUBJECT}'
	df = pd.read_sql(query, _create_engine())
	df.to_csv(f'/tmp/{filename}.csv', index= False)
	
	kwargs['ti'].xcom_push(key='filename', value=filename)

# acessando o S3 e salvando a tabela extraída no formato csv	
def _ingest_data_to_s3(**kwargs):
	# Ler credenciais da AWS do arquivo de configuração
	config = ConfigParser()
	config.read(os.path.join(os.getcwd(), 'config', 'config.cfg'))
	aws_access_key = config.get('AWS', 'access_key')
	aws_secret_key = config.get('AWS', 'secret_key')

	# Configurar cliente S3 com as credenciais
	s3_client = boto3.client(
		's3',
		aws_access_key_id=aws_access_key,
		aws_secret_access_key=aws_secret_key
	)

	filename = kwargs['ti'].xcom_pull(key='filename')
	bucket_name = 'nome_do__bucket'
	prefix = f'0000_bronze/{SUBJECT}/{filename}.csv'
	try:
		with open(f'/tmp/{filename}.csv', 'rb') as f:
			s3_client.put_object(Bucket=bucket_name, Key=prefix, Body=f)
		print("File uploaded successfully.")
	except Exception as e:
		print(f'Erro!{e}')
		
with DAG(
	dag_id='ingestion_products_bronze',
	tags=['dataLake_aws'],
	start_date=datetime(2024,4,22),
	schedule_interval=None,
	catchup=False
) as dag:
	start_execution = DummyOperator(
		task_id='start_execution'
	)

	extract_db_data = PythonOperator(
		task_id='extract_db_data',
		python_callable=_extract_data
	)

	ingest_data_to_s3 = PythonOperator(
		task_id='ingest_data_to_s3',
		python_callable=_ingest_data_to_s3
	)
	 
	finish_execution = DummyOperator(
		task_id='finish_execution'
	)
	 
	start_execution >> extract_db_data >> ingest_data_to_s3 >> finish_execution