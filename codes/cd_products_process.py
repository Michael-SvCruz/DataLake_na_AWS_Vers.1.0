from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, split, col, lit, regexp_replace
from datetime import datetime, timedelta
import boto3

spark = SparkSession.builder.getOrCreate()

ts_proc = datetime.now().strftime('%Y%m%d%H%M%S')

print(f">>>> Leitura Base de Produtos: {datetime.now().strftime('%Y%m%d%H%M%S')}")

# Caminho do bucket S3
bucket_name = 'nome_do_bucket'
prefix = '0000_bronze/products/'

s3 = boto3.client('s3')

# Listar os arquivos no diretório
response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

# Esse trecho é responsável por pegar o arquivo .csv mais recente da camada bronze e armazenar na variável 'latest_file_path'
if 'Contents' not in response:
    print("Nenhum arquivo encontrado no diretório especificado.")
else:
    # Filtrar os arquivos que seguem o padrão 'products_' e terminam com '.csv'
    files = [obj for obj in response['Contents'] if 'products_' in obj['Key'] and obj['Key'].endswith('.csv')]
    print("Arquivos filtrados:", [f['Key'] for f in files])

    # Ordenar os arquivos pela data de modificação (LastModified)
    files.sort(key=lambda x: x['LastModified'], reverse=True)
    print("Arquivos ordenados por LastModified:", [f['Key'] for f in files])

    latest_file = files[0]['Key']
    print("Arquivo mais recente:", latest_file)

    latest_file_path = f's3://{bucket_name}/{latest_file}'

t_products = spark.read.csv(latest_file_path, header=True) 

# algumas colunas são criadas para auxílio no processo e no monitoramento
t_products_00 = t_products.withColumn('filename', split(input_file_name(), '/')[5])
t_products_01 = t_products_00.withColumn('ref', split(col('filename'),'_')[1])
t_products_02 = t_products_01.withColumn('ts_file_generation', regexp_replace((split(col('filename'),'_')[2]),'.csv',''))
t_products_03 = t_products_02.withColumn('ts_proc', lit(ts_proc))

t_products_03.createOrReplaceTempView('t_products_03')
t_products_03.cache()
t_products_03.count()

t_products_03.printSchema()

t_products_04 = spark.sql("""
    select
        cast(ref as string) as ref,
        cast(ts_file_generation as string) as ts_file_generation,
        cast(ts_proc as string) as ts_proc,
        cast(product_id as string) as product_id,
        cast(product_des as string) as product_des,
        cast(product_category as string) as product_category
    from t_products_03
""")
t_products_04.createOrReplaceTempView('t_products_04')

t_products_04.show(1)

print(f">>>> Escrita Base de produtos: {datetime.now().strftime('%Y%m%d%H%M%S')}")
# arquivo de controle salvo no formato parquet, particinado em duas camadas para melhor desempenho de busca,
#nesse caso como a tabela é pequeno não influencia mas para tabelas maiores como sales irá influenciar.
t_products_04.write.partitionBy('ref','ts_proc').parquet('s3://nome_do_bucket/0001_silver/products/',mode='append')

qtd_registros_products = t_products_04.count()

# criação de uma tabela de controle
t_products_ctl = spark.sql(f"""
    select
        'products' as assunto,
        ref as ref,
        ts_file_generation as ts_file_generation,
        {ts_proc} as ts_proc,
        {qtd_registros_products} as qtd_registros
    from t_products_04
    limit 1
""")
t_products_ctl.createOrReplaceTempView('t_products_ctl')
t_products_ctl.show()

print(f">>>> Escrita Controle Base de produtos: {datetime.now().strftime('%Y%m%d%H%M%S')}")
# arquivo de controle salvo no formato parquet.
t_products_ctl.write.partitionBy('ref').parquet('s3://nome_do_bucket/0003_controle/products/',mode='append')