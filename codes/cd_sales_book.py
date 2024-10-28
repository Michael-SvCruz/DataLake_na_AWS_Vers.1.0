# critério do book de variáveis
# Quantidade de compras (por categoria, por metodo pagamento e total)
# valor em compras (por categoria, por metodo pagamento e total)
# faixas (1m, 3m, 6m, 12m)


from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, split, col, lit, regexp_replace
from datetime import datetime, timedelta

spark = SparkSession.builder.getOrCreate()

# cria uma variável com date time da data de processamento da execução do book
dt_exec_book = datetime.now().date().strftime("%Y%m%d")

# cria uma variável com timeStamp da data de processamento
ts_proc = datetime.now().strftime('%Y%m%d%H%M%S')

# cria uma variável com a data inicial de referência, baseado no tempo máximo escolhido das faixas
ref_ini = (datetime.now().date() - timedelta(days=365)).strftime("%Y%m%d")
ref_ini = ref_ini[0:6] # forma para resgatar o mês completo

print(f">>>> Leitura Base de Vendas: {datetime.now().strftime('%Y%m%d%H%M%S')}")
file = "s3://nome_do_bucket/0001_silver/sales/"
t_sales = spark.read.parquet(file)
t_sales_00 = t_sales.where(f'ref >= {ref_ini}')
t_sales_00.createOrReplaceTempView('t_sales_00')

# contagem do número de registros de t_sales_00
t_sales_00.count()


print(f">>>> Deduplicacao de Vendas: {datetime.now().strftime('%Y%m%d%H%M%S')}")
#cria uma TempView com base em t_sales_00, acrescentando uma coluna dedup_key que concatena
# ts_proc com ts_file_generation

# Deduplicacao
t_sales_dedup_00 = spark.sql("""
    select
        concat(ts_proc, ts_file_generation) as dedup_key,
        *
    from t_sales_00
""")
t_sales_dedup_00.createOrReplaceTempView('t_sales_dedup_00')

# conta os números de ocorrências de t_sales_dedup_00, agrupando por dedup_key
spark.sql("""
    select
    dedup_key,
    count(*)
    from t_sales_dedup_00
    group by 1
""").show(truncate=False)

# Cria uma TempView com o registro que contenha o max de dedup_key
t_sales_dedup_01 = spark.sql("""
    select
        max(dedup_key) as max_dedup_key
    from t_sales_dedup_00
""")
t_sales_dedup_01.createOrReplaceTempView('t_sales_dedup_01')

# cria uma TempView com todos os valores de t_sales_dedup_00 onde dedup_key é igual
# ao max_dedup_key da tabela t_sales_dedup_01
t_sales_dedup_02 = spark.sql("""
    select
        a.*
    from t_sales_dedup_00 a
    inner join t_sales_dedup_01 b
    on a.dedup_key = b.max_dedup_key
""")
t_sales_dedup_02.show(5)
t_sales_dedup_02.createOrReplaceTempView('t_sales_dedup_02')

t_sales_dedup_02.count()

print(f">>>> Leitura Base de Produtos: {datetime.now().strftime('%Y%m%d%H%M%S')}")
t_products = spark.read.parquet('s3://nome_do_bucket/0001_silver/products/')
t_products.createOrReplaceTempView('t_products')

print(f">>>> Deduplicação de Produtos: {datetime.now().strftime('%Y%m%d%H%M%S')}")

# acrescenta a concatenção de ts_file_generation e ts_proc como dedup_key
# criando a TempView t_products_dedup_00
t_products_dedup_00 = spark.sql("""
    select
        concat(ts_file_generation, ts_proc) dedup_key,
        *
    from t_products
""")
t_products_dedup_00.createOrReplaceTempView('t_products_dedup_00')

# cria uma TempView com o max valor de dedup_key
t_products_dedup_01 = spark.sql("""
    select
        max(dedup_key) max_dedup_key
    from t_products_dedup_00
""")
t_products_dedup_01.createOrReplaceTempView('t_products_dedup_01')
t_products_dedup_01.show(truncate=False)

# cria uma tempView baseada em t_products_dedup_00 trazendo somente os valores que a.dedup_key = b.max_dedup_key
t_products_dedup_02 = spark.sql("""
    select
        a.*
    from t_products_dedup_00 a
    inner join t_products_dedup_01 b
    on a.dedup_key = b.max_dedup_key
""")
t_products_dedup_02.createOrReplaceTempView('t_products_dedup_02')
t_products_dedup_02.count()

t_products_dedup_02.show(1)

print(f">>>> Data Prep: {datetime.now().strftime('%Y%m%d%H%M%S')}")

# Através de join uni product_category à tabela de sales
t_sales_products_join = spark.sql("""
    select
        a.*,
        b.product_category
    from t_sales_dedup_02 a
    left join t_products_dedup_02 b
    on a.product_id = b.product_id
""")
t_sales_products_join.createOrReplaceTempView('t_sales_products_join')

t_sales_products_join.show(5)

print(f">>>> Criação de Flags Meses: {datetime.now().strftime('%Y%m%d%H%M%S')}")
#lembrando que as flags de tempo são de 1m, 3m , 6m e 12m
month_flags = spark.sql(f"""
    select
        transaction_id,
        user_id,
        ref,
        product_category,
        dat_creation,
        amount,
        payment_method,
        (case when int(months_between(to_date('{dt_exec_book}','yyyyMMdd'), to_date(CAST(ref AS STRING), 'yyyyMMdd'))) <= 1 then 1 else 0 end) flag_u1m,
        (case when int(months_between(to_date('{dt_exec_book}','yyyyMMdd'), to_date(CAST(ref AS STRING), 'yyyyMMdd'))) <= 3 then 1 else 0 end) flag_u3m,
        (case when int(months_between(to_date('{dt_exec_book}','yyyyMMdd'), to_date(CAST(ref AS STRING), 'yyyyMMdd'))) <= 6 then 1 else 0 end) flag_u6m,
        (case when int(months_between(to_date('{dt_exec_book}','yyyyMMdd'), to_date(CAST(ref AS STRING), 'yyyyMMdd'))) <= 12 then 1 else 0 end) flag_u12m
    from t_sales_products_join
""")
month_flags.createOrReplaceTempView('month_flags')

month_flags.show(2)

print(f">>>> Criação do Book Vars: {datetime.now().strftime('%Y%m%d%H%M%S')}")

bk_00 = spark.sql(f"""
    select
        user_id,
        '{dt_exec_book}' ref,
        '{ts_proc}' ts_proc,
        sum(case when product_category = 'Livros' then 1 end) qtd_pcat_livros,
        sum(case when product_category = 'Roupas' then 1 end) qtd_pcat_roupas,
        sum(case when product_category = 'Eletronicos' then 1 end) qtd_pcat_eletr,
        sum(case when product_category = 'Jogos' then 1 end) qtd_pcat_jogos,
        
        sum(case when flag_u1m = 1 and product_category = 'Livros' then 1 end) qtd_pcat_livros_u1m,
        sum(case when flag_u1m = 1 and product_category = 'Roupas' then 1 end) qtd_pcat_roupas_u1m,
        sum(case when flag_u1m = 1 and product_category = 'Eletronicos' then 1 end) qtd_pcat_eletr_u1m,
        sum(case when flag_u1m = 1 and product_category = 'Jogos' then 1 end) qtd_pcat_jogos_u1m,
        
        sum(case when flag_u3m = 1 and product_category = 'Livros' then 1 end) qtd_pcat_livros_u3m,
        sum(case when flag_u3m = 1 and product_category = 'Roupas' then 1 end) qtd_pcat_roupas_u3m,
        sum(case when flag_u3m = 1 and product_category = 'Eletronicos' then 1 end) qtd_pcat_eletr_u3m,
        sum(case when flag_u3m = 1 and product_category = 'Jogos' then 1 end) qtd_pcat_jogos_u3m,
        
        sum(case when flag_u6m = 1 and product_category = 'Livros' then 1 end) qtd_pcat_livros_u6m,
        sum(case when flag_u6m = 1 and product_category = 'Roupas' then 1 end) qtd_pcat_roupas_u6m,
        sum(case when flag_u6m = 1 and product_category = 'Eletronicos' then 1 end) qtd_pcat_eletr_u6m,
        sum(case when flag_u6m = 1 and product_category = 'Jogos' then 1 end) qtd_pcat_jogos_u6m,
        
        sum(case when flag_u6m = 1 and product_category = 'Livros' then 1 end) qtd_pcat_livros_u12m,
        sum(case when flag_u6m = 1 and product_category = 'Roupas' then 1 end) qtd_pcat_roupas_u12m,
        sum(case when flag_u6m = 1 and product_category = 'Eletronicos' then 1 end) qtd_pcat_eletr_u12m,
        sum(case when flag_u6m = 1 and product_category = 'Jogos' then 1 end) qtd_pcat_jogos_u12m,
        
        sum(case when product_category = 'Livros' then amount end) sum_pcat_livros,
        sum(case when product_category = 'Roupas' then amount end) sum_pcat_roupas,
        sum(case when product_category = 'Eletronicos' then amount end) sum_pcat_eletr,
        sum(case when product_category = 'Jogos' then amount end) sum_pcat_jogos,
        
        sum(case when flag_u1m = 1 and product_category = 'Livros' then amount end) sum_pcat_livros_u1m,
        sum(case when flag_u1m = 1 and product_category = 'Roupas' then amount end) sum_pcat_roupas_u1m,
        sum(case when flag_u1m = 1 and product_category = 'Eletronicos' then amount end) sum_pcat_eletr_u1m,
        sum(case when flag_u1m = 1 and product_category = 'Jogos' then amount end) sum_pcat_jogos_u1m,
        
        sum(case when flag_u3m = 1 and product_category = 'Livros' then amount end) sum_pcat_livros_u3m,
        sum(case when flag_u3m = 1 and product_category = 'Roupas' then amount end) sum_pcat_roupas_u3m,
        sum(case when flag_u3m = 1 and product_category = 'Eletronicos' then amount end) sum_pcat_eletr_u3m,
        sum(case when flag_u3m = 1 and product_category = 'Jogos' then amount end) sum_pcat_jogos_u3m,
        
        sum(case when flag_u6m = 1 and product_category = 'Livros' then amount end) sum_pcat_livros_u6m,
        sum(case when flag_u6m = 1 and product_category = 'Roupas' then amount end) sum_pcat_roupas_u6m,
        sum(case when flag_u6m = 1 and product_category = 'Eletronicos' then amount end) sum_pcat_eletr_u6m,
        sum(case when flag_u6m = 1 and product_category = 'Jogos' then amount end) sum_pcat_jogos_u6m,
        
        sum(case when flag_u6m = 1 and product_category = 'Livros' then amount end) sum_pcat_livros_u12m,
        sum(case when flag_u6m = 1 and product_category = 'Roupas' then amount end) sum_pcat_roupas_u12m,
        sum(case when flag_u6m = 1 and product_category = 'Eletronicos' then amount end) sum_pcat_eletr_u12m,
        sum(case when flag_u6m = 1 and product_category = 'Jogos' then amount end) sum_pcat_jogos_u12m,
        
        avg(case when product_category = 'Livros' then amount end) avg_pcat_livros,
        avg(case when product_category = 'Roupas' then amount end) avg_pcat_roupas,
        avg(case when product_category = 'Eletronicos' then amount end) avg_pcat_eletr,
        avg(case when product_category = 'Jogos' then amount end) avg_pcat_jogos,
        
        avg(case when flag_u1m = 1 and product_category = 'Livros' then amount end) avg_pcat_livros_u1m,
        avg(case when flag_u1m = 1 and product_category = 'Roupas' then amount end) avg_pcat_roupas_u1m,
        avg(case when flag_u1m = 1 and product_category = 'Eletronicos' then amount end) avg_pcat_eletr_u1m,
        avg(case when flag_u1m = 1 and product_category = 'Jogos' then amount end) avg_pcat_jogos_u1m,
        
        avg(case when flag_u3m = 1 and product_category = 'Livros' then amount end) avg_pcat_livros_u3m,
        avg(case when flag_u3m = 1 and product_category = 'Roupas' then amount end) avg_pcat_roupas_u3m,
        avg(case when flag_u3m = 1 and product_category = 'Eletronicos' then amount end) avg_pcat_eletr_u3m,
        avg(case when flag_u3m = 1 and product_category = 'Jogos' then amount end) avg_pcat_jogos_u3m,
        
        avg(case when flag_u6m = 1 and product_category = 'Livros' then amount end) avg_pcat_livros_u6m,
        avg(case when flag_u6m = 1 and product_category = 'Roupas' then amount end) avg_pcat_roupas_u6m,
        avg(case when flag_u6m = 1 and product_category = 'Eletronicos' then amount end) avg_pcat_eletr_u6m,
        avg(case when flag_u6m = 1 and product_category = 'Jogos' then amount end) avg_pcat_jogos_u6m,
        
        avg(case when flag_u12m = 1 and product_category = 'Livros' then amount end) avg_pcat_livros_u12m,
        avg(case when flag_u12m = 1 and product_category = 'Roupas' then amount end) avg_pcat_roupas_u12m,
        avg(case when flag_u12m = 1 and product_category = 'Eletronicos' then amount end) avg_pcat_eletr_u12m,
        avg(case when flag_u12m = 1 and product_category = 'Jogos' then amount end) avg_pcat_jogos_u12m,
        
        min(case when product_category = 'Livros' then amount end) min_pcat_livros,
        min(case when product_category = 'Roupas' then amount end) min_pcat_roupas,
        min(case when product_category = 'Eletronicos' then amount end) min_pcat_eletr,
        min(case when product_category = 'Jogos' then amount end) min_pcat_jogos,
        
        min(case when flag_u1m = 1 and product_category = 'Livros' then amount end) min_pcat_livros_u1m,
        min(case when flag_u1m = 1 and product_category = 'Roupas' then amount end) min_pcat_roupas_u1m,
        min(case when flag_u1m = 1 and product_category = 'Eletronicos' then amount end) min_pcat_eletr_u1m,
        min(case when flag_u1m = 1 and product_category = 'Jogos' then amount end) min_pcat_jogos_u1m,
        
        min(case when flag_u3m = 1 and product_category = 'Livros' then amount end) min_pcat_livros_u3m,
        min(case when flag_u3m = 1 and product_category = 'Roupas' then amount end) min_pcat_roupas_u3m,
        min(case when flag_u3m = 1 and product_category = 'Eletronicos' then amount end) min_pcat_eletr_u3m,
        min(case when flag_u3m = 1 and product_category = 'Jogos' then amount end) min_pcat_jogos_u3m,
        
        min(case when flag_u6m = 1 and product_category = 'Livros' then amount end) min_pcat_livros_u6m,
        min(case when flag_u6m = 1 and product_category = 'Roupas' then amount end) min_pcat_roupas_u6m,
        min(case when flag_u6m = 1 and product_category = 'Eletronicos' then amount end) min_pcat_eletr_u6m,
        min(case when flag_u6m = 1 and product_category = 'Jogos' then amount end) min_pcat_jogos_u6m,
        
        min(case when flag_u12m = 1 and product_category = 'Livros' then amount end) min_pcat_livros_u12m,
        min(case when flag_u12m = 1 and product_category = 'Roupas' then amount end) min_pcat_roupas_u12m,
        min(case when flag_u12m = 1 and product_category = 'Eletronicos' then amount end) min_pcat_eletr_u12m,
        min(case when flag_u12m = 1 and product_category = 'Jogos' then amount end) min_pcat_jogos_u12m,
        
        max(case when product_category = 'Livros' then amount end) max_pcat_livros,
        max(case when product_category = 'Roupas' then amount end) max_pcat_roupas,
        max(case when product_category = 'Eletronicos' then amount end) max_pcat_eletr,
        max(case when product_category = 'Jogos' then amount end) max_pcat_jogos,
        
        max(case when flag_u1m = 1 and product_category = 'Livros' then amount end) max_pcat_livros_u1m,
        max(case when flag_u1m = 1 and product_category = 'Roupas' then amount end) max_pcat_roupas_u1m,
        max(case when flag_u1m = 1 and product_category = 'Eletronicos' then amount end) max_pcat_eletr_u1m,
        max(case when flag_u1m = 1 and product_category = 'Jogos' then amount end) max_pcat_jogos_u1m,
        
        max(case when flag_u3m = 1 and product_category = 'Livros' then amount end) max_pcat_livros_u3m,
        max(case when flag_u3m = 1 and product_category = 'Roupas' then amount end) max_pcat_roupas_u3m,
        max(case when flag_u3m = 1 and product_category = 'Eletronicos' then amount end) max_pcat_eletr_u3m,
        max(case when flag_u3m = 1 and product_category = 'Jogos' then amount end) max_pcat_jogos_u3m,
        
        max(case when flag_u6m = 1 and product_category = 'Livros' then amount end) max_pcat_livros_u6m,
        max(case when flag_u6m = 1 and product_category = 'Roupas' then amount end) max_pcat_roupas_u6m,
        max(case when flag_u6m = 1 and product_category = 'Eletronicos' then amount end) max_pcat_eletr_u6m,
        max(case when flag_u6m = 1 and product_category = 'Jogos' then amount end) max_pcat_jogos_u6m,
        
        max(case when flag_u12m = 1 and product_category = 'Livros' then amount end) max_pcat_livros_u12m,
        max(case when flag_u12m = 1 and product_category = 'Roupas' then amount end) max_pcat_roupas_u12m,
        max(case when flag_u12m = 1 and product_category = 'Eletronicos' then amount end) max_pcat_eletr_u12m,
        max(case when flag_u12m = 1 and product_category = 'Jogos' then amount end) max_pcat_jogos_u12m,
        
        sum(case when payment_method = 'Cartao Debito' then 1 end) qtd_pm_cd,
        sum(case when payment_method = 'PIX' then 1 end) qtd_pm_pix,
        sum(case when payment_method = 'Cartao Credito' then 1 end) qtd_pm_cc,
        sum(case when payment_method = 'Boleto' then 1 end) qtd_pm_boleto,
        
        sum(case when flag_u1m = 1 and payment_method = 'Cartao Debito' then 1 end) qtd_pm_cd_u1m,
        sum(case when flag_u1m = 1 and payment_method = 'PIX' then 1 end) qtd_pm_pix_u1m,
        sum(case when flag_u1m = 1 and payment_method = 'Cartao Credito' then 1 end) qtd_pm_cc_u1m,
        sum(case when flag_u1m = 1 and payment_method = 'Boleto' then 1 end) qtd_pm_boleto_u1m,
        
        sum(case when flag_u3m = 1 and payment_method = 'Cartao Debito' then 1 end) qtd_pm_cd_u3m,
        sum(case when flag_u3m = 1 and payment_method = 'PIX' then 1 end) qtd_pm_pix_u3m,
        sum(case when flag_u3m = 1 and payment_method = 'Cartao Credito' then 1 end) qtd_pm_cc_u3m,
        sum(case when flag_u3m = 1 and payment_method = 'Boleto' then 1 end) qtd_pm_boleto_u3m,
        
        sum(case when flag_u6m = 1 and payment_method = 'Cartao Debito' then 1 end) qtd_pm_cd_u6m,
        sum(case when flag_u6m = 1 and payment_method = 'PIX' then 1 end) qtd_pm_pix_u6m,
        sum(case when flag_u6m = 1 and payment_method = 'Cartao Credito' then 1 end) qtd_pm_cc_u6m,
        sum(case when flag_u6m = 1 and payment_method = 'Boleto' then 1 end) qtd_pm_boleto_u6m,
        
        sum(case when flag_u12m = 1 and payment_method = 'Cartao Debito' then 1 end) qtd_pm_cd_u12m,
        sum(case when flag_u12m = 1 and payment_method = 'PIX' then 1 end) qtd_pm_pix_u12m,
        sum(case when flag_u12m = 1 and payment_method = 'Cartao Credito' then 1 end) qtd_pm_cc_u12m,
        sum(case when flag_u12m = 1 and payment_method = 'Boleto' then 1 end) qtd_pm_boleto_u12m,
        
        sum(case when payment_method = 'Cartao Debito' then amount end) sum_pm_cd,
        sum(case when payment_method = 'PIX' then amount end) sum_pm_pix,
        sum(case when payment_method = 'Cartao Credito' then amount end) sum_pm_cc,
        sum(case when payment_method = 'Boleto' then amount end) sum_pm_boleto,
        
        sum(case when flag_u1m = 1 and payment_method = 'Cartao Debito' then amount end) sum_pm_cd_u1m,
        sum(case when flag_u1m = 1 and payment_method = 'PIX' then amount end) sum_pm_pix_u1m,
        sum(case when flag_u1m = 1 and payment_method = 'Cartao Credito' then amount end) sum_pm_cc_u1m,
        sum(case when flag_u1m = 1 and payment_method = 'Boleto' then amount end) sum_pm_boleto_u1m,
        
        sum(case when flag_u3m = 1 and payment_method = 'Cartao Debito' then amount end) sum_pm_cd_u3m,
        sum(case when flag_u3m = 1 and payment_method = 'PIX' then amount end) sum_pm_pix_u3m,
        sum(case when flag_u3m = 1 and payment_method = 'Cartao Credito' then amount end) sum_pm_cc_u3m,
        sum(case when flag_u3m = 1 and payment_method = 'Boleto' then amount end) sum_pm_boleto_u3m,
        
        sum(case when flag_u6m = 1 and payment_method = 'Cartao Debito' then amount end) sum_pm_cd_u6m,
        sum(case when flag_u6m = 1 and payment_method = 'PIX' then amount end) sum_pm_pix_u6m,
        sum(case when flag_u6m = 1 and payment_method = 'Cartao Credito' then amount end) sum_pm_cc_u6m,
        sum(case when flag_u6m = 1 and payment_method = 'Boleto' then amount end) sum_pm_boleto_u6m,
        
        sum(case when flag_u12m = 1 and payment_method = 'Cartao Debito' then amount end) sum_pm_cd_u12m,
        sum(case when flag_u12m = 1 and payment_method = 'PIX' then amount end) sum_pm_pix_u12m,
        sum(case when flag_u12m = 1 and payment_method = 'Cartao Credito' then amount end) sum_pm_cc_u12m,
        sum(case when flag_u12m = 1 and payment_method = 'Boleto' then amount end) sum_pm_boleto_u12m,
        
        min(case when payment_method = 'Cartao Debito' then amount end) min_pm_cd,
        min(case when payment_method = 'PIX' then amount end) min_pm_pix,
        min(case when payment_method = 'Cartao Credito' then amount end) min_pm_cc,
        min(case when payment_method = 'Boleto' then amount end) min_pm_boleto,
        
        min(case when flag_u1m = 1 and payment_method = 'Cartao Debito' then amount end) min_pm_cd_u1m,
        min(case when flag_u1m = 1 and payment_method = 'PIX' then amount end) min_pm_pix_u1m,
        min(case when flag_u1m = 1 and payment_method = 'Cartao Credito' then amount end) min_pm_cc_u1m,
        min(case when flag_u1m = 1 and payment_method = 'Boleto' then amount end) min_pm_boleto_u1m,
        
        min(case when flag_u3m = 1 and payment_method = 'Cartao Debito' then amount end) min_pm_cd_u3m,
        min(case when flag_u3m = 1 and payment_method = 'PIX' then amount end) min_pm_pix_u3m,
        min(case when flag_u3m = 1 and payment_method = 'Cartao Credito' then amount end) min_pm_cc_u3m,
        min(case when flag_u3m = 1 and payment_method = 'Boleto' then amount end) min_pm_boleto_u3m,
        
        min(case when flag_u6m = 1 and payment_method = 'Cartao Debito' then amount end) min_pm_cd_u6m,
        min(case when flag_u6m = 1 and payment_method = 'PIX' then amount end) min_pm_pix_u6m,
        min(case when flag_u6m = 1 and payment_method = 'Cartao Credito' then amount end) min_pm_cc_u6m,
        min(case when flag_u6m = 1 and payment_method = 'Boleto' then amount end) min_pm_boleto_u6m,
        
        min(case when flag_u12m = 1 and payment_method = 'Cartao Debito' then amount end) min_pm_cd_u12m,
        min(case when flag_u12m = 1 and payment_method = 'PIX' then amount end) min_pm_pix_u12m,
        min(case when flag_u12m = 1 and payment_method = 'Cartao Credito' then amount end) min_pm_cc_u12m,
        min(case when flag_u12m = 1 and payment_method = 'Boleto' then amount end) min_pm_boleto_u12m,
        
        max(case when payment_method = 'Cartao Debito' then amount end) max_pm_cd,
        max(case when payment_method = 'PIX' then amount end) max_pm_pix,
        max(case when payment_method = 'Cartao Credito' then amount end) max_pm_cc,
        max(case when payment_method = 'Boleto' then amount end) max_pm_boleto,
        
        max(case when flag_u1m = 1 and payment_method = 'Cartao Debito' then amount end) max_pm_cd_u1m,
        max(case when flag_u1m = 1 and payment_method = 'PIX' then amount end) max_pm_pix_u1m,
        max(case when flag_u1m = 1 and payment_method = 'Cartao Credito' then amount end) max_pm_cc_u1m,
        max(case when flag_u1m = 1 and payment_method = 'Boleto' then amount end) max_pm_boleto_u1m,
        
        max(case when flag_u3m = 1 and payment_method = 'Cartao Debito' then amount end) max_pm_cd_u3m,
        max(case when flag_u3m = 1 and payment_method = 'PIX' then amount end) max_pm_pix_u3m,
        max(case when flag_u3m = 1 and payment_method = 'Cartao Credito' then amount end) max_pm_cc_u3m,
        max(case when flag_u3m = 1 and payment_method = 'Boleto' then amount end) max_pm_boleto_u3m,
        
        max(case when flag_u6m = 1 and payment_method = 'Cartao Debito' then amount end) max_pm_cd_u6m,
        max(case when flag_u6m = 1 and payment_method = 'PIX' then amount end) max_pm_pix_u6m,
        max(case when flag_u6m = 1 and payment_method = 'Cartao Credito' then amount end) max_pm_cc_u6m,
        max(case when flag_u6m = 1 and payment_method = 'Boleto' then amount end) max_pm_boleto_u6m,
        
        max(case when flag_u12m = 1 and payment_method = 'Cartao Debito' then amount end) max_pm_cd_u12m,
        max(case when flag_u12m = 1 and payment_method = 'PIX' then amount end) max_pm_pix_u12m,
        max(case when flag_u12m = 1 and payment_method = 'Cartao Credito' then amount end) max_pm_cc_u12m,
        max(case when flag_u12m = 1 and payment_method = 'Boleto' then amount end) max_pm_boleto_u12m,
        
        avg(case when payment_method = 'Cartao Debito' then amount end) avg_pm_cd,
        avg(case when payment_method = 'PIX' then amount end) avg_pm_pix,
        avg(case when payment_method = 'Cartao Credito' then amount end) avg_pm_cc,
        avg(case when payment_method = 'Boleto' then amount end) avg_pm_boleto,
        
        avg(case when flag_u1m = 1 and payment_method = 'Cartao Debito' then amount end) avg_pm_cd_u1m,
        avg(case when flag_u1m = 1 and payment_method = 'PIX' then amount end) avg_pm_pix_u1m,
        avg(case when flag_u1m = 1 and payment_method = 'Cartao Credito' then amount end) avg_pm_cc_u1m,
        avg(case when flag_u1m = 1 and payment_method = 'Boleto' then amount end) avg_pm_boleto_u1m,
        
        avg(case when flag_u3m = 1 and payment_method = 'Cartao Debito' then amount end) avg_pm_cd_u3m,
        avg(case when flag_u3m = 1 and payment_method = 'PIX' then amount end) avg_pm_pix_u3m,
        avg(case when flag_u3m = 1 and payment_method = 'Cartao Credito' then amount end) avg_pm_cc_u3m,
        avg(case when flag_u3m = 1 and payment_method = 'Boleto' then amount end) avg_pm_boleto_u3m,
        
        avg(case when flag_u6m = 1 and payment_method = 'Cartao Debito' then amount end) avg_pm_cd_u6m,
        avg(case when flag_u6m = 1 and payment_method = 'PIX' then amount end) avg_pm_pix_u6m,
        avg(case when flag_u6m = 1 and payment_method = 'Cartao Credito' then amount end) avg_pm_cc_u6m,
        avg(case when flag_u6m = 1 and payment_method = 'Boleto' then amount end) avg_pm_boleto_u6m,
        
        avg(case when flag_u12m = 1 and payment_method = 'Cartao Debito' then amount end) avg_pm_cd_u12m,
        avg(case when flag_u12m = 1 and payment_method = 'PIX' then amount end) avg_pm_pix_u12m,
        avg(case when flag_u12m = 1 and payment_method = 'Cartao Credito' then amount end) avg_pm_cc_u12m,
        avg(case when flag_u12m = 1 and payment_method = 'Boleto' then amount end) avg_pm_boleto_u12m
        
    from month_flags
    group by 1
""")
bk_00.createOrReplaceTempView('bk_00')

bk_00.write.partitionBy('ref','ts_proc').parquet('s3://nome_do_bucket/0002_gold/book/',mode='append')