# Criando um Data Lake na AWS
## Índice
- [Resumo](#resumo)
- [Arquitetura AWS utilizada](#arquitetura-aws-utilizada)
- [Tecnologias, ferramentas e linguagens utilizadas](#tecnologias,-ferramentas-e-linguagens-utilizadas)
- [Preparação do SGBD](#preparação-do-SGBD)
- [Criação e estruturação do Bucket S3](#criação-e-estruturação-do-Bucket-S3)

## Resumo
O Objetivo desse projeto é a criação de uma Data Lake utilizando a Cloud AWS, para entregar dados prontos para consumo para os Cientistas de dados.

## Arquitetura AWS utilizada

## Tecnologias, Ferramentas e linguagens utilizadas
- python
- SQL
- PySpark
- MySql
- Docker
- Linux
- Jupyter Notebook
- AWS S3
- AWS EC2
- AWS Lambda
- AWS DynamoDB

## Preparação do SGBD
O SGBD utilizado para esse projeto foi o MySql, criado um DataBase e alimentado com essas [tabelas](dados/tabelas.zip) (users, products, sales). As tabelas foram criadas com a biblioteca Faker do python.

## Criação e estruturação do Bucket S3
Para esse projeto a estrutura no Bucket S3 utilizada é a seguinte:
![](imagens/layout_bucket_S3.png)

- **0000_bronze:** primeira camada do Data Lake, arquivos brutos exatamente como da fonte;
- **0001_silver:** segunda camada do Data Lake, arquivos da camada bronze com tratamento;
- **0002_gold:** terceira camada do Data Lake, books de variáveis prontos para serem utilizados pelos cientistas de dados;
- **0003_controle:** arquivos de controle dos processos entre as camadas;
- **0004_codes:** onde estarão armazenadas as Dags de uso do Airflow e scripts utilizados pelos EMR's;
- **0005_logs:** onde ficarão armazenados os Logs registrados pelos EMR's.

## Criação da instância EC2
O tipo de instância utilizada foi uma m5.xlarge com sistema operacional Ubuntu e par de chaves .pem (faça o download da chave em um local que lembre posteriormente), não irei detalhar as políticas de segurança utilizadas durante o projeto para não extender mas lembrece de sempre utilizar aquelas com o menor previlégio necessário para o seu caso porque traz uma maior segurança segurança.

## Acessando a instância EC2 via SSH
- Primeiro libera a porta 22 para o seu IP no grupo de segurança vinculado a sua instância EC2.
- Agora para acessar a instância EC2 via prompt, navegue até o caminho onde a chave.pem foi armazenada e execute o comando:
```bash
ssh -i "chave_criada.pem" ubuntu@DNS_publico
```


### Criação de pastas scripts
Foi criada uma pasta **script** na instância EC2, onde foi transferido 2 arquivos bash:
- [Instalação Docker e Airflow](bash/install_docker_airflow.sh) **:** para instalação do Docker e Airflow na instância EC2;
- [Download da Dags do S3](bash/download_files.sh) **:** para transferir do S3 prara o EC2 as DAGs que serão utilizadas no Airflow. 

### Instalação Docker e Airflow
- De permissão para executar o arquivo
```bash
chmod +x /home/ubuntu/scripts/install_docker_airflow.sh
```
- Após conceder a permissão execute o arquivo.

- Ao finalizar a instalação, na pasta ~/airflow execute o comando para iniciar o contêiner
```bash
sudo docker compose up -d
```
- Lembre-se de liberar a porta 8080 no grupo de segurança vinculado a sua instância EC2
- Após liberar a porta coloque o seguinte endereço no navegador **dns_publico_da_instância:8080** ;
- O usuário e senha são **airflow** para ambos.

### Instalação do AWS CLI na instância EC2
Para acessar o bucket S2 através da instância EC2 é necessário a instalação do CLI, mas primeiro é necessário instalar 2 ferramentas:
- **unzip**: para descompactar arquivos .zip;
- **curl** : ferramenta para fazer requisições HTTP e baixar arquivos URLs
```bash
sudo apt-get install -y unzip curl
```
- baixe o arquivo de instalação
```bash
sudo apt-get install -y unzip curl
```
- descompactar o arquivo
```bash
unzip awscliv2.zip
```
- instalar o arquivo
```bash
sudo ./aws/install
```
- confirmar a instalação verificando a versão
```bash
aws --version
```
  
