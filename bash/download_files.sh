#!/bin/bash

rm /home/ubuntu/airflow/dags/*.py

aws s3 sync s3://data-lake-714352821364/0004_codes/dags/ /home/ubuntu/airflow/dags
