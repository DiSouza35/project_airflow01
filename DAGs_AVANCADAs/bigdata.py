from airflow import DAG
from big_data_operator import BigDataOperator
from datetime import datetime

dag =  DAG('bigdata', description="Nossa Dag",
        schedule_interval=None,start_date=datetime(2023,3,5),
        catchup=False)

big_data_parquet = BigDataOperator(
    task_id= 'Big_data_01',
    path_to_csv_file = '/opt/airflow/data/Churn.csv',
    path_to_save_file = '/opt/airflow/data/Churn.parquet',
    file_type = 'parquet',
    dag= dag
)

big_data_json = BigDataOperator(
    task_id= 'Big_data_02',
    path_to_csv_file = '/opt/airflow/data/Churn.csv',
    path_to_save_file = '/opt/airflow/data/Churn.json',
    file_type = 'json',
    dag= dag
)

big_data_parquet >> big_data_json