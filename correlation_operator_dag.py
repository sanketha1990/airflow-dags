from airflow import DAG
from time import sleep
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from operators.data_reader.csv_reader_operator import CsvReaderOperator
from operators.statistical_analysis.correlation_operator import CorrelationOperator



with DAG('correlation_operator_dag', description='correlation operator', schedule_interval='*/1 * * * *',start_date=datetime(2021, 2, 2), catchup=False) as dag:

    csv_reader_operator = PythonOperator(task_id='csv_reader_operator', python_callable=CsvReaderOperator.transform, op_kwargs={'configJson' :'{"fileName": "iris_sample", "delimiter":",", "path": "/usr/local/dask-data/"}','daskClient':'10.113.113.95:8686','pipelineId':'caebf3aa-2f68-4d60-8575-62f7d4d545fa','operatorId':'a8d795d8-4807-48ed-ab48-585c6ef3037b'},provide_context=True)
    correlation_operator = PythonOperator(task_id='correlation_operator',python_callable=CorrelationOperator.transform, op_kwargs={'configJson':'{"columns":["sepallength","sepalwidth","petallength","petalwidth"],"method":"Spearman"}','pipelineId':'768c8d6d-5733-4397-8ead-6a39105f5afd','operatorId':'174e-23fg-32er-qf25','previousNodeId':'csv_reader_operator'},provide_context=True)
    
    csv_reader_operator >> correlation_operator