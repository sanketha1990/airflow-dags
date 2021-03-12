from airflow import DAG
from time import sleep
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from operators.data_reader.csv_reader_operator import CsvReaderOperator

from operators.feature_synthesis.expression_operator import ExpressionOperator

with DAG('expression_operator_pipeline', description='expression operator', schedule_interval='*/1 * * * *',start_date=datetime(2021, 2, 2), catchup=False) as dag:

    csv_reader_operator = PythonOperator(task_id='csv_reader_operator', python_callable=CsvReaderOperator.transform, op_kwargs={'configJson' :'{"fileName": "iris_sample.csv", "delimiter":",", "path": "/usr/local/dask-data/"}','daskClient':'10.113.113.95:8686','pipelineId':'pipe1','operatorId':'op1'},provide_context=True)
    #expression_operator = PythonOperator(task_id='expression_operator',python_callable=ExpressionOperator.transformDistributed, op_kwargs={'configJson':'{"rules":[{"column":"class_Test","dataType":"string","function":"num_words ([class])","functions":[{"name":"num_words","args":["class"]}]},{"column":"num_character","dataType":"string","function":"num_char ([class])","functions":[{"name":"num_char","args":["class"]}]},{"column":"isin","dataType":"string","function":"isin ([class])","functions":[{"name":"isin","args":["class","Iris"]}]}]}','pipelineId':'768c8d6d-5733-4397-8ead-6a39105f5afd','operatorId':'174e-23fg-32er-qf25','previousNodeId':'csv_reader_operator','daskClient':'10.113.113.95:8786'},provide_context=True)
    expression_operator = PythonOperator(task_id='expression_operator',python_callable=ExpressionOperator.transform, op_kwargs={'configJson':'{"rules":[{"column":"dob_day_of_month","dataType":"datetime","function":"day_of_month ( [dob] )","functions":[{"name":"day_of_month","args":["dob"]}]}]}','pipelineId':'pipe1','operatorId':'op2','previousNodeId':'csv_reader_operator'},provide_context=True)
    
    csv_reader_operator >> expression_operator