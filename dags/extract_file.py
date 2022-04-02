from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.sensors.file_sensor import FileSensor
#default_args = {"owner":"admin","start_date":datetime(2022,1,1)}
with DAG(dag_id="workflow1",schedule_interval='@daily', start_date=datetime(2022,1,1)) as dag:
    check_file = FileSensor(
        task_id="check_file",
        poke_interval= 30,
        filepath="/Users/manvijha/Documents/MyProjects/CaseStudy_DE/src_data/20201001220037_products.csv",
    )