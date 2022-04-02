from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.sensors.file_sensor import FileSensor
#default_args = {"owner":"admin","start_date":datetime(2022,1,1)}

upload_path="/Users/manvijha/Documents/MyProjects/CaseStudy_DE/src_data/20201*.csv"

#schedule interval is 10pm on 1st of each month
with DAG(dag_id="workflow1",schedule_interval='00 22 1 * *', start_date=datetime(2022,1,1)) as dag:
    check_file = FileSensor(
        task_id="check_file",
        poke_interval=60*15, #Should check after 15mins if the file is available
        retries=3, #1 original check and 3 extra checks
        timeout=60*15*4, #task timeout if the file not found after all 4 checks
        filepath=upload_path,
    )