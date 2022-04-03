from airflow import DAG
from datetime import datetime,timedelta,date
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.sensors.file_sensor import FileSensor
#default_args = {"owner":"admin","start_date":datetime(2022,1,1)}
type1='products'
type2='prices'
type3='contracts'

today = date.today()
dd=str(today.day).zfill(2) #day  is of 2 characters
mm=str(today.month).zfill(2) #month is of 2 characters
yyyy=str(today.year)

upload_path1="/Users/manvijha/Documents/MyProjects/CaseStudy_DE/src_data/"+yyyy+mm+dd+"*_"+type1+".csv"
upload_path2="/Users/manvijha/Documents/MyProjects/CaseStudy_DE/src_data/"+yyyy+mm+dd+"*_"+type2+".csv"
upload_path3="/Users/manvijha/Documents/MyProjects/CaseStudy_DE/src_data/"+yyyy+mm+dd+"*_"+type3+".csv"


#schedule interval is 10pm on 1st of each month
with DAG(dag_id="workflow1",schedule_interval='00 22 1 * *', start_date=datetime(2022,1,1)) as dag:
    check_file1 = FileSensor(
        task_id="check_file1",
        poke_interval=60*15, #Should check after 15mins if the file is available
        retries=3, #1 original check and 3 extra checks
        timeout=60*15*4, #task timeout if the file not found after all 4 checks
        filepath=upload_path1,
    )

    check_file2 = FileSensor(
        task_id="check_file2",
        poke_interval=60*15, #Should check after 15mins if the file is available
        retries=3, #1 original check and 3 extra checks
        timeout=60*15*4, #task timeout if the file not found after all 4 checks
        filepath=upload_path2,
    )

    check_file3 = FileSensor(
        task_id="check_file3",
        poke_interval=60*15, #Should check after 15mins if the file is available
        retries=3, #1 original check and 3 extra checks
        timeout=60*15*4, #task timeout if the file not found after all 4 checks
        filepath=upload_path3,
    )

    check_file1>>check_file2>>check_file3

