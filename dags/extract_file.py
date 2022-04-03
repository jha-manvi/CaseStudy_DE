from airflow import DAG
from datetime import datetime,timedelta,date
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
import glob
import pandas as pd
from insert_products import insert_products
from insert_prices import insert_prices
from insert_cont import insert_cont

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

s1 = glob.glob("/Users/manvijha/Documents/MyProjects/CaseStudy_DE/src_data/"+yyyy+mm+dd+"*_"+type1+".csv")[0]

engine = create_engine('postgresql://postgres:1234@localhost:5432/airflow_db')

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

    

    create_table1 = PostgresOperator(
        task_id="create_table1",
        postgres_conn_id='postgres_db',
        sql='CREATE TABLE IF NOT EXISTS products (id bigint primary key,deleted smallint,releasedversion smallint,productcode varchar(255), productname varchar(255),energy varchar(255),consumptiontype varchar(255), modificationdate date)'
    )

    create_table2 = PostgresOperator(
        task_id="create_table2",
        postgres_conn_id='postgres_db',
        sql='CREATE TABLE IF NOT EXISTS prices (id bigint primary key,productid int,pricecomponentid int,productcomponent varchar(255),price decimal(38,10), unit varchar(255),valid_from date,valid_until date, modificationdate date)'
    )

    create_table3 = PostgresOperator(
        task_id="create_table3",
        postgres_conn_id='postgres_db',
        sql='CREATE TABLE IF NOT EXISTS contracts (id bigint primary key,type varchar(255),energy varchar(255),usage int,usagenet int,createdat date,startdate date,enddate date,fillingdatecancellation date,cancellationreason varchar(255),city varchar(255),status varchar(255),productid int, modificationdate date)'
    )

    insert1 = PythonOperator(
        task_id="insert1",
        python_callable = insert_products
    )

    insert2 = PythonOperator(
        task_id="insert2",
        python_callable = insert_prices
    )

    insert3 = PythonOperator(
        task_id="insert3",
        python_callable = insert_cont
    )

    check_file1>>create_table1>>insert1
    check_file2>>create_table2>>insert2
    check_file3>>create_table3>>insert3

    

