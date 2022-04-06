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
        sql='CREATE TABLE IF NOT EXISTS products (id bigint ,deleted smallint,releasedversion smallint,productcode varchar(255), productname varchar(255),energy varchar(255),consumptiontype varchar(255), modificationdate date)'
    )

    create_table2 = PostgresOperator(
        task_id="create_table2",
        postgres_conn_id='postgres_db',
        sql='CREATE TABLE IF NOT EXISTS prices (id bigint ,productid int,pricecomponentid int,productcomponent varchar(255),price decimal(38,10), unit varchar(255),valid_from date,valid_until date, modificationdate date)'
    )

    create_table3 = PostgresOperator(
        task_id="create_table3",
        postgres_conn_id='postgres_db',
        sql='CREATE TABLE IF NOT EXISTS contracts (id bigint ,type varchar(255),energy varchar(255),usage int,usagenet int,createdat date,startdate date,enddate date,fillingdatecancellation date,cancellationreason varchar(255),city varchar(255),status varchar(255),productid int, modificationdate date)'
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

    validate1 = PostgresOperator(
        task_id="validate_table1",
        postgres_conn_id='postgres_db',
        sql='insert into products_post ( select distinct a.* from (select id,deleted, releasedversion,productcode,productname,energy ,consumptiontype,modificationdate from products )a inner join (select id , max(modificationdate) as maxd from products group by 1)b on a.id=b.id and a.modificationdate=b.maxd order by 1 )'
    )

    validate2 = PostgresOperator(
        task_id="validate_table2",
        postgres_conn_id='postgres_db',
        sql='insert into prices_post ( select distinct a.* from (select id  ,productid ,pricecomponentid ,productcomponent ,price , unit ,valid_from ,valid_until , modificationdate from prices ) a inner join (select id , max(modificationdate) as maxd from prices group by 1)b on a.id=b.id and a.modificationdate=b.maxd order by 1 )'
    )

    validate3 = PostgresOperator(
        task_id="validate_table3",
        postgres_conn_id='postgres_db',
        sql='insert into contracts_post ( select distinct a.* from (select id ,type ,energy ,usage ,usagenet ,createdat ,startdate ,enddate ,fillingdatecancellation ,cancellationreason ,city ,status,productid , modificationdate from contracts ) a inner join (select id , max(modificationdate) as maxd from contracts group by 1)b on a.id=b.id and a.modificationdate=b.maxd order by 1 )'
    )

    mid_result_op=PostgresOperator(
        task_id="op1",
        postgres_conn_id='postgres_db',
        sql = "insert into output_to_analyst_mid ( select distinct createdat,usage,productcode,productname,pricecomponentid,productcomponent,price from (select a.createdat,a.usage,b.id,b.productcode,b.productname from (select * from contracts_post ) a left outer join (select * from products_post) b  on a.productid=b.id and a.energy=b.energy ) a1 left outer join (select * from prices_post where valid_until='9999-12-31') c on a1.id=c.productid);"
    )

    final_result_op=PostgresOperator(
        task_id="op_final",
        postgres_conn_id='postgres_db',
        sql = "insert into fin_out(select createdat,productname,sum(consumption) as consumption,sum(revenue) as revenue from ( select productname,createdat,usage as consumption, (baseprice+(usage*workingprice)) as revenue from ( select distinct a1.createdat,a1.usage,a1.productname,avg(workingprice) as workingprice, avg(baseprice) as baseprice from  (select createdat,usage,productname, price as workingprice from output_to_analyst_mid where productcomponent='workingprice') a1 inner join (select createdat,usage,productname, price as baseprice from output_to_analyst_mid where productcomponent='baseprice') b1 on a1.createdat=b1.createdat and a1.usage=b1.usage and a1.productname=b1.productname   group by 1,2,3) as e order by productname , createdat) f group by 1,2 order by 1,2)"
    )

    answer1_mid=PostgresOperator(
        task_id="answer1_mid",
        postgres_conn_id='postgres_db',
        sql = "insert into answer1_mid (select distinct contractid,createdat,usage,productcode,productname,pricecomponentid,productcomponent,price from (select a.id as contractid,a.createdat,a.usage,b.id,b.productcode,b.productname from (select * from contracts_post ) a left outer join (select * from products_post) b  on a.productid=b.id and a.energy=b.energy ) a1 left outer join (select * from prices_post where valid_until='9999-12-31') c on a1.id=c.productid);"
    )

    answer1_final=PostgresOperator(
        task_id="answer1_final",
        postgres_conn_id='postgres_db',
        sql="insert into answer1_fin (select contractid, createdat,avg(revenue) as revenue from ( select contractid,productname,createdat,usage as consumption, (baseprice+(usage*workingprice)) as revenue from ( select distinct a1.contractid,a1.createdat,a1.usage,a1.productname,avg(workingprice) as workingprice, avg(baseprice) as baseprice from  (select contractid,createdat,usage,productname, price as workingprice from answer1_mid where productcomponent='workingprice') a1 inner join (select contractid,createdat,usage,productname, price as baseprice from answer1_mid where productcomponent='baseprice') b1 on a1.createdat=b1.createdat and a1.usage=b1.usage and a1.productname=b1.productname   group by 1,2,3,4) as e order by contractid,productname , createdat) f where createdat>='2020-10-01' and createdat<='2021-01-01' group by 1,2 order by 1,2);"
    )

    answer2_final=PostgresOperator(
        task_id="answer2_final",
        postgres_conn_id='postgres_db',
        sql="select count(*) from contracts_post where status='indelivery';"
    )

    

    check_file1>>create_table1>>insert1>>validate1
    check_file2>>create_table2>>insert2>>validate2
    check_file3>>create_table3>>insert3>>validate3

    
