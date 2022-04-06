from datetime import datetime,timedelta,date
from sqlalchemy import create_engine
import glob
import pandas as pd


def insert_cont():

    today = date.today()
    dd=str(today.day).zfill(2) #day  is of 2 characters
    mm=str(today.month).zfill(2) #month is of 2 characters
    yyyy=str(today.year)
    type1='contracts'

    g=glob.glob("/Users/manvijha/Documents/MyProjects/CaseStudy_DE/src_data/"+yyyy+mm+dd+"*_"+type1+".csv")[0]
    engine = create_engine('postgresql://postgres:1234@localhost:5432/airflow_db')


    df=pd.read_csv(g,sep=";")
    df.to_sql(
        name='contracts', 
        con=engine,
        index=False,
        if_exists='append' # if the table already exists, append this data
        )