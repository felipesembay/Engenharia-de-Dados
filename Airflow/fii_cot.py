import datetime as dt
import requests
import pandas as pd
import numpy as np
import yfinance as yf
import pandas_datareader.data as web
import sqlalchemy
from pandas import json_normalize
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email_operator import EmailOperator



path_tmp1 = "/tmp/fii.parquet"
path_tmp2 = "/tmp/fii2.parquet"
path_tmp3 = "/tmp/fii3.parquet"
path_tmp4 = "/tmp/fii4.parquet"
path_tmp5 = "/tmp/fii5.csv"
email_failed = "felipesembay91@gmail.com"

dag = DAG(
    dag_id="elt-Fundo-Imobiliário-cotações",
    description="Pipeline para o processo de ETL referente a cotações dos fundos imobiliários.",
    #start_date=days_ago(0),
    schedule_interval=dt.timedelta(hours=6),
    start_date= dt.datetime.now(),
    dagrun_timeout=timedelta(minutes=60))

tickers = ['CACR11.SA',
 'BPFF11.SA',
 'ALZR11.SA',
 'ARRI11.SA',
 'AIEC11.SA',
 'BARI11.SA',
 'BBPO11.SA',
 'BRCR11.SA',
 'BCIA11.SA',
 'BCRI11.SA',
 'BRCO11.SA',
 'BTLG11.SA',
 'CPFF11.SA',
 'HGCR11.SA',
 'HGFF11.SA',
 'HGLG11.SA',
 'HGRU11.SA',
 'CARE11.SA',
 'VRTA11.SA',
 'GTWR11.SA',
 'GGRC11.SA',
 'HABT11.SA',
 'HCTR11.SA',
 'HGBS11.SA',
 'HGRE11.SA',
 'HSML11.SA',
 'HFOF11.SA',
 'FIIB11.SA',
 'IRDM11.SA',
 'JSRE11.SA',
 'KNRI11.SA',
 'KNHY11.SA',
 'KNIP11.SA',
 'KNCR11.SA',
 'KNSC11.SA',
 'KFOF11.SA',
 'MALL11.SA',
 'MCCI11.SA',
 'MXRF11.SA',
 'MFII11.SA',
 'MGFF11.SA',
 'MORE11.SA',
 'OUJP11.SA',
 'PLCR11.SA',
 'PORD11.SA',
 'QAGR11.SA',
 'RBRL11.SA',
 'RBRY11.SA',
 'RBRP11.SA',
 'RBRF11.SA',
 'RBRR11.SA',
 'RECR11.SA',
 'RECT11.SA',
 'RBFF11.SA',
 'RCRB11.SA',
 'RBVA11.SA',
 'SADI11.SA',
 'SARE11.SA',
 'SDIL11.SA',
 'SPTW11.SA',
 'SNCI11.SA',
 'TRXF11.SA',
 'VGIP11.SA',
 'VGIR11.SA',
 'CVBI11.SA',
 'LVBI11.SA',
 'RVBI11.SA',
 'VCJR11.SA',
 'VIFI11.SA',
 'VILG11.SA',
 'VINO11.SA',
 'VISC11.SA',
 'VTLT11.SA',
 'XPCI11.SA',
 'XPIN11.SA',
 'XPLG11.SA',
 'XPML11.SA',
 'XPPR11.SA',
 'XPSF11.SA']

start = "2010-01-01"
    
def _extract_close(**context):
    
    data_c = web.get_data_yahoo(tickers, start=start)['Close']
    data_c = data_c.reset_index()
    data_c = data_c.round(2)
    data_c.fillna(0, inplace=True)
    data_c = pd.melt(data_c, id_vars = ["Date"])
    data_c['Symbols'] = data_c['Symbols'].astype(str).str.replace('.SA','')
    data_c = data_c.rename(columns = {"Symbols":"Codigo"})
    data_c = data_c.rename(columns = {"value":"Close"})
    data_c.to_parquet(path_tmp1,index=False)

def _extract_open(**context):
    data_o = web.get_data_yahoo(tickers, start=start)['Open']
    data_o = data_o.reset_index()
    data_o = data_o.round(2)
    data_o.fillna(0, inplace=True)
    data_o = pd.melt(data_o, id_vars = ["Date"])
    data_o['Symbols'] = data_o['Symbols'].astype(str).str.replace('.SA','')
    data_o = data_o.rename(columns = {"Date":"Date1"})
    data_o = data_o.rename(columns = {"Symbols":"Codigo1"})   
    data_o = data_o.rename(columns = {"value":"Open"})
    data_o.to_parquet(path_tmp2,index=False)
    
def _extract_low(**context):   
    data_l = web.get_data_yahoo(tickers, start=start)['Low']    
    data_l = data_l.reset_index()
    data_l = data_l.round(2)
    data_l.fillna(0, inplace=True)
    data_l = pd.melt(data_l, id_vars = ["Date"])
    data_l['Symbols'] = data_l['Symbols'].astype(str).str.replace('.SA','')
    data_l = data_l.rename(columns = {"Date":"Date2"})
    data_l = data_l.rename(columns = {"Symbols":"Codigo2"})    
    data_l = data_l.rename(columns = {"value":"Low"})
    data_l.to_parquet(path_tmp3,index=False)
    
def _extract_high(**context):
    data_h = web.get_data_yahoo(tickers, start=start)['High']      
    data_h = data_h.reset_index()
    data_h = data_h.round(2)
    data_h.fillna(0, inplace=True)
    data_h = pd.melt(data_h, id_vars = ["Date"])
    data_h['Symbols'] = data_h['Symbols'].astype(str).str.replace('.SA','')
    data_h = data_h.rename(columns = {"Date":"Date3"}) 
    data_h = data_h.rename(columns = {"Symbols":"Codigo3"})     
    data_h = data_h.rename(columns = {"value":"High"})
    data_h.to_parquet(path_tmp4,index=False)
    
def _join():
    dfc = pd.read_parquet(path_tmp1)
    dfo = pd.read_parquet(path_tmp2)
    dfl = pd.read_parquet(path_tmp3)
    dfh = pd.read_parquet(path_tmp4)
    
    acao = pd.concat([dfc, dfo, dfl, dfh], axis=1)
    acao = acao.reset_index()

    del acao['Date1']
    del acao['Date2']
    del acao['Date3']
    del acao['Codigo1']
    del acao['Codigo2']
    del acao['Codigo3']
    
    acao.to_csv(path_tmp5, index=False)
  
def _load():
    #conectando a base de dados de oltp.
    engine_mysql_oltp = sqlalchemy.create_engine('mysql+pymysql://root:abc123@172.17.0.3:3306/economia')

    #importando dados 
    join = pd.read_csv(path_tmp5)
    
    #carregando os dados no banco de dados.
    join.to_sql("fii_cotacao", engine_mysql_oltp, if_exists="replace",index=False)
    
extract_task1 = PythonOperator(
    task_id="Extract_data_close", 
    python_callable=_extract_close,
    email_on_failure=True,
    email=email_failed, 
    dag=dag
)

extract_task2 = PythonOperator(
    task_id="Extract_data_open", 
    python_callable=_extract_open,
    email_on_failure=True,
    email=email_failed, 
    dag=dag
)

extract_task3 = PythonOperator(
    task_id="Extract_data_low", 
    python_callable=_extract_low,
    email_on_failure=True,
    email=email_failed, 
    dag=dag
)

extract_task4 = PythonOperator(
    task_id="Extract_data_high", 
    python_callable=_extract_high,
    email_on_failure=True,
    email=email_failed, 
    dag=dag
)

extract_task5 = PythonOperator(
    task_id="join_dataset", 
    python_callable=_join,
    email_on_failure=True,
    email=email_failed, 
    dag=dag
)

load_task = PythonOperator(
    task_id="Load_Dataset",
    email_on_failure=True,
    email=email_failed, 
    python_callable=_load,
    dag=dag
)

clean_task = BashOperator(
    task_id="Clean",
    email_on_failure=True,
    email=email_failed,
    bash_command="scripts/clean.sh",
    dag=dag
)

[extract_task1  >> extract_task2  >> extract_task3  >> extract_task4]  >> extract_task5  >>load_task >> clean_task
    