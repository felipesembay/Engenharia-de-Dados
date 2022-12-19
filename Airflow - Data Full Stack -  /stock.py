import requests
import pandas as pd
import yfinance as yf
import pandas_datareader.data as web
import datetime as dt
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import sqlalchemy

dag = DAG(
    dag_id="Ibov",
    description="A DAG para coletar cotações das empresas listadas no Ibov.",
    #schedule_interval="@daily",
    schedule_interval=dt.timedelta(hours=6),
    #start_date= dt.datetime(2022, 11, 29, 18, 0),
    start_date= dt.datetime.now(),
    #schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60)
)

email_failed = "felipesembay91@gmail.com"
datafile1 = "/tmp/stock1.parquet"
datafile2 = "/tmp/stock2.parquet"
datafile3 = "/tmp/stock3.parquet"
datafile4 = "/tmp/stock4.parquet"
datafile5 = "/tmp/stock5.csv"
tickers = ['WEGE3.SA',
                'EMBR3.SA',
                'AZUL4.SA',
                'ECOR3.SA',
                'CCRO3.SA',
                'ECOR3.SA',
                'GOLL4.SA',
                'RAIL3.SA',
                'POSI3.SA',
                'BRFS3.SA',
                'JBSS3.SA',
                'MRFG3.SA',
                'BEEF3.SA',
                'SMTO3.SA',
                'ABEV3.SA',
                'ASAI3.SA',
                'CRFB3.SA',
                'PCAR3.SA',
                'NTCO3.SA',
                'RAIZ4.SA',
                'SLCE3.SA',
                'AMER3.SA',
                'ARZZ3.SA',
                'SOMA3.SA',
                'LREN3.SA',
                'MGLU3.SA',
                'PETZ3.SA',
                'VIIA3.SA',
                'ALPA4.SA',
                'CYRE3.SA',
                'EZTC3.SA',
                'MRVE3.SA',
                'CVCB3.SA',
                'COGN3.SA',
                'RENT3.SA',
                'YDUQ3.SA',
                'BRML3.SA',
                'IGTI11.SA',
                'MULT3.SA',
                'BPAN4.SA',
                'BBDC3.SA',
                'BBDC4.SA',
                'BBAS3.SA',
                'BPAC11.SA',
                'ITSA4.SA',
                'ITUB4.SA',
                'SANB11.SA',
                'BBSE3.SA',
                'IRBR3.SA',
                'SULA11.SA',
                'B3SA3.SA',
                'CIEL3.SA',
                'DXCO3.SA',
                'KLBN11.SA',
                'SUZB3.SA',
                'BRAP4.SA',
                'CMIN3.SA',
                'VALE3.SA',
                'BRKM5.SA',
                'GGBR4.SA',
                'GOAU4.SA',
                'CSNA3.SA',
                'USIM5.SA',
                'RRRP3.SA',
                'CSAN3.SA',
                'PETR3.SA',
                'PETR4.SA',
                'PRIO3.SA',
                'UGPA3.SA',
                'VBBR3.SA',
                'HYPE3.SA',
                'RADL3.SA',
                'FLRY3.SA',
                'HAPV3.SA',
                'QUAL3.SA',
                'RDOR3.SA',
                'LWSA3.SA',
                'CASH3.SA',
                'TOTS3.SA',
                'VIVT3.SA',
                'TIMS3.SA',
                'SBSP3.SA',
                'CMIG4.SA',
                'CPLE6.SA',
                'CPFE3.SA',
                'ELET3.SA',
                'ELET6.SA',
                'ENBR3.SA',
                'ENGI11.SA',
                'ENEV3.SA',
                'EGIE3.SA',
                'EQTL3.SA',
                'TAEE11.SA']
                
start = "2006-01-01"


def _extract_close(**context):
    
    data_c = web.get_data_yahoo(tickers, start=start)['Close']
    data_c = data_c.reset_index()
    data_c = data_c.round(2)
    data_c.fillna(0, inplace=True)
    data_c = pd.melt(data_c, id_vars = ["Date"])
    data_c['Symbols'] = data_c['Symbols'].astype(str).str.replace('.SA','')
    data_c = data_c.rename(columns = {"Symbols":"Codigo"})
    data_c = data_c.rename(columns = {"value":"Close"})
    data_c.to_parquet(datafile1,index=False)

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
    data_o.to_parquet(datafile2,index=False)
    
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
    data_l.to_parquet(datafile3,index=False)
    
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
    data_h.to_parquet(datafile4,index=False)
    
def _join():
    dfc = pd.read_parquet(datafile1)
    dfo = pd.read_parquet(datafile2)
    dfl = pd.read_parquet(datafile3)
    dfh = pd.read_parquet(datafile4)
    
    acao = pd.concat([dfc, dfo, dfl, dfh], axis=1)
    acao = acao.reset_index()

    del acao['Date1']
    del acao['Date2']
    del acao['Date3']
    del acao['Codigo1']
    del acao['Codigo2']
    del acao['Codigo3']
    
    acao.to_csv(datafile5, index=False)
  
def _load():
    #conectando a base de dados de oltp.
    engine_mysql_oltp = sqlalchemy.create_engine('mysql+pymysql://root:abc123@172.17.0.3:3306/economia')
    
    #importando dados 
    join = pd.read_csv(datafile5)
    
    #carregando os dados no banco de dados.
    join.to_sql("acao_cotacao", engine_mysql_oltp, if_exists="replace",index=False)
    
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
    