import datetime as dt
import requests
import pandas as pd
import quandl
import sqlalchemy
from pandas import json_normalize
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.dates import days_ago

email_failed = "felipesembay91@gmail.com"
path_temp_csv = "/tmp/macroeconomics/dataset.csv"


dag = DAG(
    dag_id="pipeline_data_macroeconomics",
    description="A DAG para coletar informações macroeconômicas.",
    schedule_interval="@daily",
    #schedule_interval=dt.timedelta(hours=1),
    start_date=days_ago(0),
    #schedule_interval=None
    
)

def _extract(**context):

    selic = "BCB/432"
    dolar = "BCB/10813"
    cdi = "BCB/4392"
    ipca = "BCB/13522"
    

    #datafile = "/tmp/dataset-{}.csv".format(start_date)
    quandl.ApiConfig.api_key = '658nsbb_irvyrCRsfTzEAXZ'
    start_date = '2006-01-01'
    selic = quandl.get(selic, start_date=start_date, collapse='monthly').reset_index()
    dolar = quandl.get(dolar, start_date=start_date, collapse='monthly').reset_index()
    cdi = quandl.get(cdi, start_date=start_date, collapse='monthly').reset_index()
    ipca = quandl.get(ipca, start_date=start_date, collapse='monthly').reset_index()
    
    #exportando os dados para a área de stage.
    selic.to_csv(path_temp_csv,index=False)
    dolar.to_csv(path_temp_csv,index=False)
    cdi.to_csv(path_temp_csv,index=False)
    ipca.to_csv(path_temp_csv,index=False)

    #selic = selic.reset_index()
    selic = selic.rename(columns = {"Value":"Selic"})
    selic = selic.rename(columns = {"Date":"Data"})
    #dolar = dolar.reset_index()
    dolar = dolar.rename(columns = {"Value":"Dolar"})
    #cdi = cdi.reset_index()
    cdi = cdi.rename(columns = {"Value":"CDI"})
    #ipca = ipca.reset_index()
    ipca = ipca.rename(columns = {"Value":"IPCA"})
    df_final = pd.concat([selic, cdi, ipca, dolar], axis = 1)
    df_final.drop(['Date'], axis=1, inplace=True)
    df_final['Selic'] = df_final['Selic']/100
    df_final['CDI'] = df_final['CDI']/100
    df_final['IPCA'] = df_final['IPCA']/100
    
    #persistindo o dataset no arquivo temporario.
    df_final.to_csv(path_temp_csv,index=False)
    
def _load():
    #conectando a base de dados de oltp.
    engine_mysql_oltp = sqlalchemy.create_engine('mysql+pymysql://root:abc123@172.17.0.3:3306/economia')
    
    #selecionando os dados.
    #lendo os dados a partir dos arquivos csv.
    df_final = pd.read_csv(path_temp_csv)

    #carregando os dados no banco de dados.
    df_final.to_sql("dados_macroeconomicos", engine_mysql_oltp, if_exists="replace",index=False)
    
    
extract_task = PythonOperator(
    task_id="Extract_Dataset", 
    python_callable=_extract,
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

extract_task  >> load_task >> clean_task