import datetime as dt
import requests
import pandas as pd
import numpy as np
import yfinance as yf
import pandas_datareader.data as web
import sqlalchemy
from pandas import json_normalize
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email_operator import EmailOperator



path_temp_csv = "/tmp/extration.csv"
path_temp_csv2 = "/tmp/transformation.csv"
email_failed = "felipesembay91@gmail.com"

dag = DAG(
    dag_id="etl-pipeline1-Dados-Fundo-Imobiliário_semanal",
    description="Pipeline para o processo de ETL referente a Fundos Imobiliários.",
    start_date=dt.datetime.now(),
    schedule_interval=dt.timedelta(hours=3))

def _extract_fundamentus():
    url = "https://www.fundsexplorer.com.br/ranking"
    headers = {
    'User-Agent': 
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_4) AppleWebKit/537.36'
        ' (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36'}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        df = pd.read_html(response.content, encoding='utf-8')[0]
    df.sort_values('Códigodo fundo', inplace=True)
    df['código'] = df['Códigodo fundo'] + '.SA'
    df.sort_values("Códigodo fundo", inplace=True)
    df.drop_duplicates(subset="Códigodo fundo", keep=False, inplace=True)
    
    
    #exportando os dados para o disco.
    df.to_csv(path_temp_csv,index=False)
    

def _transform_fundamentus():
    df = pd.read_csv(path_temp_csv)
    categorical_columns = ['Códigodo fundo','Setor']
    idx = df[df['Setor'].isna()].index
    df.drop(idx, inplace=True)
    df[categorical_columns] = df[categorical_columns].astype('category')
    col_floats = list(df.iloc[:,2:-1].columns)
    ## Dados nulos
    df[col_floats] = df[col_floats].fillna(value=0)
    df[col_floats] = df[col_floats].applymap(lambda x: str(x).replace('R$', '').replace('.0','').replace('.','').replace('%','').replace(',','.'))
    df[col_floats] = df[col_floats].astype('float')
    idx = df[np.isinf(df[col_floats]).any(1)].index
    df.drop(idx, inplace=True)
    df['P/VPA'] = df['P/VPA']/100
    df['DividendYield'] = df['DividendYield']/100
    df['DY (3M)Acumulado'] = df['DY (3M)Acumulado']/100
    df['DY (6M)Acumulado'] = df['DY (6M)Acumulado']/100
    df['DY (12M)Acumulado'] = df['DY (12M)Acumulado']/100
    df['DY (3M)Média'] = df['DY (3M)Média']/100
    df['DY (6M)Média'] = df['DY (6M)Média']/100
    df['DY (12M)Média'] = df['DY (12M)Média']/100
    df['DY Ano'] = df['DY Ano']/100
    df['Variação Preço'] = df['Variação Preço']/100
    df['Rentab.Período'] = df['Rentab.Período']/100
    df['Rentab.Acumulada'] = df['Rentab.Acumulada']/100
    df['DYPatrimonial'] = df['DYPatrimonial']/100
    df['VariaçãoPatrimonial'] = df['VariaçãoPatrimonial']/100
    df['Rentab. Patr.no Período'] = df['Rentab. Patr.no Período']/100
    df['Rentab. Patr.Acumulada'] = df['Rentab. Patr.Acumulada']/100
    df['VacânciaFísica'] = df['VacânciaFísica']/100
    df['VacânciaFinanceira'] = df['VacânciaFinanceira']/100
    df['Liquidez Diária'] = df['Liquidez Diária'].astype('int')
    df = df.rename(columns = {"Códigodo fundo":"Codigo"})
    dfs = df.copy()
    
    
    ifix_weekly = ['ARCT11', 'AFHI11', 'BCFF11','BLMR11', 'BLMG11','BTAL11', 'BTRA11',
                   'CPTS11', 'DEVA11','GALG11', 'HSAF11', 'HSLG11', 'KISU11', 'NSLU11', 
                   'MCHF11','NCHB11', 'PATL11', 'RZAK11', 'TORD11', 'SNFF11','RZTR11',  
                   'TGAR11',  'URPR11', 'VGHF11', 'PVBI11','VSLH11', 'VIUR11']
    
    ifix_weekly = pd.DataFrame(ifix_weekly)
    ifix_weekly = ifix_weekly.rename(columns= {0:"Codigo"})
    df2 = pd.merge(ifix_weekly, dfs, how = 'inner', on = 'Codigo')
    
    df2.to_csv(path_temp_csv2,index=False)
    
def _load_fundamentus():
    #conectando com o banco de dados postgresql
    engine_mysql_oltp = sqlalchemy.create_engine('mysql+pymysql://root:abc123@172.17.0.3:3306/economia')
    
    #selecionando os dados.
    #lendo os dados a partir dos arquivos csv.
    dataset_df2 = pd.read_csv(path_temp_csv2)
    
    dataset_df2.to_sql("fii_fund_semanal", engine_mysql_oltp, if_exists="replace",index=False)
    
extract_task = PythonOperator(
    task_id="Extract_Dataset", 
    python_callable=_extract_fundamentus,
    email_on_failure=True,
    email=email_failed, 
    dag=dag
)

transform_task = PythonOperator(
    task_id="Transform_Dataset",
    email_on_failure=True,
    email=email_failed, 
    python_callable=_transform_fundamentus, 
    dag=dag
)

load_task = PythonOperator(
    task_id="Load_Dataset",
    email_on_failure=True,
    email=email_failed, 
    python_callable=_load_fundamentus,
    dag=dag
)

clean_task = BashOperator(
    task_id="Clean",
    email_on_failure=True,
    email=email_failed,
    bash_command="scripts/clean.sh",
    dag=dag
)

extract_task >> transform_task >> load_task  >> clean_task