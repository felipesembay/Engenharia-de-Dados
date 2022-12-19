import datetime as dt
import requests
import pandas as pd
import sqlalchemy
from pandas import json_normalize
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email_operator import EmailOperator

path_temp_csv = "/tmp/fundamentus.csv"
email_failed = "felipesembay91@gmail.com"

dag = DAG(
    dag_id="Fundamentus",
    description="DAG para coletar informações referente a dados fundamentalistas das empresas.",
    start_date=days_ago(0),
    schedule_interval=dt.timedelta(hours=3))


#start_date = "2022-11-01"
#end_date = "2022-11-13"
def _extract():
    url = 'https://www.fundamentus.com.br/resultado.php'
    header = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.89 Safari/537.36"}
    r = requests.get(url, headers=header)
    df = pd.read_html(r.text,  decimal=',', thousands='.')[0]
    for coluna in ['Div.Yield', 'Mrg Ebit', 'Mrg. Líq.', 'ROIC', 'ROE', 'Cresc. Rec.5a']:
        df[coluna] = df[coluna].str.replace('.', '')
        df[coluna] = df[coluna].str.replace(',', '.')
        df[coluna] = df[coluna].str.rstrip('%').astype('float') / 100
        
    #Criando uma lista com as empresas que fazem parte do Ibov
    ibov = ['WEGE3','EMBR3','AZUL4',
 'CCRO3','ECOR3','GOLL4','RAIL3','POSI3','BRFS3','JBSS3','MRFG3','BEEF3','SMTO3','ABEV3','ASAI3',
 'CRFB3','PCAR3','NTCO3','RAIZ4','SLCE3','AMER3','ARZZ3','SOMA3','LREN3','MGLU3','PETZ3',
 'VIIA3','ALPA4','CYRE3','EZTC3','MRVE3','CVCB3','COGN3','RENT3','YDUQ3','BRML3','IGTI11',
 'MULT3','BPAN4','BBDC3','BBDC4','BBAS3','BPAC11','ITSA4','ITUB4','SANB11','BBSE3','IRBR3',
 'SULA11','B3SA3','CIEL3','DXCO3','KLBN11','SUZB3','BRAP4','CMIN3','VALE3','BRKM5','GGBR4',
 'GOAU4','CSNA3','USIM5','RRRP3','CSAN3','PETR3','PETR4','PRIO3','UGPA3','VBBR3','HYPE3',
 'RADL3','FLRY3','HAPV3','QUAL3','RDOR3','LWSA3','CASH3','TOTS3','VIVT3','TIMS3','SBSP3',
 'CMIG4','CPLE6','CPFE3','ELET3','ELET6','ENBR3','ENGI11','ENEV3','EGIE3','EQTL3','TAEE11']
    
    #fazendo um merge com os dados do Ifix, como forma de filtrar apenas os fundos que vamos querer.
    ibov = pd.DataFrame(ibov)
    ibov = ibov.rename(columns= {0:"Papel"})
    #df = df.rename(colums={"Papel":"Codigo"})
    df = pd.merge(ibov, df, how = 'inner', on = 'Papel')

    #exportando os dados para o disco.
    df.to_csv(path_temp_csv,index=False)
    
def _load():
    #conectando com o banco de dados postgresql
    engine_mysql_oltp = sqlalchemy.create_engine('mysql+pymysql://root:abc123@172.17.0.3:3306/economia')
    
    #selecionando os dados.
    #lendo os dados a partir dos arquivos csv.
    dataset_df = pd.read_csv(path_temp_csv)

    #carregando os dados no banco de dados.
    dataset_df.to_sql("fundamentos", engine_mysql_oltp, if_exists="replace",index=False)
    
    
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

extract_task >> load_task >> clean_task
