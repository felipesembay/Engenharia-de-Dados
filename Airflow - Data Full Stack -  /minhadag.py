import datetime as dt
import requests
import pandas as pd
import quandl
import sqlalchemy
from pandas import json_normalize
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email_operator import EmailOperator



path_temp_csv = "/tmp/pib.csv"
email_failed = "felipesembay91@gmail.com"

dag = DAG(
    dag_id="elt-pipeline1-Dados-Pib",
    description="Pipeline para o processo de ETL dos dados do PIB para o banco de dados.",
    start_date=days_ago(0),
    schedule_interval="@daily")


#start_date = "2022-11-01"
#end_date = "2022-11-13"
def _extract():
    url = "https://servicodados.ibge.gov.br/api/v3/agregados/5932/periodos/199601|199602|199603|199604|199701|199702|199703|199704|199801|199802|199803|199804|199901|199902|199903|199904|200001|200002|200003|200004|200101|200102|200103|200104|200201|200202|200203|200204|200301|200302|200303|200304|200401|200402|200403|200404|200501|200502|200503|200504|200601|200602|200603|200604|200701|200702|200703|200704|200801|200802|200803|200804|200901|200902|200903|200904|201001|201002|201003|201004|201101|201102|201103|201104|201201|201202|201203|201204|201301|201302|201303|201304|201401|201402|201403|201404|201501|201502|201503|201504|201601|201602|201603|201604|201701|201702|201703|201704|201801|201802|201803|201804|201901|201902|201903|201904|202001|202002|202003|202004|202101|202102|202103|202104|202201|202202/variaveis/6561?localidades=N1[all]&classificacao=11255[90707]"
    r = requests.get(url)
    data = r.json()
    data = data[0]['resultados'][0]['series'][0]['serie']
    df = json_normalize(data).T
    df = df.reset_index()
    df = df.rename(columns = {0:"Pib"})
    df = df.rename(columns = {"index":"Ano"})
    c=pd.Series(pd.date_range('1996', freq='Q', periods=len(df['Ano'])))
    df2 = pd.DataFrame(df)
    bb = pd.concat([c, df2], axis = 1).reset_index(drop = True)
    bb.dropna(inplace=True)
    bb.rename(columns={0:'Date'}, inplace = True)
    del bb['Ano']
    bb['Pib'] = bb['Pib'].astype(float)
    bb['Pib'] = bb['Pib']/100


    #exportando os dados para o disco.
    bb.to_csv(path_temp_csv,index=False)
    
def _load():
    #conectando com o banco de dados postgresql
    engine_mysql = sqlalchemy.create_engine('mysql+pymysql://root:abc123@172.17.0.3:3306/economia')
   
    #selecionando os dados.
    #lendo os dados a partir dos arquivos csv.
    dataset_df = pd.read_csv(path_temp_csv)

    #carregando os dados no banco de dados.
    dataset_df.to_sql("pib", engine_mysql, if_exists="replace",index=False)
    
    
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