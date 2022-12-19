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



path_temp_csv1 = "/tmp/desemprego.csv"
email_failed = "felipesembay91@gmail.com"

dag = DAG(
    dag_id="elt-pipeline1-Dados-Desemprego",
    description="Pipeline para o processo de ETL dos dados de Desemprego para o banco de dados.",
    start_date=days_ago(0),
    schedule_interval="@daily")

#start_date = "2022-11-01"
#end_date = "2022-11-13"
def _extract():
    url = "https://servicodados.ibge.gov.br/api/v3/agregados/6381/periodos/201203|201204|201205|201206|201207|201208|201209|201210|201211|201212|201301|201302|201303|201304|201305|201306|201307|201308|201309|201310|201311|201312|201401|201402|201403|201404|201405|201406|201407|201408|201409|201410|201411|201412|201501|201502|201503|201504|201505|201506|201507|201508|201509|201510|201511|201512|201601|201602|201603|201604|201605|201606|201607|201608|201609|201610|201611|201612|201701|201702|201703|201704|201705|201706|201707|201708|201709|201710|201711|201712|201801|201802|201803|201804|201805|201806|201807|201808|201809|201810|201811|201812|201901|201902|201903|201904|201905|201906|201907|201908|201909|201910|201911|201912|202001|202002|202003|202004|202005|202006|202007|202008|202009|202010|202011|202012|202101|202102|202103|202104|202105|202106|202107|202108|202109|202110|202111|202112|202201|202202|202203|202204|202205|202206|202207|202208|202209/variaveis/4099|4103?localidades=N1[all]"
    r = requests.get(url)
    data = r.json()
    data = data[0]['resultados'][0]['series'][0]['serie']
    df = json_normalize(data).T
    df = df.reset_index()
    df = df.rename(columns = {0:"Desemprego"})
    df = df.rename(columns = {"index":"Ano"})
    c=pd.Series(pd.date_range('2012-03', freq='M', periods=len(df['Ano'])))
    df2 = pd.DataFrame(df)
    bb = pd.concat([c, df2], axis = 1).reset_index(drop = True)
    bb.dropna(inplace=True)
    bb.rename(columns={0:'Date'}, inplace = True)
    del bb['Ano']
    bb['Desemprego'] = bb['Desemprego'].astype(float)
    bb['Desemprego'] = bb['Desemprego']/100

    #exportando os dados para o disco.
    bb.to_csv(path_temp_csv1,index=False)
    
def _load():
    #conectando com o banco de dados postgresql
    engine_mysql_oltp = sqlalchemy.create_engine('mysql+pymysql://root:abc123@172.17.0.3:3306/economia')
    
    #selecionando os dados.
    #lendo os dados a partir dos arquivos csv.
    dataset_df = pd.read_csv(path_temp_csv1)

    #carregando os dados no banco de dados.
    dataset_df.to_sql("desemprego", engine_mysql_oltp, if_exists="replace",index=False)
    
    
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