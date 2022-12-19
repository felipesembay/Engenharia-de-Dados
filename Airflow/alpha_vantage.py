import requests
import pandas as pd
import yfinance as yf
import pandas_datareader.data as web
import datetime as dt
from datetime import datetime
from io import StringIO
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import sqlalchemy
from io import StringIO

dag = DAG(
    dag_id="Alpha_Vantage",
    description="A DAG para coletar cotações das empresas listadas no Ibov.",
    #schedule_interval="@daily",
    schedule_interval=dt.timedelta(hours=6),
    #start_date= dt.datetime(2022, 11, 29, 18, 0),
    start_date= dt.datetime.now(),
    #schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60)
)
email_failed = "felipesembay91@gmail.com"
arquivo1 = "/tmp/fundo1.csv"
arquivo2 = "/tmp/fundo2.csv"
arquivo3 = "/tmp/fundo3.csv"
arquivo4 = "/tmp/fundo4.csv"
arquivo5 = "/tmp/fundo5.csv"
arquivo6 = "/tmp/fundo6.csv"

#datafile3 = "/tmp/fii3.csv"

chave_key = 'F5AXGMT36LSFNRI9MS'

dag = DAG(
    dag_id="data_alpha_vantage",
    description="A DAG utiliza API da Alpha Vantage para coletar dados de FII.",
    #schedule_interval="@daily",
    schedule_interval=dt.timedelta(hours=6),
    start_date=dt.datetime.now(),
    #end_date=dt.datetime(2022, 11, 25)
)

    
def _extract_alpha1():
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY&symbol=ARCT11.SAO&outputsize=full&apikey={chave_key}&datatype=csv'
    r = requests.get(url)
    arct11 = pd.read_csv(StringIO(r.text))
    arct11 = pd.DataFrame(arct11)
    arct11 = arct11.assign(codigo='ARCT11')
    
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY&symbol=AFHI11.SAO&outputsize=full&apikey={chave_key}&datatype=csv'
    r = requests.get(url)
    afhi11 = pd.read_csv(StringIO(r.text))
    afhi11 = pd.DataFrame(afhi11)
    afhi11 = afhi11.assign(codigo='AFHI11')
    
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY&symbol=BCFF11.SAO&outputsize=full&apikey={chave_key}&datatype=csv'
    r = requests.get(url)
    bcff11 = pd.read_csv(StringIO(r.text))
    bcff11 = pd.DataFrame(afhi11)
    bcff11 = bcff11.assign(codigo='BCFF11')
    
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY&symbol=BLMR11.SAO&outputsize=full&apikey={chave_key}&datatype=csv'
    r = requests.get(url)
    blmr11 = pd.read_csv(StringIO(r.text))
    blmr11 = pd.DataFrame(blmr11)
    blmr11 = blmr11.assign(codigo='BLMR11')
    
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY&symbol=BLMG11.SAO&outputsize=full&apikey={chave_key}&datatype=csv'
    r = requests.get(url)
    blmg11 = pd.read_csv(StringIO(r.text))
    blmg11 = pd.DataFrame(blmg11)
    blmg11 = blmg11.assign(codigo='BLMG11')
    
    ##### Concatenando e salvando os arquivos
    fundo1 = pd.concat([arct11, afhi11, bcff11, blmr11, blmg11])
    #fundo1['timestamp'] = pd.to_datetime(fundo1['timestamp'])
    #fundo1.sort_values(by='timestamp')
    fundo1.to_csv(arquivo1, index=False)
    
def _extract_alpha2():
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY&symbol=GALG11.SAO&outputsize=full&apikey={chave_key}&datatype=csv'
    r = requests.get(url)
    galg11 = pd.read_csv(StringIO(r.text))
    galg11 = pd.DataFrame(galg11)
    galg11 = galg11.assign(codigo='GALG11')
    
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY&symbol=BTAL11.SAO&outputsize=full&apikey={chave_key}&datatype=csv'
    r = requests.get(url)
    btal11 = pd.read_csv(StringIO(r.text))
    btal11 = pd.DataFrame(btal11)
    btal11 = btal11.assign(codigo='BTAL11')
    
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY&symbol=BTRA11.SAO&outputsize=full&apikey={chave_key}&datatype=csv'
    r = requests.get(url)
    btra11 = pd.read_csv(StringIO(r.text))
    btra11 = pd.DataFrame(btra11)
    btra11 = btra11.assign(codigo='BTRAL11')
    
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY&symbol=CPTS11.SAO.SAO&outputsize=full&apikey={chave_key}&datatype=csv'
    r = requests.get(url)
    cpts11 = pd.read_csv(StringIO(r.text))
    cpts11 = pd.DataFrame(cpts11)
    cpts11 = cpts11.assign(codigo='CPTS11')
    
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY&symbol=DEVA11.SAO.SAO&outputsize=full&apikey={chave_key}&datatype=csv'
    r = requests.get(url)
    deva11 = pd.read_csv(StringIO(r.text))
    deva11 = pd.DataFrame(deva11)
    deva11 = deva11.assign(codigo='DEVA11')
    
    ##### Concatenando e salvando os arquivos
    fundo2 = pd.concat([btal11, btra11, cpts11, deva11, galg11])
    #fundo1['timestamp'] = pd.to_datetime(fundo1['timestamp'])
    #fundo1.sort_values(by='timestamp')
    fundo2.to_csv(arquivo2,index=False)
    
def _extract_alpha3():
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY&symbol=HSAF11.SAO&outputsize=full&apikey={chave_key}&datatype=csv'
    r = requests.get(url)
    hsaf11 = pd.read_csv(StringIO(r.text))
    hsaf11 = pd.DataFrame(hsaf11)
    hsaf11 = hsaf11.assign(codigo='HSAF11')
    
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY&symbol=HSLG11.SAO&outputsize=full&apikey={chave_key}&datatype=csv'
    r = requests.get(url)
    hslg11 = pd.read_csv(StringIO(r.text))
    hslg11 = pd.DataFrame(hslg11)
    hslg11 = hslg11.assign(codigo='HSLG11')
    
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY&symbol=KISU11.SAO&outputsize=full&apikey={chave_key}&datatype=csv'
    r = requests.get(url)
    kisu11 = pd.read_csv(StringIO(r.text))
    kisu11 = pd.DataFrame(kisu11)
    kisu11 = kisu11.assign(codigo='KISU11')
    
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY&symbol=NSLU11.SAO.SAO&outputsize=full&apikey={chave_key}&datatype=csv'
    r = requests.get(url)
    nslu11 = pd.read_csv(StringIO(r.text))
    nslu11 = pd.DataFrame(nslu11)
    nslu11 = nslu11.assign(codigo='NSLU11')
    
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY&symbol=MCHF11.SAO.SAO&outputsize=full&apikey={chave_key}&datatype=csv'
    r = requests.get(url)
    mchf11 = pd.read_csv(StringIO(r.text))
    mchf11 = pd.DataFrame(mchf11)
    mchf11 = mchf11.assign(codigo='MCHF11')
    
    ##### Concatenando e salvando os arquivos
    fundo3 = pd.concat([hsaf11, hslg11, kisu11, nslu11, mchf11])
    #fundo1['timestamp'] = pd.to_datetime(fundo1['timestamp'])
    #fundo1.sort_values(by='timestamp')
    fundo3.to_csv(arquivo3,index=False)
    
def _extract_alpha4():
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY&symbol=NCHB11.SAO&outputsize=full&apikey={chave_key}&datatype=csv'
    r = requests.get(url)
    nchb11 = pd.read_csv(StringIO(r.text))
    nchb11 = pd.DataFrame(nchb11)
    nchb11 = nchb11.assign(codigo='NCHB11')
    
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY&symbol=PATL11.SAO&outputsize=full&apikey={chave_key}&datatype=csv'
    r = requests.get(url)
    patl11 = pd.read_csv(StringIO(r.text))
    patl11 = pd.DataFrame(patl11)
    patl11 = patl11.assign(codigo='PATL11')
    
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY&symbol=SNFF11.SAO&outputsize=full&apikey={chave_key}&datatype=csv'
    r = requests.get(url)
    snff11 = pd.read_csv(StringIO(r.text))
    snff11 = pd.DataFrame(snff11)
    snff11 = snff11.assign(codigo='SNFF11')
    
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY&symbol=RZAK11.SAO.SAO&outputsize=full&apikey={chave_key}&datatype=csv'
    r = requests.get(url)
    rzak11 = pd.read_csv(StringIO(r.text))
    rzak11 = pd.DataFrame(rzak11)
    rzak11 = rzak11.assign(codigo='RZAK11')
    
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY&symbol=TORD11.SAO.SAO&outputsize=full&apikey={chave_key}&datatype=csv'
    r = requests.get(url)
    tord11 = pd.read_csv(StringIO(r.text))
    tord11 = pd.DataFrame(tord11)
    tord11 = tord11.assign(codigo='TORD11')
    
    ##### Concatenando e salvando os arquivos
    fundo4 = pd.concat([nchb11, patl11, rzak11, tord11, snff11])
    #fundo1['timestamp'] = pd.to_datetime(fundo1['timestamp'])
    #fundo1.sort_values(by='timestamp')
    fundo4.to_csv(arquivo4,index=False)
    
def _extract_alpha5():
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY&symbol=RZTR11.SAO&outputsize=full&apikey={chave_key}&datatype=csv'
    r = requests.get(url)
    rztr11 = pd.read_csv(StringIO(r.text))
    rztr11 = pd.DataFrame(rztr11)
    rztr11 = rztr11.assign(codigo='RZTR11')
    
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY&symbol=TGAR11.SAO&outputsize=full&apikey={chave_key}&datatype=csv'
    r = requests.get(url)
    tgar11 = pd.read_csv(StringIO(r.text))
    tgar11 = pd.DataFrame(tgar11)
    tgar11 = tgar11.assign(codigo='TGAR11')
    
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY&symbol=URPR11.SAO&outputsize=full&apikey={chave_key}&datatype=csv'
    r = requests.get(url)
    urpr11 = pd.read_csv(StringIO(r.text))
    urpr11 = pd.DataFrame(urpr11)
    urpr11 = urpr11.assign(codigo='URPR11')
    
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY&symbol=VGHF11.SAO.SAO&outputsize=full&apikey={chave_key}&datatype=csv'
    r = requests.get(url)
    vghf11 = pd.read_csv(StringIO(r.text))
    vghf11 = pd.DataFrame(vghf11)
    vghf11 = vghf11.assign(codigo='VGHF11')
    
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY&symbol=PVBI11.SAO.SAO&outputsize=full&apikey={chave_key}&datatype=csv'
    r = requests.get(url)
    pvbi11 = pd.read_csv(StringIO(r.text))
    pvbi11 = pd.DataFrame(pvbi11)
    pvbi11 = pvbi11.assign(codigo='PVBI11')
    
    ##### Concatenando e salvando os arquivos
    fundo5 = pd.concat([rztr11, tgar11, urpr11, vghf11, pvbi11])
    #fundo1['timestamp'] = pd.to_datetime(fundo1['timestamp'])
    #fundo1.sort_values(by='timestamp')
    fundo5.to_csv(arquivo5,index=False)
    
def _extract_alpha6():
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY&symbol=VSLH11.SAO&outputsize=full&apikey={chave_key}&datatype=csv'
    r = requests.get(url)
    vslh11 = pd.read_csv(StringIO(r.text))
    vslh11 = pd.DataFrame(vslh11)
    vslh11 = vslh11.assign(codigo='VSLH11')
    
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY&symbol=VIUR11.SAO&outputsize=full&apikey={chave_key}&datatype=csv'
    r = requests.get(url)
    viur11 = pd.read_csv(StringIO(r.text))
    viur11 = pd.DataFrame(viur11)
    viur11 = viur11.assign(codigo='VIUR11')
    
    ##### Concatenando e salvando os arquivos
    fundo6 = pd.concat([vslh11, viur11])
    #fundo1['timestamp'] = pd.to_datetime(fundo1['timestamp'])
    #fundo1.sort_values(by='timestamp')
    fundo6.to_csv(arquivo6,index=False)    


def _load():
    #conectando a base de dados de oltp.
    engine_mysql_oltp = sqlalchemy.create_engine('mysql+pymysql://root:abc123@172.17.0.3:3306/economia')
    #importando dados 
    join1 = pd.read_csv(arquivo1)
    join2 = pd.read_csv(arquivo2)
    join3 = pd.read_csv(arquivo3)
    join4 = pd.read_csv(arquivo4)
    join5 = pd.read_csv(arquivo5)
    join6 = pd.read_csv(arquivo6)
    join_f = pd.concat([join1, join2, join3, join4, join5, join6])
    join_f['timestamp'] = pd.to_datetime(join_f['timestamp'])
    join_f.sort_values(by='timestamp', ascending=False)
    del join_f['{']

    #carregando os dados no banco de dados.
    join_f.to_sql("fii_cotacao_semanal", engine_mysql_oltp, if_exists="replace",index=False)
    

extract_task1 = PythonOperator(
    task_id="extract_alpha1", 
    python_callable=_extract_alpha1,
    email_on_failure=True,
    email=email_failed, 
    dag=dag
)

extract_task2 = BashOperator(
    task_id='sleep',
    bash_command='sleep 58',
    dag=dag
)

extract_task3 = PythonOperator(
    task_id="extract_alpha2", 
    python_callable=_extract_alpha2,
    email_on_failure=True,
    email=email_failed, 
    dag=dag
)

extract_task4 = BashOperator(
    task_id='sleep2',
    bash_command='sleep 58',
    dag=dag
)

extract_task5 = PythonOperator(
    task_id="extract_alpha3", 
    python_callable=_extract_alpha3,
    email_on_failure=True,
    email=email_failed, 
    dag=dag
)

extract_task6 = BashOperator(
    task_id='sleep3',
    bash_command='sleep 58',
    dag=dag
)

extract_task7 = PythonOperator(
    task_id="extract_alpha4", 
    python_callable=_extract_alpha4,
    email_on_failure=True,
    email=email_failed, 
    dag=dag
)

extract_task8 = BashOperator(
    task_id='sleep4',
    bash_command='sleep 59',
    dag=dag
)

extract_task9 = PythonOperator(
    task_id="extract_alpha5", 
    python_callable=_extract_alpha5,
    email_on_failure=True,
    email=email_failed, 
    dag=dag
)

extract_task10 = BashOperator(
    task_id='sleep5',
    bash_command='sleep 59',
    dag=dag
)

extract_task11 = PythonOperator(
    task_id="extract_alpha6", 
    python_callable=_extract_alpha6,
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

extract_task1  >> extract_task2 >> extract_task3  >> extract_task4 >> extract_task5  >> extract_task6 >> extract_task7  >> extract_task8 >> extract_task9  >> extract_task10 >> extract_task11  >> load_task >> clean_task
    

    
    
    
    
      