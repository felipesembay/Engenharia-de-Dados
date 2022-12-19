# Engenharia-de-Dados

Nesse repositório serão adicionados notebooks e arquivos referente a área de Engenharia de Dados. 

Pasta **Airflow - Data Full Stack** -  Repositório para armanezar os arquivos do Pipeline utilizando **Airflow**. Nessa pasta, estão todos os arquivos das Dag's que foram feitas no Airflow, para automatizar, orquestrar todo o nosso pipeline de Dados. 

No meu projeto Data Full Stack, utilizei o MySQL na Azure como banco de dados principal. Porém fiz os testes primeiramente no MySQL no Docker. Para utilizar as DAG's basta alterar o endereço da banco de dados, para aquele banco que você vai usar. 

No arquivo **creat_sql.txt**, consta o script para a criação do banco de dados **MYSQL**.

O projeto consiste em: 

 - Utilizar a **API do IBGE** para conectar aos Dados referente a Desemprego e PIB. 
 - Utilizar a **Biblioteca do Yahoo Finance** para capturar os dados de todas as empresas que fazem parte do Ibov e parte das cotas dos fundos imobiliários (Ifix). 
 - Utilizar a **API do Alpha Vantage** para capturar cerca de 27 cotas de fundos imobiliários que não tinha no Yahoo Finance. Essas cotações foram capturadas de forma semana. 
 - Fazer Scrapy nos sites do **fundamentus** e do **fundsexplorer.com.br/ranking** para fazer o scrapy referente as informações fundamentalistas das ações e das cotas dos fundos imobiliários. 
 - Utilizar a **biblioteca QUANDL** do Python para capturar os seguintes dados macroeconômicos: SELIC, Dólar, CDI, IPCA. 


 ############################################################################################
