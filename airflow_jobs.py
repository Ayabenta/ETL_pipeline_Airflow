from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator 
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.operators.mysql_operator import MySqlOperator
from bs4 import BeautifulSoup as sp
import csv
import enum
import pandas as pd
from selenium import webdriver

def scraper():
    f = open('products.csv', 'w', encoding='utf-8') 
    f.write("")
    driver = webdriver.Chrome(executable_path="/home/aya/VSworkplace/Kafka-airflow_Miniproject/selenium_chrome/drivers/chromedriver_linux64/chromedriver")
    url = 'https://www.amazon.fr/s?rh=n%3A4551203031&fs=true&ref=lp_4551203031_sar'
    driver.get(url)
    soup =sp(driver.page_source, 'html.parser')
    containers = soup.findAll('div',{'class': 'sg-col-4-of-12 s-result-item s-asin sg-col-4-of-16 sg-col s-widget-spacing-small sg-col-4-of-20'})
    for index ,container in enumerate(containers): 
        try : 
            print(index)
            url1 = 'https://www.amazon.fr'+ container.a.get('href')
            driver.get(url1)
            soup1 =sp(driver.page_source, 'html.parser')
            price= soup1.find('span', {'class':'a-size-base a-color-price'}).text.replace(',', '.')
            print(price)
            title = soup1.find('span',{'id' : 'productTitle'}).text.strip().replace(',','-')
            store=soup1.find('a',{'class':'a-link-normal', 'id' : 'bylineInfo'}).text
            rating=soup1.find('span',{'class' : 'a-icon-alt'}).text.replace(',', '.')
            reviews = soup1.find('span',{'id':'acrCustomerReviewText'}).text
            f.write(str(title)+','+str(store)+','+str(price)+','+str(rating)+','+str(reviews) + "\n")
        except AttributeError: 
            url1 = 'https://www.amazon.fr'+ container.a.get('href')
            driver.get(url1)
            soup1 =sp(driver.page_source, 'html.parser')
            price=None
            print(price)
            title = soup1.find('span',{'id' : 'productTitle'}).text.strip().replace(',','-')
            store=soup1.find('a',{'class':'a-link-normal', 'id' : 'bylineInfo'}).text
            rating=soup1.find('span',{'class' : 'a-icon-alt'}).text.replace(',', '.')
            reviews = soup1.find('span',{'id':'acrCustomerReviewText'}).text
            f.write(str(title)+','+str(store)+','+str(price)+','+str(rating)+','+str(reviews) + "\n")

    f.close

def transform():
    df = pd.read_csv('./products.csv', names=['Title','store','price','rating','NumberOfReviews'],index_col=None)
    for index in df.index:
        df.loc[index, 'store'] = df.loc[index, 'store'][20:]
        df.loc[index,'rating'] = df.loc[index,'rating'].replace(' sur ','/').replace('étoiles', '').strip()
        if df.loc[index, 'rating'] == 'Previouspage' : 
            df.loc[index, 'rating'] = 'None'
        df.loc[index,'NumberOfReviews'] = df.loc[index,'NumberOfReviews'].replace(' évaluations', '')

    df.to_csv('/home/aya/VSworkplace/products_transformed.csv')
    df.to_json('/home/aya/VSworkplace/products_transformed.json')

args =  {
    'owner' : 'Aya bentaleb',
    'start_date' : days_ago(0),
    'email' : ['ayabentale01@gmail.com'],
    'email_on_failure' : True,
    'email_on_retry' : True,
    'retries' : 1,
    'retry_delay' : timedelta(seconds=5),
    'schedule_intervale' : '@Weekly'
}
dag = DAG(dag_id='Scaping_job', default_args= args, description= 'Scraping Avito, sourcing data to hdfs for further spark jobs and mysql for storage')

Extracting = PythonOperator (task_id= 'Extracting_from_Amazon',
                             python_callable= scraper, dag=dag)
Transform = PythonOperator(task_id = 'Preprocessing',
                            python_callable=transform,dag=dag)
PreparingforMysql = BashOperator (task_id = 'Preparingfile',
                                 bash_command = 'mv /home/aya/VSworkplace/products_transformed.csv /var/lib/mysql-files/products.csv',
                                 dag=dag)
load_to_mysql = MySqlOperator(task_id= 'loading_to_mysqlserver',mysql_conn_id ="admin",
                              sql = ("LOAD DATA  INFILE '/var/lib/mysql-files/products_transformed.csv' INTO TABLE AVITO FIELDS TERMINATED BY ',';")
                                ,dag=dag)
load_to_hdfs = BashOperator(task_id='Loading_to_hdfs',
                            bash_command= 'start-dfs.sh && start-yarn.sh && hadoop fs -put -f /home/aya/VSworkplace/products_transformed.json /scrapingjob',
                            dag=dag )
Extracting >> Transform >> PreparingforMysql>>load_to_mysql>>load_to_hdfs


