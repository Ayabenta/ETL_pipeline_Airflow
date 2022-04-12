from cgi import print_arguments
import enum
from bs4 import BeautifulSoup as sp
from urllib.request import Request, urlopen
import csv
f = open('products.csv', 'w', encoding='utf-8') 
f.write("")
from selenium import webdriver
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
    