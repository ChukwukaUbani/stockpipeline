import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
from datetime import datetime
from include.util import get_database_conn
import os


main_url = 'https://afx.kwayisi.org/ngx/'
list_of_df = []
# Data Extraction layer
def extract_data():
    for page in range(1, 3):
        url = main_url + f'?page={page}'  # Modify the URL for each page
        scrapped_data = requests.get(url)
        scrapped_data = scrapped_data.content
        soup = bs(scrapped_data, 'lxml')
        html_data = str(soup.find_all('table')[3])
        df = pd.read_html(html_data)[0]
        list_of_df.append(df)
    combined_data = pd.concat(list_of_df)
    if not os.path.exists('/usr/local/airflow/raw'):
        os.mkdir('/usr/local/airflow/raw')
    combined_data.to_csv('./raw/ngx_stock_data.csv', index= False)
    print('Data Successfully written to a csv file')

def transform_data():
    stock_data = pd.read_csv('/usr/local/airflow/raw/ngx_stock_data.csv') # Read csv file
    #Add a new column with the current timestamp when the scrapped is made
    current_date = datetime.today().strftime("%Y-%m-%d")
    stock_data['Date'] = current_date
    stock_data['Date'] = pd.to_datetime(stock_data['Date'])
    stock_data['Volume'] = stock_data['Volume'].apply(lambda val: val)
    mean_volume = stock_data['Volume'].loc[stock_data['Volume'].notna()].mean()
     # Fill missing volume values with the mean
    stock_data['Volume'].fillna(mean_volume, inplace=True)
    stock_data = stock_data[['Date', 'Ticker', 'Name', 'Volume', 'Price', 'Change']]
    if not os.path.exists('/usr/local/airflow/transformed'):
        os.mkdir('/usr/local/airflow/transformed')
    stock_data.to_csv('/usr/local/airflow/transformed/ngx_stock_data.csv', index= False)
    print('Data transformed and written to a csv file')


# Data load layer
def load_data_to_db():
    stock_data = pd.read_csv('/usr/local/airflow/transformed/ngx_stock_data.csv') # Read csv file
    connection = get_database_conn()
    stock_data.to_sql('ngx_stock_data', con= connection, if_exists='append', index= False)
    print('Data successfully written to PostgreSQL database')
    
