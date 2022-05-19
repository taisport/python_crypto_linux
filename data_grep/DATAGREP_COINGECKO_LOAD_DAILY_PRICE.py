##### python Reference :
### https://stackoverflow.com/questions/26265403/easiest-way-to-create-a-color-gradient-on-excel-using-python-pandas
### https://xlsxwriter.readthedocs.io/working_with_conditional_formats.html

import urllib.request
import requests
import os
import zipfile
import glob
import pandas as pd
import numpy as np
import PROP.DB
import urllib3
import ccxt
from typing import Optional, List
from loguru import logger
from datetime import date, timedelta, datetime
from decimal import *
import time


_binance = ccxt.binanceusdm()
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 320)

#############
### Variable
#############
csv_end_date = date(2022, 5, 1)


## sample url : https://data.binance.vision/data/spot/daily/klines/BTCUSDT/5m/BTCUSDT-5m-2022-03-28.zip
## get the current directory
current_path = os.path.abspath(os.getcwd())
csvfolder = "csv_folder"

currency = 'USD'
timeframe = '1d'
DIR = os.path.dirname(os.path.realpath(__file__))
csvfolderpath = os.path.join(DIR, csvfolder)
print(csvfolderpath)

#############
### fundction
#############

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


## create file director for storig btc csv file
def create_csvfolder():
    if not os.path.exists(os.path.join(DIR, csvfolder)):
        os.makedirs(os.path.join(DIR, csvfolder))


def csv_download(csv_download_url, coincurrency, csv_start_date):
    for single_date in daterange(csv_start_date, csv_end_date):
        csv_download_full_url = csv_download_url + str(single_date.strftime("%Y-%m-%d")) + ".zip"
        csv_download_file_path = os.path.join(csvfolderpath, coincurrency + '-' + coincurrency + '-' + single_date.strftime("%Y-%m-%d") + ".zip")
        csvfile = requests.get(csv_download_full_url)
        with open(csv_download_file_path, 'wb') as output:
            output.write(csvfile.content)

        if os.path.getsize(csv_download_file_path) == 330:
            os.remove(csv_download_file_path)



def unzip_file():
    for item in os.listdir(csvfolderpath):
        if item.endswith(".zip"):
            file_name = csvfolderpath + "\\" + item
            zip_ref = zipfile.ZipFile(file_name)
            zip_ref.extractall(csvfolderpath)
            zip_ref.close()
            os.remove(file_name)


def load_all_csv_to_dataframe():
    csv_files = glob.glob(csvfolderpath + "/*.csv")

    df_list = (pd.read_csv(file, usecols=[0, 1, 2, 3, 4, 5], header=None) for file in csv_files)

    big_df = pd.concat(df_list, ignore_index=True)
    big_df.columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
    big_df['Date'] = pd.to_datetime(big_df['timestamp'] / 1000, unit='s')

    return big_df.sort_values(by=['Date'])


def GetCryptoList():
    connection = PROP.DB.DB_CRYPTO()
    CryptoList = []
    CryptoDateList = []
    CoingeckoCryptoList = []
    if connection.is_connected():
        cursor = connection.cursor()
        cursor.execute("select symbol, start_date, coingecko_coinid from crypto_info where source = 'Coingecko' and active_flag = 1;")
        Crypto = cursor.fetchall()
        cursor.close()
        for row in Crypto:
            CryptoList.append(row[0])
            CryptoDateList.append(row[1])
            CoingeckoCryptoList.append(row[2])
        return (CryptoList, CryptoDateList, CoingeckoCryptoList)


def deleteAllFile():
    os.chdir(csvfolderpath)
    all_files = os.listdir()

    for f in all_files:
        os.remove(f)


def InsertCryptoPrice(symbol, currency, update_date,  close_price, source):
    connection = PROP.DB.DB_CRYPTO()
    cursor = connection.cursor()
    sql = "REPLACE INTO crypto_daily_price (symbol, currency, update_date, close_price, source) VALUES (%s, %s, %s, %s, %s) "
    val = (symbol, currency, update_date, close_price, source)
    cursor.execute(sql, val)
    connection.commit()
    cursor.close()
    connection.close()

def fetch_price_usd_daily_all(coin_id: str = 'solana') -> Optional[pd.DataFrame]:
    url = 'https://api.coingecko.com/api/v3/coins/'+ coin_id + \
        '/market_chart?vs_currency=usd&days=3&interval=daily'
    response = requests.get(url)
    data_dict = response.json()
    price_list = data_dict.get('prices')
    if price_list is None:
        logger.error('cannot fetch data from api, coin_id={}', coin_id)
        return
    data_df = pd.DataFrame(price_list)
    data_df.columns = ['ts', coin_id + '_price_usd']
    data_df['ts'] = data_df['ts'].apply(lambda x: pd.to_datetime(_binance.iso8601(x)))
    data_df = data_df.set_index('ts') # daily time point = 00:00:00Z
    return data_df


## main

#############
### Data Preparation
#############

print("Data Preparation")

create_csvfolder()
CryptoList, CryptoDateList, CoingeckoCryptoList = GetCryptoList()
i = 0
for x in CryptoList:
    print(x)
    data_set = []
    coin = x
    coin_currency = x + currency
    data_set = fetch_price_usd_daily_all(CoingeckoCryptoList[i])
    data_set = data_set.reset_index(level=0)
    data_set = data_set.assign(symbol=coin)
    data_set = data_set.assign(currency=currency)
    data_set = data_set.assign(source='coingecko')

    for y in range(0, len(data_set)):
        symbol = data_set.iloc[y, 2]
        update_date = data_set.iloc[y, 0].strftime('%y-%m-%d')
        close_price = Decimal(data_set.iloc[y, 1])
        currency = data_set.iloc[y, 3]
        source = data_set.iloc[y, 4]
        InsertCryptoPrice(symbol, currency, update_date, close_price, source)
    i = i + 1
    time.sleep(1)