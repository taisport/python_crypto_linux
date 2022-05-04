#!/usr/bin/env python
import requests
import json
import time
import datetime as dt
import os
import yaml
import pandas as pd
import dateutil.relativedelta
import numpy as np
import PROP.DB
from datetime import date
from datetime import datetime

DIR = os.path.dirname(os.path.realpath(__file__))

if not os.path.exists(os.path.join(DIR, 'data')):
    os.makedirs(os.path.join(DIR, 'data'))
if not os.path.exists(os.path.join(DIR, 'tmp')):
    os.makedirs(os.path.join(DIR, 'tmp'))

try:
    with open(os.path.join(DIR, 'config_private.yaml'), 'r') as stream:
        private_config = yaml.safe_load(stream)
except FileNotFoundError:
    private_config = None
except yaml.YAMLError as exc:
        print(exc)

try:
    with open('config.yaml', 'r') as stream:
        config = yaml.safe_load(stream)
except FileNotFoundError:
    config = None
except yaml.YAMLError as exc:
        print(exc)

def cfg(key):
    try:
        return private_config[key]
    except:
        try:
            return config[key]
        except:
            return None

def read_json(json_file):
    with open(json_file, "r", encoding="utf-8") as fp:
        return json.load(fp)

PRICE_DATA_FILE = os.path.join(DIR, "data", "crypto_price_history.json")
REFERENCE_TICKER = cfg("REFERENCE_TICKER")
TICKER_INFO_FILE = os.path.join(DIR, "data_persist", "crypto_ticker_info.json")
TICKER_INFO_DICT = read_json(TICKER_INFO_FILE)
REF_TICKER = {"ticker": REFERENCE_TICKER, "sector": "--- Reference ---", "industry": "--- Reference ---", "universe": "--- Reference ---"}
UNKNOWN = "unknown"

def get_tickers_from_crypto_info(tickers):
    connection = PROP.DB.DB_CRYPTO()
    universe = "N/A"
    sql = """SELECT symbol, category as sector, category as industry 
             FROM crypto_info 
             WHERE active_flag = 1 
             AND symbol != 'BTC' 
             AND category != 'Stablecoin' """
    df = pd.read_sql(sql, connection)
    secs = {}
    for index, row in df.iterrows():
        sec = {}
        sec["ticker"] = row["symbol"]
        sec["sector"] = row["sector"]
        sec["industry"] = row["industry"]
        sec["universe"] = universe
        secs[sec["ticker"]] = sec
    tickers.update(secs)
    return tickers

def get_price_from_hkg_stock_info(tickers, start_date, end_date):
    connection = PROP.DB.DB_CRYPTO()
    cursor = connection.cursor()
    sql = "select open_price as Open, high_price as High, low_price as Low, close_price as Close, volume as Volume, TIMESTAMP(update_date) as Update_date from crypto_daily_price where symbol = %s and update_date >= %s and update_date <= %s"
    df = pd.read_sql(sql, connection, params=[tickers, start_date, end_date])
    return df

def get_resolved_securities():
    tickers = {REFERENCE_TICKER: REF_TICKER}
    return get_tickers_from_crypto_info(tickers)

SECURITIES = get_resolved_securities().values()

def write_to_file(dict, file):
    with open(file, "w", encoding='utf8') as fp:
        json.dump(dict, fp, ensure_ascii=False)

def write_price_history_file(tickers_dict):
    write_to_file(tickers_dict, PRICE_DATA_FILE)

def write_ticker_info_file(info_dict):
    write_to_file(info_dict, TICKER_INFO_FILE)

def enrich_ticker_data(ticker_response, security):
    ticker_response["sector"] = security["sector"]
    ticker_response["industry"] = security["industry"]
    ticker_response["universe"] = security["universe"]

def print_data_progress(ticker, universe, idx, securities, error_text, elapsed_s, remaining_s):
    dt_ref = datetime.fromtimestamp(0)
    dt_e = datetime.fromtimestamp(elapsed_s)
    elapsed = dateutil.relativedelta.relativedelta (dt_e, dt_ref)
    if remaining_s and not np.isnan(remaining_s):
        dt_r = datetime.fromtimestamp(remaining_s)
        remaining = dateutil.relativedelta.relativedelta (dt_r, dt_ref)
        remaining_string = f'{remaining.hours}h {remaining.minutes}m {remaining.seconds}s'
    else:
        remaining_string = "?"
    print(f'{ticker} from {universe}{error_text} ({idx+1} / {len(securities)}). Elapsed: {elapsed.hours}h {elapsed.minutes}m {elapsed.seconds}s. Remaining: {remaining_string}.')

def get_remaining_seconds(all_load_times, idx, len):
    load_time_ma = pd.Series(all_load_times).rolling(np.minimum(idx+1, 25)).mean().tail(1).item()
    remaining_seconds = (len - idx) * load_time_ma
    return remaining_seconds

def get_hkstockdb_data(security, start_date, end_date):

        ticker_data = {}
        ticker = security["ticker"]
        df = get_price_from_hkg_stock_info(ticker, start_date, end_date)
        db_stock_data = df.to_dict()
        timestamps = list(db_stock_data["Update_date"].values())
        timestamps = list(map(lambda timestamp: int(timestamp.timestamp()), timestamps))
        opens = list(db_stock_data["Open"].values())
        closes = list(db_stock_data["Close"].values())
        lows = list(db_stock_data["Low"].values())
        highs = list(db_stock_data["High"].values())
        volumes = list(db_stock_data["Volume"].values())
        candles = []

        for i in range(0, len(opens)):
            candle = {}
            candle["open"] = opens[i]
            candle["close"] = closes[i]
            candle["low"] = lows[i]
            candle["high"] = highs[i]
            candle["volume"] = volumes[i]
            candle["datetime"] = timestamps[i]
            candles.append(candle)

        ticker_data["candles"] = candles
        enrich_ticker_data(ticker_data, security)

        return ticker_data

def load_prices_from_database(securities, info = {}):
    print("*** Loading Stocks from database ***")
    today = date.today()
    start = time.time()
    start_date = today - dt.timedelta(days=1 * 365 )  # 183 = 6 months
    tickers_dict = {}
    load_times = []
    for idx, security in enumerate(securities):
        ticker = security["ticker"]
        ticker_data = get_hkstockdb_data(security, start_date, today)
        r_start = time.time()
        now = time.time()
        current_load_time = now - r_start
        load_times.append(current_load_time)
        remaining_seconds = remaining_seconds = get_remaining_seconds(load_times, idx, len(securities))
        print_data_progress(ticker, security["universe"], idx, securities, "", time.time() - start, remaining_seconds)
        tickers_dict[ticker] = ticker_data
    write_price_history_file(tickers_dict)

def save_data(securities, info = {}):
    load_prices_from_database(securities, info)

def main():
    save_data(SECURITIES)
    write_ticker_info_file(TICKER_INFO_DICT)

if __name__ == "__main__":
    main()
