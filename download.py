import os.path
import pandas as pd
import requests
import multitasking
import json
import csv
import numpy as np
import os
import datetime as dt
from tools import ProgressBar

today = dt.date.today().strftime("%Y-%m-%d")


def parse_quotes(data):
    timestamps = data["timestamp"]
    ticker = data['meta']['symbol']
    ohlc = data["indicators"]["quote"][0]
    closes, volumes, open, high, low = ohlc["close"], ohlc["volume"], ohlc["open"], ohlc["high"], ohlc["low"]
    try:
        adjclose = data["indicators"]["adjclose"][0]["adjclose"]
    except:
        adjclose = closes

    # quotes = pd.DataFrame({"Adj Close": adjclose, "Volume": volumes, "Close": closes})
    quotes = pd.DataFrame({"adj_close": adjclose, "volume": volumes, "open": open, "high": high, "low": low, "close": closes})
    quotes['Ticker'] = ticker
    quotes.index = pd.to_datetime(timestamps, unit="s").date
    quotes.sort_index(inplace=True)

    return quotes


def download_one(ticker: str, interval: str = "1d", period: str = "1y"):
    """
    Download historical data for a single ticker.

    Parameters
    ----------
    ticker: str
        Ticker for which to download historical information.
    interval: str
        Frequency between data.
    period: str
        Data period to download.

    Returns
    -------
    data: dict
        Scraped dictionary of information.
    """
    base_url = 'https://query1.finance.yahoo.com'

    params = dict(range=period, interval=interval.lower(), includePrePost=False)

    url = "{}/v8/finance/chart/{}".format(base_url, ticker)
    data = requests.get(url=url, params=params)

    if "Will be right back" in data.text:
        raise RuntimeError("*** YAHOO! FINANCE is currently down! ***\n")

    data = data.json()
    return data
