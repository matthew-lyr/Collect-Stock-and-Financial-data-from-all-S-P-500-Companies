import urllib.request, json 
from io import StringIO
import pandas as pd
import pandas_datareader.data as web
import pandas_gbq
import numpy as np
import datetime
import yahoo_fin.stock_info as si
from yahoo_fin.stock_info import *
import logging
import csv

class sp500_data_collector:
    def __init__(self):
        self.tickers_summary = self.get_tickers_list()
        self.tickers_list = [ticker for ticker in self.tickers_summary['Symbol']]
      
    def get_tickers_list(self, source = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'):
        payload      = pd.read_html(source)
        first_table  = payload[0]
        second_table = payload[1]
        return first_table    
    
    def get_data_func(self, func, tickers_to_get, retries = 3, adjust_header = True, transpose = True, only_take_latest = True):
        uncollected = []
        stats = pd.DataFrame()

        if tickers_to_get == "default":
            tickers_list = self.tickers_list
        elif isinstance(tickers_to_get, list):
            tickers_list = tickers_to_get
        else:
            tickers_list = [tickers_to_get]
            
        for ticker in tickers_list:
            try:
                print("Collecting {}".format(ticker))
                if transpose:
                    stat = pd.DataFrame(func(ticker).transpose())
                else:
                    stat = pd.DataFrame(func(ticker))
                if adjust_header:
                    stat.columns = [name.replace("(","_").replace(")","_").replace(" ","_").replace("/","_over_") for name in stat.iloc[0]]
                stat['ticker'] = ticker
                if only_take_latest:
                    stat = stat.iloc[[1]]
                stat['create_date'] = datetime.datetime.now()
                stats = stats.append(stat)
            except Exception:
                print("cannot get {}".format(ticker))
                uncollected.append(ticker)
        
        if len(uncollected)>0 or retries>0:
            retries = retries - 1
            for i in range(len(uncollected)):
                ticker = uncollected.pop()
                try:
                    if transpose:
                        stat = pd.DataFrame(func(ticker).transpose())
                    else:
                        stat = pd.DataFrame(func(ticker))
                    if adjust_header:
                        stat.columns = [name.replace("(","_").replace(")","_").replace(" ","_").replace("/","_over_") for name in stat.iloc[0]]
                    stat['ticker'] = ticker
                    if only_take_latest:
                        stat = stat.iloc[[1]]
                    stat['create_date'] = datetime.datetime.now()
                    stats = stats.append(stat)
                except Exception:
                    print("cannot get {}".format(ticker))
                    uncollected.append(ticker)
        return stats
                    
    def get_ticker_stats(self, tickers_to_get = "default", retries = 3):
        self.ticker_stats = self.get_data_func(get_stats_valuation, tickers_to_get, retries)
        
    def get_tickers_balance_sheets(self, tickers_to_get = "default", retries = 3):
        self.tickers_balance_sheets = self.get_data_func(get_balance_sheet, tickers_to_get, retries, adjust_header = False)                   
        
    def get_tickers_income_statments(self, tickers_to_get = "default", retries = 3):
        self.tickers_income_statments = self.get_data_func(get_income_statement, tickers_to_get, retries, adjust_header = False)
    
    def get_tickers_cash_flows(self, tickers_to_get = "default", retries = 3):
        self.tickers_cash_flows = self.get_data_func(get_cash_flow, tickers_to_get, retries, adjust_header = False)
  
    def get_stock_prices(self, tickers_to_get = "default", start_date = datetime.datetime.now().strftime("%m/%d/%Y"), end_date = datetime.datetime.now().strftime("%m/%d/%Y"), interval = '1d',  retries = 3):
        func = lambda ticker: get_data(ticker, start_date = start_date, end_date = end_date, interval = interval,)
        self.stock_prices = self.get_data_func(lambda ticker: func(ticker), tickers_to_get, retries, adjust_header = False, transpose = False, only_take_latest = False)





