# Collect Stock and Financial data from all S&P 500 Companies
The main purpose of this program is to easily collect relevant financial and stock price information for all companies in S&P 500 for data scraping. 
The future development includes further covering all publicly traded companies.

### How to use:

Create an object
```
sp500_data = sp500_data_collector()
```

Get stats for Apple and Facebook
```
sp500_data.get_ticker_stats(tickers_to_get = ['AAPL','FB'])
print(sp500_data.ticker_stats)
```

If you don't specify what companies to get, it will get stats for every company in the S&P 500 company list
```
sp500_data.get_ticker_stats()
print(sp500_data.ticker_stats)
```

#### Other types of information:

Get balance sheets for Apple and Facebook
```
sp500_data.get_tickers_balance_sheets(tickers_to_get = ['AAPL','FB'])
print(sp500_data.tickers_balance_sheets)
```

Get income statements for Apple and Facebook
```
sp500_data.get_tickers_income_statments(tickers_to_get = ['AAPL','FB'])
print(sp500_data.tickers_income_statments)
```

Get stock cash flows info for Apple and Facebook
```
sp500_data.get_tickers_cash_flows(tickers_to_get = ['AAPL','FB'])
print(sp500_data.tickers_cash_flows)
```

Get stock prices for Apple and Facebook, interval can be '1d' for daily, '1wk' for weekly, or '1mo' or monthly
```
sp500_data.get_stock_prices(tickers_to_get = ['AAPL','FB'], start_date = '01/1/2019', end_date = '02/28/2021',interval = '1mo')
print(sp500_data.stock_prices)

```
