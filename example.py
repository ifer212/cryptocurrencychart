# -*- coding: utf-8 -*-
"""
Created on Sat Jul  9 20:24:49 2022
"""

from cryptocurrencychart.api import CryptoCurrencyChartApi

import datetime
import time
import os
import pandas as pd


# Dates periods definition
start_date1 = datetime.date(2017, 1, 1)
end_date1 = datetime.date(2019, 1, 1)

start_date2 = datetime.date(2019, 1, 2)
end_date2 = datetime.date(2021, 1, 1)

start_date3 = datetime.date(2021, 1, 2)
end_date3 = datetime.datetime.today().date()



# Path to save the data
os.makedirs('data', exist_ok=True)  

# Connecting to api
api = CryptoCurrencyChartApi()
coins = api.get_mcap_coins() # Coins orderded by current market cap, only ids
coins = api.get_coins() # List of all coins with symbols and ids

# Downloading following data
data_types = api.get_data_types() # List of datatypes
use_dtypes = ['price',
              'priceReturnOnInvestment',
              'tradeVolume',
              'priceSentiment',
              'supply',
              'openPrice',
              'closePrice',
              'highPrice',
              'lowPrice',
              'marketCap'
              ]

# Loopping through each coin
for c, coin in enumerate(coins[0:500]):
    print(f'Doing {coin["name"]} ({coin["symbol"]})')
    coin_list = []
    filename = 'data/'+coin['name']+'.csv'
    # Checking if file already exists
    if os.path.isfile(filename):
        chist_df = pd.read_csv(filename)
        print('\t File already exists, updating data...')
        start_dates = [datetime.datetime.strptime(chist_df['date'].iloc[-1], "%Y-%m-%d").date()]
        end_dates = [end_date3]
    else:
        start_dates = [start_date1, start_date2, start_date3]
        end_dates = [end_date1, end_date2, end_date3]
    
    # Max. calls to API history are 2 years, splitting in 3 time ranges since 2017
    for t, start_date in enumerate(start_dates):
        if start_dates[t] != end_dates[t]:
            for d, datatype in enumerate(use_dtypes):
                chist_dict = api.view_coin_varhistory(coin["id"], start_dates[t], end_dates[t], data_type = datatype)
                if d==0 and ~os.path.isfile(filename):
                    chist_df = pd.DataFrame(chist_dict['data'])
                else:
                    chist_newdf = pd.DataFrame(chist_dict['data'])
                    if not chist_df.empty:
                        chist_df = chist_df.merge(chist_newdf, on='date', how='outer')
                    else:
                        chist_df = chist_newdf # Avoid breaking if money didnt exist in that time period
            coin_list.append(chist_df)
            time.sleep(1.5)
    if coin_list:
        coin_df = pd.concat(coin_list, ignore_index=True)
        print('\t Saving...')    
        coin_df.to_csv(filename, index=False)
    else:
        print('\t Nothing to update!')
                
