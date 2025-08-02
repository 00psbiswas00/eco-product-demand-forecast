from pytrends.request import TrendReq
import pandas as pd
import sqlite3
import os
import time

pytrends= TrendReq(hl='en-US', tz=330)

kw_list = [
    "sustainable fashion",
    "eco-friendly clothing",
    "organic cotton",
    "eco-friendly products",
    "biodegradable packaging",
    "zero waste products",
    "green energy",
    "solar panels",
    "electric scooter",
    "organic food",
    "natural skincare"
]

def fetch_trends(kw_list):
    all_data=[]
    for i in range(0, len(kw_list),5):
        batch= kw_list[i:i+5] #load 5 items into the list by slicing 
        pytrends.build_payload(batch, timeframe="today 12-m")
        time.sleep(2) 
        data= pytrends.interest_over_time()
        all_data.append(data)
    return pd.concat(all_data)
    

if __name__=="__main__":
    trend_data= fetch_trends(kw_list)
    print(trend_data.head())


