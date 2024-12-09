import boto3
import requests
import pandas as pd
import os
import json
from datetime import datetime
import time


#setting up the environment variables
API_KEY = os.getenv('ALPHA_VANTAGE_API_KEY')
STOCK_SYMBOLS=['AAPL', 'TSLA', 'MSFT']
API_URL_TEMPLATE = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={symbol}&interval=5min&apikey={api_key}'


def fetch_data(symbol):
    api_url = API_URL_TEMPLATE.format(symbol=symbol, api_key=API_KEY)
    response=requests.get(api_url)
    data=response.json()
    if( response.status_code == 200):
        data=response.json()
        if "Time Series ( 30 min)" in data:
            return data["Time Series (30 min)"]   
        else:
            print(f"Error fetching data for {symbol}")
            return None
    else:
        print(f"Error fetching data for {symbol}: {response.status_code}") 
        return None


#processing the data and cleaning it
def process_data(data):
    df=pd.DataFrame(data).T
    df.reset_index(inplace=True)
    df['datetime']=pd.to_datetime(df['index'])  
    df['open']=pd.to_numeric(df['1. open'])
    df['high']=pd.to_numeric(df['2. high'])
    df['low']=pd.to_numeric(df['3. low'])
    df['close']=pd.to_numeric(df['4. close'])
    df['volume']=pd.to_numeric(df['5. volume'])
    df.drop(columns=['index','1. open','2. high','3. low','4. close','5. volume'],inplace=True)

    #sorting the data by datetime
    df.sort_values('datetime',inplace=True)
    return df

#saving the data to s3
def save_to_s3(df,symbol):
    s3_client=boto3.client('s3')

    #converting the dataframe to csv and saving it to s3
    csv_buffer=df.to_csv(index=False)

    s3_client.put_object(
        Bucket='stock-data-mine',
        Key=f'{symbol}/{symbol}_{datetime.now().strftime("%Y%m%d%H%M%S")}.csv',
        Body=csv_buffer
        )
    
    print(f"Data saved to s3 for {symbol}")


#lambda event handler


def lambda_handler(event, context):
    # This function will be invoked by Lambda
    for symbol in STOCK_SYMBOLS:
        print(f"Fetching data for {symbol}")
        data = fetch_data(symbol)
        if data:
            df = process_data(data)
            save_to_s3(df, symbol)
        else:
            print(f"Error fetching data for {symbol}")
    return {
        'statusCode': 200,
        'body': json.dumps('Data fetch and save completed')

    }