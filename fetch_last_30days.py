import os
import json
import requests
import psycopg2
from datetime import datetime,timedelta

# Retrieve configuration values from environment variables
api_key = 'Enter your API key here'
db_name = 'Enter your database name here'
db_user = 'Enter your database username here'
db_password = 'Enter your database password here'
db_host = 'Enter your database host here'
db_port = 'Enter your database port here'

# List of FAANG stocks
stocks = ['META', 'AAPL', 'AMZN', 'NFLX', 'GOOGL']

def fetch_last_30_days(stock):
    """Fetch the last 30 days of stock data."""
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={stock}&apikey={api_key}'
    response = requests.get(url)
    data = response.json()

    if 'Time Series (Daily)' in data:
        time_series = data['Time Series (Daily)']
        recent_data = {
            k: v for k, v in time_series.items()
            if datetime.strptime(k, '%Y-%m-%d') >= datetime.now() - timedelta(days=30)
        }
        return recent_data
    else:
        raise ValueError(f"Failed to fetch data for {stock}: {data.get('Error Message')}")

def insert_stock_data(cursor, stock, timestamp, values):
    """Insert stock data into the database."""
    open_price = values['1. open']
    high_price = values['2. high']
    low_price = values['3. low']
    close_price = values['4. close']
    volume = values['5. volume']
    
    query = """
        INSERT INTO stock_data (symbol, timestamp, open_price, high_price, low_price, close_price, volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol, timestamp) DO NOTHING;
    """
    cursor.execute(query, (stock, timestamp, open_price, high_price, low_price, close_price, volume))

def main():
    # Connect to the database
    conn = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_password,
        host=db_host,
        port=db_port
    )
    cursor = conn.cursor()

    # Fetch and insert data for the last 30 days
    for stock in stocks:
        print(f"Fetching data for {stock}...")
        stock_data = fetch_last_30_days(stock)
        for timestamp, values in stock_data.items():
            insert_stock_data(cursor, stock, timestamp, values)

    # Commit and close the connection
    conn.commit()
    cursor.close()
    conn.close()
    print("Data for the last 30 days successfully loaded into the database.")

if __name__ == '__main__':
    main()