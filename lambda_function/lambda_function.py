import os
import json
import requests
import psycopg2
from datetime import datetime

# Environment variables for database and API configuration
db_name = os.environ['DB_NAME']
db_user = os.environ['DB_USER']
db_password = os.environ['DB_PASSWORD']
db_host = os.environ['DB_HOST']
db_port = os.environ['DB_PORT']
api_key = os.environ['API_KEY']

# List of FAANG stocks
stocks = ['META', 'AAPL', 'AMZN', 'NFLX', 'GOOGL']

def fetch_today_data(stock):
    """Fetch today's data for a stock."""
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={stock}&apikey={api_key}'
    response = requests.get(url)
    data = response.json()

    if 'Time Series (Daily)' in data:
        time_series = data['Time Series (Daily)']
        today = datetime.now().strftime('%Y-%m-%d')
        if today in time_series:
            return {today: time_series[today]}
        else:
            print(f"No data available for {stock} on {today}")
            return None
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

def lambda_handler(event, context):
    # Connect to the database
    conn = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_password,
        host=db_host,
        port=db_port
    )
    cursor = conn.cursor()

    for stock in stocks:
        print(f"Fetching today's data for {stock}...")
        stock_data = fetch_today_data(stock)
        if stock_data:
            for timestamp, values in stock_data.items():
                insert_stock_data(cursor, stock, timestamp, values)

    # Commit and close the connection
    conn.commit()
    cursor.close()
    conn.close()
    print("Today's data successfully added to the database.")

    return {
        'statusCode': 200,
        'body': json.dumps("Today's stock data has been updated.")
    }
