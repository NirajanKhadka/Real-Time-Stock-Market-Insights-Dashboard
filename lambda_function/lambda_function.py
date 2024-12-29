import os
import json
import requests
import psycopg2
from datetime import datetime
import logging

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

import logging

def lambda_handler(event, context):
    logging.basicConfig(level=logging.INFO)
    
    # Connect to the database
    try:
        conn = psycopg2.connect(
            dbname=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port
        )
        cursor = conn.cursor()
        logging.info("Database connection established.")
    except Exception as e:
        logging.error(f"Error connecting to database: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps("Failed to connect to database.")
        }

    # Fetch today's data for each stock
    for stock in stocks:
        logging.info(f"Fetching today's data for {stock}...")
        stock_data = fetch_today_data(stock)
        if stock_data:
            for timestamp, values in stock_data.items():
                insert_stock_data(cursor, stock, timestamp, values)
                logging.info(f"Inserted data for {stock} on {timestamp}.")
        else:
            logging.warning(f"No data available for {stock} today.")

    # Commit and close the connection
    try:
        conn.commit()
        logging.info("Today's data successfully committed to the database.")
    except Exception as e:
        logging.error(f"Error committing data to database: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps("Failed to commit data.")
        }
    finally:
        cursor.close()
        conn.close()

    return {
        'statusCode': 200,
        'body': json.dumps("Today's stock data has been updated successfully.")
    }
