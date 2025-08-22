import requests
import psycopg2
import json
import os
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_database_connection():
    """Establish connection to PostgreSQL database"""
    try:
        conn = psycopg2.connect(
            host='postgres',
            database='stock_data',
            user='airflow',
            password='airflow'
        )
        return conn
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise

def fetch_stock_data_from_api(symbol='AAPL'):
    """Fetch stock data from Alpha Vantage API"""
    try:
        api_key = os.getenv('ALPHA_VANTAGE_API_KEY')
        print(f"[DEBUG] Using API key: {api_key}")
        if not api_key:
            logger.error("Alpha Vantage API key not found in environment variables")
            print("Alpha Vantage API key not found in environment variables")
            return None
        url = f'https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={symbol}&apikey={api_key}'
        print(f"[DEBUG] Fetching URL: {url}")
        response = requests.get(url, timeout=30)
        print(f"[DEBUG] API response status: {response.status_code}")
        response.raise_for_status()
        data = response.json()
        print(f"[DEBUG] API response data: {data}")
        # Check if API returned an error or no data
        if 'Error Message' in data:
            logger.error(f"API Error: {data['Error Message']}")
            print(f"API Error: {data['Error Message']}")
            return None
        if 'Note' in data:
            logger.warning(f"API Note: {data['Note']}")
            print(f"API Note: {data['Note']}")
            return None
        if 'Global Quote' not in data:
            logger.error("No Global Quote data found in API response")
            print("No Global Quote data found in API response")
            return None
        print(f"[DEBUG] Extracted Global Quote for {symbol}: {data['Global Quote']}")
        return data['Global Quote']
    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed: {e}")
        print(f"API request failed: {e}")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse JSON response: {e}")
        print(f"Failed to parse JSON response: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error fetching stock data: {e}")
        print(f"Unexpected error fetching stock data: {e}")
        return None

def insert_stock_data(conn, stock_data):
    """Insert stock data into PostgreSQL table"""
    try:
        cursor = conn.cursor()
        # Extract data from API response
        symbol = stock_data.get('01. symbol', '')
        open_price = float(stock_data.get('02. open', 0))
        high_price = float(stock_data.get('03. high', 0))
        low_price = float(stock_data.get('04. low', 0))
        price = float(stock_data.get('05. price', 0))
        volume = int(stock_data.get('06. volume', 0))
        latest_trading_day = stock_data.get('07. latest trading day', '')
        previous_close = float(stock_data.get('08. previous close', 0))
        change = float(stock_data.get('09. change', 0))
        change_percent = stock_data.get('10. change percent', '0%').replace('%', '')
        print(f"[DEBUG] Inserting data for {symbol} on {latest_trading_day}")
        # Insert data into table
        insert_query = """
        INSERT INTO stock_prices (
            symbol, open_price, high_price, low_price, current_price, 
            volume, trading_date, previous_close, price_change, 
            change_percent, created_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol, trading_date) DO UPDATE SET
            open_price = EXCLUDED.open_price,
            high_price = EXCLUDED.high_price,
            low_price = EXCLUDED.low_price,
            current_price = EXCLUDED.current_price,
            volume = EXCLUDED.volume,
            previous_close = EXCLUDED.previous_close,
            price_change = EXCLUDED.price_change,
            change_percent = EXCLUDED.change_percent,
            created_at = EXCLUDED.created_at
        """
        cursor.execute(insert_query, (
            symbol, open_price, high_price, low_price, price,
            volume, latest_trading_day, previous_close, change,
            float(change_percent), datetime.now()
        ))
        conn.commit()
        cursor.close()
        logger.info(f"Successfully inserted/updated data for {symbol}")
        print(f"Successfully inserted/updated data for {symbol}")
        return True
    except Exception as e:
        logger.error(f"Failed to insert stock data: {e}")
        print(f"Failed to insert stock data: {e}")
        conn.rollback()
        return False

def fetch_and_store_stock_data():
    """Main function to fetch and store stock data"""
    symbols = ['AAPL', 'GOOGL', 'MSFT', 'TSLA']  # Multiple stocks
    
    try:
        # Get database connection
        conn = get_database_connection()
        
        success_count = 0
        
        for symbol in symbols:
            try:
                logger.info(f"Fetching data for {symbol}")
                
                # Fetch stock data from API
                stock_data = fetch_stock_data_from_api(symbol)
                
                if stock_data:
                    # Insert data into database
                    if insert_stock_data(conn, stock_data):
                        success_count += 1
                    else:
                        logger.warning(f"Failed to store data for {symbol}")
                else:
                    logger.warning(f"No data retrieved for {symbol}")
                    
            except Exception as e:
                logger.error(f"Error processing {symbol}: {e}")
                continue
        
        conn.close()
        
        logger.info(f"Pipeline completed. Successfully processed {success_count}/{len(symbols)} symbols")
        
        if success_count == 0:
            raise Exception("Failed to process any stock data")
            
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise

if __name__ == "__main__":
    fetch_and_store_stock_data()