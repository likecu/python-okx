#!/usr/bin/env python
# -*- coding: utf-8 -*-
import requests
import time
import datetime
import pandas as pd
import os
import json
import random
import pymysql
from dotenv import load_dotenv
from tqdm import tqdm

# ======================
# 配置参数
# ======================
CONFIG = {
    "API_URL": "https://www.okx.com",  # OKX API base URL
    "INST_ID": "BTC-USDT",  # trading pair
    "BAR": "15m",  # time granularity (1s/1m/3m/5m/15m/30m/1H/2H/4H/6H/12H/1D etc)
    "LIMIT": 100,  # per page data count (max 100)
    "TIME_RANGE_DAYS": 730,  # time range (days) - 2 years of 15min data
    "MAX_DATA_LIMIT": 10000000,  # max data limit
    "SAVE_PATH": "./",  # data save path (with trailing /)
    "TEMP_FILE": "temp_history_15m.csv",  # temp data file name
    "FINAL_FILE": "sorted_history_15m.csv",  # final sorted file name
    "STATE_FILE": "download_state_15m.json",  # state save file name
    "MAX_RETRIES": 5,  # API max retry count
    "RETRY_DELAY": 5,  # retry delay seconds (base value)
    "RANDOM_DELAY": 3,  # random delay upper limit (avoid request storm)
    "MYSQL_TABLE": "sorted_history_15m"  # MySQL table name
}

# 数据库连接参数
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '!A33b3e561fec',
    'database': 'okx_data',
    'charset': 'utf8mb4'
}

# 全局数据库连接对象
connection = None


def create_connection():
    """创建与MySQL数据库的持久化连接"""
    global connection
    try:
        if connection and connection.open:
            return connection
        else:
            connection = None

        if not connection:
            connection = pymysql.connect(
                host=DB_CONFIG['host'],
                port=3306,
                user=DB_CONFIG['user'],
                password=DB_CONFIG['password'],
                database=DB_CONFIG['database'],
                charset=DB_CONFIG['charset'],
                cursorclass=pymysql.cursors.DictCursor,
                autocommit=False  # manual transaction management
            )
        return connection
    except Exception as e:
        print("Database connection failed: %s" % e)
        return None


def create_table_if_not_exists():
    """Create table if not exists"""
    connection = create_connection()
    if not connection:
        return False

    try:
        with connection.cursor() as cursor:
            # Create table SQL statement
            sql = """
            CREATE TABLE IF NOT EXISTS %s (
                ts DATETIME NOT NULL,
                open DECIMAL(30,15),
                high DECIMAL(30,15),
                low DECIMAL(30,15),
                close DECIMAL(30,15),
                volume DECIMAL(30,15),
                vol_ccy VARCHAR(50),
                vol_ccy_quote DECIMAL(30,15),
                confirm VARCHAR(10),
                currency VARCHAR(20),
                PRIMARY KEY (ts)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """ % CONFIG['MYSQL_TABLE']
            cursor.execute(sql)
        # Don't close connection, keep persistent
        print("Table %s is ready" % CONFIG['MYSQL_TABLE'])
        return True
    except Exception as e:
        print("Failed to create table: %s" % e)
        return False


def save_to_mysql(df):
    """Save DataFrame to MySQL table"""
    connection = create_connection()
    if not connection:
        return False

    try:
        with connection.cursor() as cursor:
            # Prepare SQL statement
            sql = """
            INSERT INTO %s 
            (ts, open, high, low, close, volume, vol_ccy, vol_ccy_quote, confirm,currency)
            VALUES (%%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s,%%s)
            ON DUPLICATE KEY UPDATE
            open = VALUES(open),
            high = VALUES(high),
            low = VALUES(low),
            close = VALUES(close),
            volume = VALUES(volume),
            vol_ccy = VALUES(vol_ccy),
            vol_ccy_quote = VALUES(vol_ccy_quote),
            confirm = VALUES(confirm),
            currency = VALUES(currency)
            """ % CONFIG['MYSQL_TABLE']

            # Prepare data
            data = []
            for _, row in df.iterrows():
                data.append((
                    row['ts'],
                    row['open'],
                    row['high'],
                    row['low'],
                    row['close'],
                    row['volume'],
                    row['vol_ccy'],
                    row['vol_ccy_quote'],
                    row['confirm'],
                    CONFIG['INST_ID']
                ))

            # Batch insert data
            cursor.executemany(sql, data)
        # Commit transaction, but don't close connection
        connection.commit()
        print("Successfully wrote %d records to MySQL table %s" % (len(df), CONFIG['MYSQL_TABLE']))
        return True
    except Exception as e:
        print("Failed to write to MySQL: %s" % e)
        connection.rollback()  # Rollback transaction on error
        return False





# ======================
# Load saved state
# ======================
def load_state():
    state_path = CONFIG["SAVE_PATH"] + CONFIG["STATE_FILE"]
    if os.path.exists(state_path):
        try:
            with open(state_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            print("Failed to load state file: %s, will restart download" % e)
    return None


# ======================
# Save current state
# ======================
def save_state(state):
    state_path = CONFIG["SAVE_PATH"] + CONFIG["STATE_FILE"]
    try:
        with open(state_path, 'w') as f:
            json.dump(state, f)
    except Exception as e:
        print("Failed to save state file: %s" % e)


# ======================
# API request with retry mechanism
# ======================
def fetch_data_with_retry(after_ts):
    retries = 0
    while retries < CONFIG["MAX_RETRIES"]:
        try:
            # Prepare API request URL and parameters
            url = "%s/api/v5/market/history-candles" % CONFIG["API_URL"]
            params = {
                "instId": CONFIG["INST_ID"],
                "after": str(after_ts),
                "bar": CONFIG["BAR"],
                "limit": str(CONFIG["LIMIT"])
            }
            
            # Make API request
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()  # Raise HTTPError for bad responses
            
            # Parse JSON response
            response_data = response.json()

            # Check API response status
            if response_data["code"] != "0":
                error_msg = "API request failed (Code: %s): %s" % (response_data['code'], response_data['msg'])

                # Special handling for timeout error
                if response_data["code"] == "51054":
                    print("Request timeout, retrying (%d/%d)..." % (retries + 1, CONFIG["MAX_RETRIES"]))
                    retries += 1
                    # Exponential backoff strategy
                    delay = CONFIG["RETRY_DELAY"] * (2 ** retries) + random.uniform(0, CONFIG["RANDOM_DELAY"])
                    print("Waiting %.2f seconds before retry..." % delay)
                    time.sleep(delay)
                    continue
                else:
                    raise Exception(error_msg)

            return response_data

        except Exception as e:
            print("API request exception: %s" % str(e))
            retries += 1
            if retries < CONFIG["MAX_RETRIES"]:
                delay = CONFIG["RETRY_DELAY"] * (2 ** retries) + random.uniform(0, CONFIG["RANDOM_DELAY"])
                print("Waiting %.2f seconds before retry (%d/%d)..." % (delay, retries, CONFIG["MAX_RETRIES"]))
                time.sleep(delay)
            else:
                raise Exception("Reached maximum retry count, download interrupted")


# ======================
# Main data fetch program
# ======================
def main():
    global connection
    try:
        # Create save directory
        os.makedirs(CONFIG["SAVE_PATH"], exist_ok=True)

        # Ensure MySQL table exists
        if not create_table_if_not_exists():
            print("Cannot create MySQL table, program exit")
            return

        # Load previous download state
        state = load_state()

        if state:
            print("Detected previous download progress, continuing download...")
            current_after_ts = state["current_after_ts"]
            total_records = state["total_records"]
            last_saved_time = datetime.datetime.fromtimestamp(state["last_saved_time"] / 1000)
            print("Last saved time: %s" % last_saved_time.strftime('%Y-%m-%d %H:%M:%S'))
            print("Downloaded records: %d" % total_records)
        else:
            # Calculate new time range
            end_time = datetime.datetime.now()
            start_time = end_time - datetime.timedelta(days=CONFIG["TIME_RANGE_DAYS"])
            current_after_ts = int(end_time.timestamp() * 1000)
            total_records = 0

            print("Start new download of %s %s K-line data (last %d days)" % (CONFIG['INST_ID'], CONFIG['BAR'], CONFIG['TIME_RANGE_DAYS']))
            print(
                "Target time range: %s to %s" % (start_time.strftime('%Y-%m-%d %H:%M:%S'), end_time.strftime('%Y-%m-%d %H:%M:%S')))

        # Data fetch parameters
        request_count = 0
        start_time_window = time.time()
        batch_data = []

        # Main data fetch loop
        while True:
            try:
                # Handle API rate limit (20 requests/2 seconds)
                request_count += 1
                if request_count > 20:
                    elapsed = time.time() - start_time_window
                    if elapsed < 2:
                        time.sleep(2 - elapsed)
                    request_count = 0
                    start_time_window = time.time()

                # Make API request (with retry mechanism)
                response = fetch_data_with_retry(current_after_ts)

                page_data = response.get("data", [])
                if not page_data:
                    print("API returned empty data, no more historical data available")
                    break

                # Parse data and add to batch (reverse order to make it chronological)
                page_data_sorted = sorted(page_data, key=lambda x: int(x[0]))  # Sort by timestamp ascending
                batch_data.extend(page_data_sorted)
                total_records += len(page_data)

                # Get the oldest timestamp in current page (for next page request)
                oldest_ts_in_page = int(page_data[-1][0])  # Original data is in reverse chronological order

                # Print progress
                oldest_time = datetime.datetime.fromtimestamp(oldest_ts_in_page // 1000)
                print(
                    "Fetched %d records | Oldest data time: %s | Total records: %d" % (len(page_data), oldest_time.strftime('%Y-%m-%d %H:%M:%S'), total_records))

                # Check if reached time range boundary
                start_ts = int(
                    (datetime.datetime.now() - datetime.timedelta(days=CONFIG["TIME_RANGE_DAYS"])).timestamp() * 1000)
                if oldest_ts_in_page <= start_ts:
                    print("Reached start time of target time range")
                    break

                # Update after parameter for next page (subtract 1ms to avoid duplicate fetch)
                current_after_ts = oldest_ts_in_page - 1

                # Check if reached max data limit
                if total_records >= CONFIG["MAX_DATA_LIMIT"]:
                    print("Warning: Reached maximum data limit (%d records)" % CONFIG['MAX_DATA_LIMIT'])
                    break

                # Save data every 1000 records
                if len(batch_data) >= 1000:
                    # Process and save data
                    df = pd.DataFrame(
                        batch_data,
                        columns=["ts", "open", "high", "low", "close", "volume", "vol_ccy", "vol_ccy_quote", "confirm"]
                    )
                    df["ts"] = pd.to_datetime(df["ts"].astype(int), unit="ms")  # Convert timestamp to datetime

                    # Ensure data is in chronological order
                    df = df.sort_values("ts").reset_index(drop=True)

                    # Save to MySQL table
                    if save_to_mysql(df):
                        print("Successfully saved %d records to MySQL table" % len(batch_data))

                        # Save current state
                        state = {
                            "current_after_ts": current_after_ts,
                            "total_records": total_records,
                            "last_saved_time": current_after_ts
                        }
                        save_state(state)

                        # Clear batch data
                        batch_data = []
                    else:
                        print("Failed to save data to MySQL, program exit")
                        return

                # Safe interval (avoid request storm)
                time.sleep(0.1)

            except Exception as e:
                print("Exception occurred: %s" % str(e))
                print("Saving current state and exiting...")

                # Save current state
                state = {
                    "current_after_ts": current_after_ts,
                    "total_records": total_records,
                    "last_saved_time": current_after_ts
                }
                save_state(state)

                # Save remaining batch data
                if batch_data:
                    df = pd.DataFrame(
                        batch_data,
                        columns=["ts", "open", "high", "low", "close", "volume", "vol_ccy", "vol_ccy_quote", "confirm"]
                    )
                    df["ts"] = pd.to_datetime(df["ts"].astype(int), unit="ms")
                    df = df.sort_values("ts").reset_index(drop=True)

                    if save_to_mysql(df):
                        print("Successfully saved remaining %d records to MySQL table" % len(batch_data))
                    else:
                        print("Failed to save remaining data to MySQL")

                print("Program paused. You can run it again at any time to continue downloading.")
                return

        # Process remaining data
        if batch_data:
            df = pd.DataFrame(
                batch_data,
                columns=["ts", "open", "high", "low", "close", "volume", "vol_ccy", "vol_ccy_quote", "confirm"]
            )
            df["ts"] = pd.to_datetime(df["ts"].astype(int), unit="ms")

            # Ensure data is in chronological order
            df = df.sort_values("ts").reset_index(drop=True)

            if save_to_mysql(df):
                print("Successfully saved remaining %d records to MySQL table" % len(batch_data))

                # Delete state file, indicating download completion
                state_path = CONFIG["SAVE_PATH"] + CONFIG["STATE_FILE"]
                if os.path.exists(state_path):
                    os.remove(state_path)
                    print("Deleted state file")

                print("\nData has been completely saved to MySQL table: %s" % CONFIG['MYSQL_TABLE'])
                print("Total records: %d" % total_records)
            else:
                print("Failed to save remaining data to MySQL")
        else:
            print("No remaining data to save")
            print("\nData has been completely saved to MySQL table: %s" % CONFIG['MYSQL_TABLE'])
            print("Total records: %d" % total_records)

    finally:
        # Close database connection when program ends
        if connection and connection.open:
            connection.close()
            print("Closed database connection")


if __name__ == "__main__":
    main()
