#!/usr/bin/env python
# -*- coding: utf-8 -*-
import requests
import time
import datetime
import pymysql
from datetime import datetime as dt


CONFIG = {
    "API_URL": "https://www.okx.com",
    "INST_ID": "BTC-USDT",
    "BAR": "15m",
    "LIMIT": 100
}


DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '!A33b3e561fec',
    'database': 'okx_data',
    'charset': 'utf8mb4'
}


def create_connection():
    """创建与MySQL数据库的连接"""
    try:
        connection = pymysql.connect(
            host=DB_CONFIG['host'],
            port=3306,
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            database=DB_CONFIG['database'],
            charset=DB_CONFIG['charset'],
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=False
        )
        return connection
    except Exception as e:
        print("数据库连接失败: %s" % e)
        return None


def get_latest_timestamp_from_db(connection, table_name):
    """从数据库获取最新的时间戳"""
    try:
        with connection.cursor() as cursor:
            sql = "SELECT MAX(ts) as latest_ts FROM %s" % table_name
            cursor.execute(sql)
            result = cursor.fetchone()
            if result and result['latest_ts']:
                return result['latest_ts']
            return None
    except Exception as e:
        print("获取最新时间戳失败: %s" % e)
        return None


def fetch_latest_data_from_okx(after_ts=None):
    """从OKX API获取最新的价格数据"""
    try:
        url = "%s/api/v5/market/history-candles" % CONFIG["API_URL"]
        params = {
            "instId": CONFIG["INST_ID"],
            "bar": CONFIG["BAR"],
            "limit": str(CONFIG["LIMIT"])
        }
        
        if after_ts:
            after_ts_ms = int(after_ts.timestamp() * 1000)
            params["after"] = str(after_ts_ms)
        
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        
        response_data = response.json()
        
        if response_data["code"] != "0":
            raise Exception("API请求失败 (Code: %s): %s" % (response_data['code'], response_data['msg']))
        
        return response_data.get("data", [])
    except Exception as e:
        print("获取OKX数据失败: %s" % str(e))
        return None


def save_to_database(connection, data, table_name):
    """将数据保存到数据库"""
    try:
        with connection.cursor() as cursor:
            sql = """
            INSERT INTO %s 
            (ts, open, high, low, close, volume, vol_ccy, vol_ccy_quote, confirm, currency)
            VALUES (%%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s)
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
            """ % table_name
            
            records_to_insert = []
            for item in data:
                ts = dt.fromtimestamp(int(item[0]) / 1000)
                records_to_insert.append((
                    ts,
                    float(item[1]),
                    float(item[2]),
                    float(item[3]),
                    float(item[4]),
                    float(item[5]),
                    item[6],
                    float(item[7]),
                    item[8],
                    CONFIG['INST_ID']
                ))
            
            if records_to_insert:
                cursor.executemany(sql, records_to_insert)
                connection.commit()
                print("成功保存 %d 条记录到数据库表 %s" % (len(records_to_insert), table_name))
                return len(records_to_insert)
            else:
                print("没有新数据需要保存")
                return 0
    except Exception as e:
        print("保存数据到数据库失败: %s" % e)
        connection.rollback()
        return 0


def main():
    """主函数"""
    connection = None
    try:
        print("开始爬取最新的价格数据...")
        print("交易对: %s, 时间周期: %s" % (CONFIG['INST_ID'], CONFIG['BAR']))
        
        connection = create_connection()
        if not connection:
            print("无法连接到数据库，程序退出")
            return
        
        table_name = "sorted_history_15m"
        
        latest_ts = get_latest_timestamp_from_db(connection, table_name)
        if latest_ts:
            print("数据库中最新的数据时间: %s" % latest_ts.strftime('%Y-%m-%d %H:%M:%S'))
        else:
            print("数据库中没有数据，将获取最新的 %d 条记录" % CONFIG['LIMIT'])
        
        data = fetch_latest_data_from_okx(latest_ts)
        
        if data is None:
            print("获取数据失败")
            return
        
        if not data:
            print("API返回空数据，没有新的价格数据")
            return
        
        print("从OKX API获取到 %d 条数据" % len(data))
        
        saved_count = save_to_database(connection, data, table_name)
        
        if saved_count > 0:
            print("数据更新完成！共保存 %d 条新记录" % saved_count)
        else:
            print("没有新数据需要更新")
        
    except Exception as e:
        print("程序执行出错: %s" % str(e))
    finally:
        if connection and connection.open:
            connection.close()
            print("已关闭数据库连接")


if __name__ == "__main__":
    main()
