#!/usr/bin/env python
# -*- coding: utf-8 -*-
import requests
import time
import datetime
import pymysql
from datetime import datetime as dt
import json
import os


def load_config():
    """加载配置文件"""
    config_path = os.path.join(os.path.dirname(__file__), 'config/trading_pairs.json')
    with open(config_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def create_connection(db_config):
    """创建与MySQL数据库的连接

    参数:
        db_config: 数据库配置字典，包含 host, user, password, database, charset

    返回:
        pymysql.Connection: 数据库连接对象，失败返回None
    """
    try:
        connection = pymysql.connect(
            host=db_config['host'],
            port=db_config.get('port', 3306),
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database'],
            charset=db_config['charset'],
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=False
        )
        return connection
    except Exception as e:
        print("数据库连接失败: %s" % e)
        return None


def get_latest_timestamp_from_db(connection, table_name, currency):
    """从数据库获取指定币种的最新时间戳

    参数:
        connection: 数据库连接对象
        table_name: 表名
        currency: 币种标识（如 BTC-USDT）

    返回:
        datetime: 最新时间戳，无数据返回None
    """
    try:
        with connection.cursor() as cursor:
            sql = "SELECT MAX(ts) as latest_ts FROM %s WHERE currency = %%s" % table_name
            cursor.execute(sql, (currency,))
            result = cursor.fetchone()
            if result and result['latest_ts']:
                return result['latest_ts']
            return None
    except Exception as e:
        print("获取最新时间戳失败: %s" % e)
        return None


def fetch_latest_data_from_okx(inst_id, bar, limit, api_url, after_ts=None):
    """从OKX API获取指定币种的最新价格数据

    参数:
        inst_id: 交易对标识（如 BTC-USDT）
        bar: K线周期（如 15m）
        limit: 获取数据条数
        api_url: API基础URL
        after_ts: 起始时间戳（可选）

    返回:
        list: K线数据列表，失败返回None
    """
    try:
        url = "%s/api/v5/market/history-candles" % api_url
        params = {
            "instId": inst_id,
            "bar": bar,
            "limit": str(limit)
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


def save_to_database(connection, data, table_name, currency):
    """将数据保存到数据库

    参数:
        connection: 数据库连接对象
        data: K线数据列表
        table_name: 表名
        currency: 币种标识

    返回:
        int: 成功保存的记录数
    """
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
            confirm = VALUES(confirm)
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
                    currency
                ))

            if records_to_insert:
                cursor.executemany(sql, records_to_insert)
                connection.commit()
                print("成功保存 %d 条记录到数据库表 %s (币种: %s)" % (len(records_to_insert), table_name, currency))
                return len(records_to_insert)
            else:
                print("没有新数据需要保存")
                return 0
    except Exception as e:
        print("保存数据到数据库失败: %s" % e)
        connection.rollback()
        return 0


def fetch_single_currency(currency_config, db_config, api_config, table_name):
    """爬取单个币种的数据

    参数:
        currency_config: 币种配置字典
        db_config: 数据库配置字典
        api_config: API配置字典
        table_name: 表名

    返回:
        bool: 是否成功
    """
    connection = None
    try:
        inst_id = currency_config['symbol']
        bar = currency_config.get('bar', '15m')

        print("\n开始爬取 %s (%s) 的最新价格数据..." % (currency_config['name'], inst_id))
        print("时间周期: %s" % bar)

        connection = create_connection(db_config)
        if not connection:
            print("无法连接到数据库，跳过 %s" % inst_id)
            return False

        latest_ts = get_latest_timestamp_from_db(connection, table_name, inst_id)
        if latest_ts:
            print("数据库中最新的数据时间: %s" % latest_ts.strftime('%Y-%m-%d %H:%M:%S'))
        else:
            print("数据库中没有 %s 数据，将获取最新的 %d 条记录" % (inst_id, api_config['limit']))

        data = fetch_latest_data_from_okx(
            inst_id=inst_id,
            bar=bar,
            limit=api_config['limit'],
            api_url=api_config['base_url'],
            after_ts=latest_ts
        )

        if data is None:
            print("获取 %s 数据失败" % inst_id)
            return False

        if not data:
            print("API返回空数据，%s 没有新的价格数据" % inst_id)
            return True

        print("从OKX API获取到 %d 条 %s 数据" % (len(data), inst_id))

        saved_count = save_to_database(connection, data, table_name, inst_id)

        if saved_count > 0:
            print("%s 数据更新完成！共保存 %d 条新记录" % (inst_id, saved_count))
        else:
            print("%s 没有新数据需要更新" % inst_id)

        return True

    except Exception as e:
        print("爬取 %s 数据时出错: %s" % (currency_config['symbol'], str(e)))
        return False
    finally:
        if connection and connection.open:
            connection.close()


def main():
    """主函数：爬取所有启用的币种数据"""
    try:
        config = load_config()
        db_config = config['database_config']
        api_config = config['api_config']
        table_name = db_config['table_name']

        print("=" * 60)
        print("多币种数据爬取工具")
        print("=" * 60)

        enabled_pairs = [p for p in config['trading_pairs'] if p.get('enabled', True)]
        print("\n启用的交易对数量: %d" % len(enabled_pairs))

        success_count = 0
        fail_count = 0

        for currency_config in enabled_pairs:
            if fetch_single_currency(currency_config, db_config, api_config, table_name):
                success_count += 1
            else:
                fail_count += 1

            print("\n" + "-" * 60)

        print("\n" + "=" * 60)
        print("爬取完成！")
        print("成功: %d, 失败: %d" % (success_count, fail_count))
        print("=" * 60)

    except Exception as e:
        print("程序执行出错: %s" % str(e))


if __name__ == "__main__":
    main()
