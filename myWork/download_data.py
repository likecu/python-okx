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
from tqdm import tqdm


def load_config():
    """加载配置文件

    返回:
        dict: 配置字典
    """
    config_path = os.path.join(os.path.dirname(__file__), 'config/trading_pairs.json')
    with open(config_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def create_connection(db_config):
    """创建与MySQL数据库的持久化连接

    参数:
        db_config: 数据库配置字典

    返回:
        pymysql.Connection: 数据库连接对象，失败返回None
    """
    global connection
    try:
        if connection and connection.open:
            return connection
        else:
            connection = None

        if not connection:
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
        print("Database connection failed: %s" % e)
        return None


def create_table_if_not_exists(table_name, db_config):
    """创建表（如果不存在）

    参数:
        table_name: 表名
        db_config: 数据库配置字典

    返回:
        bool: 是否成功
    """
    conn = create_connection(db_config)
    if not conn:
        return False

    try:
        with conn.cursor() as cursor:
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
                PRIMARY KEY (ts, currency)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """ % table_name
            cursor.execute(sql)
        print("Table %s is ready" % table_name)
        return True
    except Exception as e:
        print("Failed to create table: %s" % e)
        return False


def save_to_mysql(df, table_name, currency, db_config):
    """保存DataFrame到MySQL表

    参数:
        df: 包含K线数据的DataFrame
        table_name: 表名
        currency: 币种标识
        db_config: 数据库配置字典

    返回:
        bool: 是否成功
    """
    conn = create_connection(db_config)
    if not conn:
        return False

    try:
        with conn.cursor() as cursor:
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
                    currency
                ))

            cursor.executemany(sql, data)
        conn.commit()
        print("Successfully wrote %d records to MySQL table %s (currency: %s)" % (len(df), table_name, currency))
        return True
    except Exception as e:
        print("Failed to write to MySQL: %s" % e)
        conn.rollback()
        return False


def load_state(state_file_path):
    """加载保存的状态

    参数:
        state_file_path: 状态文件路径

    返回:
        dict: 状态字典，无状态返回None
    """
    if os.path.exists(state_file_path):
        try:
            with open(state_file_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            print("Failed to load state file: %s, will restart download" % e)
    return None


def save_state(state, state_file_path):
    """保存当前状态

    参数:
        state: 状态字典
        state_file_path: 状态文件路径
    """
    try:
        with open(state_file_path, 'w') as f:
            json.dump(state, f)
    except Exception as e:
        print("Failed to save state file: %s" % e)


def fetch_data_with_retry(after_ts, inst_id, bar, limit, api_url, max_retries, retry_delay, random_delay):
    """带重试机制的API请求

    参数:
        after_ts: 起始时间戳
        inst_id: 交易对标识
        bar: K线周期
        limit: 获取数据条数
        api_url: API基础URL
        max_retries: 最大重试次数
        retry_delay: 重试延迟基数
        random_delay: 随机延迟上限

    返回:
        dict: API响应数据
    """
    retries = 0
    while retries < max_retries:
        try:
            url = "%s/api/v5/market/history-candles" % api_url
            params = {
                "instId": inst_id,
                "after": str(after_ts),
                "bar": bar,
                "limit": str(limit)
            }

            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()

            response_data = response.json()

            if response_data["code"] != "0":
                error_msg = "API request failed (Code: %s): %s" % (response_data['code'], response_data['msg'])

                if response_data["code"] == "51054":
                    print("Request timeout, retrying (%d/%d)..." % (retries + 1, max_retries))
                    retries += 1
                    delay = retry_delay * (2 ** retries) + random.uniform(0, random_delay)
                    print("Waiting %.2f seconds before retry..." % delay)
                    time.sleep(delay)
                    continue
                else:
                    raise Exception(error_msg)

            return response_data

        except Exception as e:
            print("API request exception: %s" % str(e))
            retries += 1
            if retries < max_retries:
                delay = retry_delay * (2 ** retries) + random.uniform(0, random_delay)
                print("Waiting %.2f seconds before retry (%d/%d)..." % (delay, retries, max_retries))
                time.sleep(delay)
            else:
                raise Exception("Reached maximum retry count, download interrupted")


def download_single_currency(currency_config, db_config, api_config, table_name, save_path):
    """下载单个币种的历史数据

    参数:
        currency_config: 币种配置字典
        db_config: 数据库配置字典
        api_config: API配置字典
        table_name: 表名
        save_path: 保存路径

    返回:
        bool: 是否成功
    """
    global connection
    try:
        inst_id = currency_config['symbol']
        bar = currency_config.get('bar', '15m')
        time_range_days = currency_config.get('time_range_days', 730)

        state_file_path = os.path.join(save_path, "download_state_%s.json" % inst_id.replace('-', '_'))

        print("\n" + "=" * 60)
        print("开始下载 %s (%s) 历史数据" % (currency_config['name'], inst_id))
        print("=" * 60)

        os.makedirs(save_path, exist_ok=True)

        if not create_table_if_not_exists(table_name, db_config):
            print("Cannot create MySQL table, skip %s" % inst_id)
            return False

        state = load_state(state_file_path)

        if state:
            print("检测到之前的下载进度，继续下载...")
            current_after_ts = state["current_after_ts"]
            total_records = state["total_records"]
            last_saved_time = datetime.datetime.fromtimestamp(state["last_saved_time"] / 1000)
            print("上次保存时间: %s" % last_saved_time.strftime('%Y-%m-%d %H:%M:%S'))
            print("已下载记录数: %d" % total_records)
        else:
            end_time = datetime.datetime.now()
            start_time = end_time - datetime.timedelta(days=time_range_days)
            current_after_ts = int(end_time.timestamp() * 1000)
            total_records = 0

            print("开始下载 %s %s K线数据 (最近 %d 天)" % (inst_id, bar, time_range_days))
            print("目标时间范围: %s 到 %s" % (start_time.strftime('%Y-%m-%d %H:%M:%S'), end_time.strftime('%Y-%m-%d %H:%M:%S')))

        request_count = 0
        start_time_window = time.time()
        batch_data = []
        max_data_limit = 10000000

        while True:
            try:
                request_count += 1
                if request_count > 20:
                    elapsed = time.time() - start_time_window
                    if elapsed < 2:
                        time.sleep(2 - elapsed)
                    request_count = 0
                    start_time_window = time.time()

                response = fetch_data_with_retry(
                    after_ts=current_after_ts,
                    inst_id=inst_id,
                    bar=bar,
                    limit=api_config['limit'],
                    api_url=api_config['base_url'],
                    max_retries=api_config['max_retries'],
                    retry_delay=api_config['retry_delay'],
                    random_delay=api_config['random_delay']
                )

                page_data = response.get("data", [])
                if not page_data:
                    print("API返回空数据，没有更多历史数据")
                    break

                page_data_sorted = sorted(page_data, key=lambda x: int(x[0]))
                batch_data.extend(page_data_sorted)
                total_records += len(page_data)

                oldest_ts_in_page = int(page_data[-1][0])
                oldest_time = datetime.datetime.fromtimestamp(oldest_ts_in_page // 1000)
                print("已获取 %d 条记录 | 最早数据时间: %s | 总记录数: %d" % (len(page_data), oldest_time.strftime('%Y-%m-%d %H:%M:%S'), total_records))

                start_ts = int((datetime.datetime.now() - datetime.timedelta(days=time_range_days)).timestamp() * 1000)
                if oldest_ts_in_page <= start_ts:
                    print("已达到目标时间范围的起始时间")
                    break

                current_after_ts = oldest_ts_in_page - 1

                if total_records >= max_data_limit:
                    print("警告：已达到最大数据限制 (%d 条记录)" % max_data_limit)
                    break

                if len(batch_data) >= 1000:
                    df = pd.DataFrame(
                        batch_data,
                        columns=["ts", "open", "high", "low", "close", "volume", "vol_ccy", "vol_ccy_quote", "confirm"]
                    )
                    df["ts"] = pd.to_datetime(df["ts"].astype(int), unit="ms")
                    df = df.sort_values("ts").reset_index(drop=True)

                    if save_to_mysql(df, table_name, inst_id, db_config):
                        print("成功保存 %d 条记录到MySQL表" % len(batch_data))

                        state = {
                            "current_after_ts": current_after_ts,
                            "total_records": total_records,
                            "last_saved_time": current_after_ts
                        }
                        save_state(state, state_file_path)

                        batch_data = []
                    else:
                        print("保存数据到MySQL失败，退出")
                        return False

                time.sleep(0.1)

            except Exception as e:
                print("发生异常: %s" % str(e))
                print("保存当前状态并退出...")

                state = {
                    "current_after_ts": current_after_ts,
                    "total_records": total_records,
                    "last_saved_time": current_after_ts
                }
                save_state(state, state_file_path)

                if batch_data:
                    df = pd.DataFrame(
                        batch_data,
                        columns=["ts", "open", "high", "low", "close", "volume", "vol_ccy", "vol_ccy_quote", "confirm"]
                    )
                    df["ts"] = pd.to_datetime(df["ts"].astype(int), unit="ms")
                    df = df.sort_values("ts").reset_index(drop=True)

                    if save_to_mysql(df, table_name, inst_id, db_config):
                        print("成功保存剩余 %d 条记录到MySQL表" % len(batch_data))
                    else:
                        print("保存剩余数据到MySQL失败")

                print("程序暂停。可以随时重新运行以继续下载。")
                return False

        if batch_data:
            df = pd.DataFrame(
                batch_data,
                columns=["ts", "open", "high", "low", "close", "volume", "vol_ccy", "vol_ccy_quote", "confirm"]
            )
            df["ts"] = pd.to_datetime(df["ts"].astype(int), unit="ms")
            df = df.sort_values("ts").reset_index(drop=True)

            if save_to_mysql(df, table_name, inst_id, db_config):
                print("成功保存剩余 %d 条记录到MySQL表" % len(batch_data))

                if os.path.exists(state_file_path):
                    os.remove(state_file_path)
                    print("已删除状态文件")

                print("\n%s 数据已完全保存到MySQL表: %s" % (inst_id, table_name))
                print("总记录数: %d" % total_records)
            else:
                print("保存剩余数据到MySQL失败")
        else:
            print("没有剩余数据需要保存")
            print("\n%s 数据已完全保存到MySQL表: %s" % (inst_id, table_name))
            print("总记录数: %d" % total_records)

        return True

    except Exception as e:
        print("下载 %s 数据时出错: %s" % (inst_id, str(e)))
        return False
    finally:
        if connection and connection.open:
            connection.close()


def main():
    """主函数：下载所有启用的币种历史数据"""
    global connection
    connection = None

    try:
        config = load_config()
        db_config = config['database_config']
        api_config = config['api_config']
        table_name = db_config['table_name']
        save_path = "./"

        print("=" * 60)
        print("多币种历史数据下载工具")
        print("=" * 60)

        enabled_pairs = [p for p in config['trading_pairs'] if p.get('enabled', True)]
        print("\n启用的交易对数量: %d" % len(enabled_pairs))

        success_count = 0
        fail_count = 0

        for currency_config in enabled_pairs:
            if download_single_currency(currency_config, db_config, api_config, table_name, save_path):
                success_count += 1
            else:
                fail_count += 1

        print("\n" + "=" * 60)
        print("下载完成！")
        print("成功: %d, 失败: %d" % (success_count, fail_count))
        print("=" * 60)

    except Exception as e:
        print("程序执行出错: %s" % str(e))
    finally:
        if connection and connection.open:
            connection.close()
            print("已关闭数据库连接")


if __name__ == "__main__":
    main()
