import pandas as pd
from io import StringIO
import pymysql
import os
import json


def load_config():
    """加载配置文件

    返回:
        dict: 配置字典
    """
    config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config/trading_pairs.json')
    with open(config_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def create_db_connection(db_config=None):
    """创建数据库连接

    参数:
        db_config: 数据库配置字典，为None时从配置文件加载

    返回:
        pymysql.Connection: 数据库连接对象
    """
    if db_config is None:
        config = load_config()
        db_config = config['database_config']

    try:
        connection = pymysql.connect(
            host=db_config['host'],
            port=db_config.get('port', 3306),
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database'],
            charset=db_config['charset'],
            cursorclass=pymysql.cursors.DictCursor
        )
        return connection
    except Exception as e:
        print("数据库连接失败: %s" % e)
        return None


def parse_kline_data(data_string):
    """解析CSV格式的K线数据字符串

    参数:
        data_string: CSV格式的K线数据字符串

    返回:
        pd.DataFrame: 处理后的K线数据
    """
    df = pd.read_csv(data_string)

    if 'ts' not in df.columns:
        raise ValueError("数据中缺少时间戳列 'ts'")

    numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'vol_ccy', 'vol_ccy_quote', 'confirm']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    df['ts'] = pd.to_datetime(df['ts'])

    if 'confirm' in df.columns:
        df = df[df['confirm'] == 1].reset_index(drop=True)

    if 'open' in df.columns:
        df = df.rename(columns={
            'open': 'o', 'high': 'h', 'low': 'l',
            'close': 'c', 'volume': 'vol'
        })

    return df


def load_kline_from_file(file_path):
    """从文件加载K线数据

    参数:
        file_path: 文件路径

    返回:
        pd.DataFrame: K线数据
    """
    with open(file_path, 'r') as f:
        data = f.read()
    return parse_kline_data(data)


def load_kline_from_db(currency='BTC-USDT', table_name='sorted_history_15m', db_config=None):
    """从数据库加载指定币种的K线数据

    参数:
        currency: 币种标识（如 BTC-USDT）
        table_name: 数据表名
        db_config: 数据库配置字典，为None时从配置文件加载

    返回:
        pd.DataFrame: 处理后的K线数据
    """
    connection = create_db_connection(db_config)
    if not connection:
        raise Exception("无法连接到数据库")

    try:
        with connection.cursor() as cursor:
            sql = """
            SELECT ts, open, high, low, close, volume, vol_ccy, vol_ccy_quote, confirm
            FROM %s
            WHERE currency = %%s
            ORDER BY ts ASC
            """ % table_name
            cursor.execute(sql, (currency,))
            results = cursor.fetchall()

        df = pd.DataFrame(results)

        if df.empty:
            print("警告: 数据库中没有 %s 的数据" % currency)
            return df

        df['ts'] = pd.to_datetime(df['ts'])

        if 'confirm' in df.columns:
            df = df[df['confirm'] == 1].reset_index(drop=True)

        df = df.rename(columns={
            'open': 'o', 'high': 'h', 'low': 'l',
            'close': 'c', 'volume': 'vol'
        })

        print("从数据库加载了 %d 条 %s 数据" % (len(df), currency))
        return df

    except Exception as e:
        print("从数据库加载数据失败: %s" % e)
        raise
    finally:
        connection.close()


def load_kline_from_db_by_time_range(currency='BTC-USDT', table_name='sorted_history_15m',
                                     start_time=None, end_time=None, db_config=None):
    """从数据库加载指定币种和时间范围的K线数据

    参数:
        currency: 币种标识（如 BTC-USDT）
        table_name: 数据表名
        start_time: 开始时间（datetime对象或字符串），为None时不限制
        end_time: 结束时间（datetime对象或字符串），为None时不限制
        db_config: 数据库配置字典

    返回:
        pd.DataFrame: 处理后的K线数据
    """
    connection = create_db_connection(db_config)
    if not connection:
        raise Exception("无法连接到数据库")

    try:
        with connection.cursor() as cursor:
            sql = """
            SELECT ts, open, high, low, close, volume, vol_ccy, vol_ccy_quote, confirm
            FROM %s
            WHERE currency = %%s
            """ % table_name

            params = [currency]

            if start_time:
                sql += " AND ts >= %s"
                if isinstance(start_time, str):
                    start_time = pd.to_datetime(start_time)
                params.append(start_time)

            if end_time:
                sql += " AND ts <= %s"
                if isinstance(end_time, str):
                    end_time = pd.to_datetime(end_time)
                params.append(end_time)

            sql += " ORDER BY ts ASC"

            cursor.execute(sql, params)
            results = cursor.fetchall()

        df = pd.DataFrame(results)

        if df.empty:
            print("警告: 数据库中没有 %s 在指定时间范围内的数据" % currency)
            return df

        df['ts'] = pd.to_datetime(df['ts'])

        if 'confirm' in df.columns:
            df = df[df['confirm'] == 1].reset_index(drop=True)

        df = df.rename(columns={
            'open': 'o', 'high': 'h', 'low': 'l',
            'close': 'c', 'volume': 'vol'
        })

        print("从数据库加载了 %d 条 %s 数据 (时间范围: %s 到 %s)" % (
            len(df), currency,
            df['ts'].min().strftime('%Y-%m-%d %H:%M:%S'),
            df['ts'].max().strftime('%Y-%m-%d %H:%M:%S')
        ))
        return df

    except Exception as e:
        print("从数据库加载数据失败: %s" % e)
        raise
    finally:
        connection.close()


def save_optimization_results(results_df, file_path='optimization_results.csv', save_all=True):
    """保存参数优化结果到CSV文件

    参数:
        results_df: 优化结果DataFrame
        file_path: 保存路径（默认：当前目录下的optimization_results.csv）
        save_all: 是否保存所有参数组合（否则仅保存前5名）
    """
    if results_df.empty:
        print("警告: 无有效结果可保存")
        return

    selected_columns = [
        'short_window', 'long_window', 'buy_ratio', 'sell_ratio',
        'total_return', 'final_portfolio', 'trade_count',
        'max_drawdown', 'win_rate', 'avg_return'
    ]

    df_to_save = results_df if save_all else results_df.head()

    import time
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    file_name = f"data/{timestamp}_{file_path}" if save_all else file_path
    df_to_save[selected_columns].to_csv(
        file_name,
        index=False,
        float_format="%.4f"
    )

    print(f"\n数据已保存到: {file_name}")
    print(f"保存列: {', '.join(selected_columns)}")
