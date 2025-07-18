import os
import time

import requests
from dotenv import load_dotenv

from myWork.another.all import process_trade_records


def get_data(user_name):
    url = "https://www.okx.com/priapi/v5/ecotrade/public/community/user/trade-records"
    import time

    # 获取当前时间戳（毫秒级）
    current_time = int(time.time() * 1000)

    # 计算2天前的时间戳（2天 = 2 * 24 * 60 * 60 * 1000毫秒）
    two_days_ago = current_time - 6 * 24 * 60 * 60 * 1000

    params = {
        "uniqueName": user_name,
        "startModify": str(two_days_ago),  # 开始时间：2天前
        "endModify": str(current_time),  # 结束时间：当前时间
        "limit": "1000",
        "t": str(current_time)  # 请求时间戳：当前时间
    }

    with open('header.json', 'r') as file:
        headers_content = file.read()
        # 根据文件内容格式解析 headers（示例为 JSON 格式）
        import json
        headers = json.loads(headers_content)

    response = requests.get(url, params=params, headers=headers)

    response_json = response.json()

    # 处理数据空值和类型转换
    data = response_json["data"]
    for record in data:
        # 数值字段：空字符串转 None，非空转浮点数
        for key in ["px", "sz", "avgPx", "lever", "value"]:
            val = record.get(key, "")
            record[key] = float(val) if val != "" else None
        # 时间戳字段转整数
        for key in ["cTime", "fillTime", "uTime"]:
            record[key] = int(record.get(key, 0))
    return data

    # # 打印响应内容（根据需求处理）
    # print(response.text)
    # response_json = response.json()
    # return response_json


import pymysql
from pymysql import Error

load_dotenv()

# 获取配置
MYSQL_CONN = os.getenv("MYSQL_CONN")
MYSQL_PASS = os.getenv("MYSQL_PASS")


def create_connection():
    """创建与MySQL数据库的连接"""
    try:
        connection = pymysql.connect(
            host=MYSQL_CONN,
            port=3306,
            user='root',
            password=MYSQL_PASS,
            database='trading_db',
            cursorclass=pymysql.cursors.DictCursor
        )
        print('已成功连接到MySQL数据库')
        return connection
    except Error as e:
        print(f"连接数据库时发生错误: {e}")
        return None


def create_table():
    """创建交易记录表（如果不存在）"""
    connection = create_connection()
    if connection is None:
        return

    try:
        with connection.cursor() as cursor:
            create_table_query = """
            CREATE TABLE IF NOT EXISTS trade_records (
                id INT AUTO_INCREMENT PRIMARY KEY,
                ordId VARCHAR(255) NOT NULL UNIQUE,
                alias VARCHAR(255),
                avgPx DECIMAL(18, 8),
                baseName VARCHAR(255),
                cTime BIGINT,
                fillTime BIGINT,
                instFamily VARCHAR(255),
                instId VARCHAR(255),
                instType VARCHAR(255),
                lever DECIMAL(10, 2),
                nickName VARCHAR(255),
                ordType VARCHAR(255),
                posSide VARCHAR(255),
                px DECIMAL(18, 8),
                quoteName VARCHAR(255),
                side VARCHAR(255),
                sz DECIMAL(18, 8),
                uTime BIGINT,
                uly VARCHAR(255),
                uniqueName VARCHAR(255),
                value DECIMAL(18, 8),
                result TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            cursor.execute(create_table_query)
        connection.commit()
        print("交易记录表已创建或已存在")
    except Error as e:
        print(f"创建表时发生错误: {e}")
    finally:
        if connection:
            connection.close()


def save_trade_records_to_mysql(new_data):
    """
    将新交易记录保存到MySQL数据库，并根据ordId去重

    参数：
    new_data (list): 新交易记录列表（字典格式，需包含ordId字段）
    """
    if not new_data:
        print("无新记录需要保存")
        return

    connection = create_connection()
    if connection is None:
        return

    try:
        with connection.cursor() as cursor:
            # 准备插入语句
            columns = ', '.join(new_data[0].keys())
            placeholders = ', '.join(['%s'] * len(new_data[0]))
            insert_query = f"replace INTO trade_records ({columns}) VALUES ({placeholders})"

            # 批量插入数据
            values = []
            for record in new_data:
                # 转换为元组
                record_values = tuple(record.values())
                values.append(record_values)

            cursor.executemany(insert_query, values)
        connection.commit()

        print(f"成功写入 {len(new_data)} 条新记录到MySQL数据库")

        # 处理交易记录（示例函数，可根据需求实现）
        process_trade_records()

    except Error as e:
        # 回滚事务
        connection.rollback()
        print(f"保存记录时发生错误: {e}")
    finally:
        if connection:
            connection.close()



def main():
    try:
        with open("user", "r") as f:
            params = [line.strip() for line in f if line.strip()]  # 去除空行
    except FileNotFoundError:
        print("错误: user文件未找到!")
        exit(1)

    # 遍历参数并定期处理
    while True:
        try:
            for param in params:
                a = get_data(param)
                save_trade_records_to_mysql(a)
        except:

            continue
        print(time.strftime("%Y-%m-%d %H:%M:%S"))  # 输出示例：2025-05-24 14:30:00
        time.sleep(15)  # 处理完所有参数后等待60秒

if __name__ == '__main__':
    main()