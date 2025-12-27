#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
sys.path.insert(0, '/root/python-okx/python-okx')

from myWork.process.read import load_kline_from_db, create_db_connection
import pymysql

print("测试数据库连接...")
conn = create_db_connection()
if conn:
    print("数据库连接成功!")
    
    with conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) as cnt FROM sorted_history_15m WHERE currency = %s", ('BTC-USDT',))
        result = cursor.fetchone()
        print(f"BTC-USDT 记录数: {result['cnt']}")
        
        cursor.execute("SELECT ts, currency, open, high, low, close FROM sorted_history_15m WHERE currency = %s LIMIT 3", ('BTC-USDT',))
        results = cursor.fetchall()
        print(f"查询到 {len(results)} 条记录:")
        for r in results:
            print(f"  {r}")
    conn.close()
else:
    print("数据库连接失败!")

print("\n测试 load_kline_from_db 函数...")
df = load_kline_from_db(currency='BTC-USDT', table_name='sorted_history_15m')
print(f"加载的数据形状: {df.shape}")
if not df.empty:
    print(f"前3行:\n{df.head(3)}")
else:
    print("数据为空!")
