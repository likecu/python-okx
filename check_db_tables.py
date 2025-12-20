import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

# 加载环境变量
load_dotenv()

# 获取数据库配置
db_user = os.getenv('MYSQL_USER')
db_pass = os.getenv('MYSQL_PASS')
db_host = os.getenv('MYSQL_CONN')
db_name = os.getenv('MYSQL_DB')

# 创建数据库连接
engine = create_engine(f'mysql+pymysql://{db_user}:{db_pass}@{db_host}/{db_name}')

# 连接数据库并查询表格
try:
    conn = engine.connect()
    result = conn.execute('SHOW TABLES;')
    
    print('Available tables in the database:')
    tables = []
    for table in result:
        tables.append(table[0])
        print(f'  - {table[0]}')
    
    conn.close()
    
    # 检查是否有sorted_history相关的表格
    sorted_tables = [t for t in tables if 'sorted' in t.lower()]
    if sorted_tables:
        print('\nTables with "sorted" in their name:')
        for table in sorted_tables:
            print(f'  - {table}')
except Exception as e:
    print(f'Error accessing database: {e}')
