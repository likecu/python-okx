#!/usr/bin/env python3.8
import pymysql
import sys

def test_sql_script():
    """
    测试SQL脚本的正确性
    """
    # 数据库配置（请使用环境变量配置）
    db_config = {
        'host': os.environ.get('DB_HOST', '127.0.0.1'),
        'user': 'root',
        'password': os.environ.get('DB_PASSWORD', 'YOUR_DB_PASSWORD'),
        'database': 'okx_data'
    }
    
    try:
        # 连接数据库
        connection = pymysql.connect(
            host=db_config['host'],
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database'],
            cursorclass=pymysql.cursors.DictCursor
        )
        
        print("数据库连接成功！")
        
        # 创建游标
        with connection.cursor() as cursor:
            # 读取创建表脚本
            with open('/Users/aaa/PycharmProjects/python-okx1/database/create_api_keys_table.sql', 'r') as f:
                create_script = f.read()
            
            # 执行创建表脚本
            print("执行创建表脚本...")
            cursor.execute(create_script)
            connection.commit()
            print("表创建成功！")
            
            # 读取初始化数据脚本
            with open('/Users/aaa/PycharmProjects/python-okx1/database/init_api_keys.sql', 'r') as f:
                init_script = f.read()
            
            # 执行初始化数据脚本
            print("执行初始化数据脚本...")
            cursor.execute(init_script)
            connection.commit()
            print("数据初始化成功！")
            
            # 验证数据
            print("验证数据...")
            cursor.execute("SELECT key_type, key_name, priority, is_enabled FROM api_keys ORDER BY key_type, priority ASC")
            result = cursor.fetchall()
            print(f"查询到 {len(result)} 条记录：")
            for row in result:
                print(f"类型: {row['key_type']}, 名称: {row['key_name']}, 优先级: {row['priority']}, 状态: {'启用' if row['is_enabled'] else '禁用'}")
            
        print("测试成功！")
        return True
        
    except Exception as e:
        print(f"测试失败：{str(e)}")
        return False
    finally:
        # 关闭连接
        if 'connection' in locals() and connection.open:
            connection.close()
            print("数据库连接已关闭")

if __name__ == "__main__":
    test_sql_script()
