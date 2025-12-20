import pymysql
import csv
import os
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

class DataExporter:
    def __init__(self):
        # 从环境变量获取数据库连接参数
        self.host = os.getenv('MYSQL_CONN')
        self.user = os.getenv('MYSQL_USER')
        self.password = os.getenv('MYSQL_PASS')
        self.database = 'trading_db'
        self.connection = None

    def connect(self):
        """建立数据库连接"""
        try:
            self.connection = pymysql.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database,
                cursorclass=pymysql.cursors.DictCursor
            )
            print(f"成功连接到数据库: {self.database}")
            return True
        except pymysql.Error as e:
            print(f"数据库连接错误: {e}")
            return False

    def disconnect(self):
        """关闭数据库连接"""
        if self.connection:
            self.connection.close()
            self.connection = None
            print("数据库连接已关闭")

    def export_fields(self, table_name, field1, field2, output_file='exported_data.csv', where_clause=''):
        """导出指定表中的两个字段到CSV文件

        参数:
            table_name: 表名
            field1: 第一个字段名
            field2: 第二个字段名
            output_file: 输出的CSV文件名
            where_clause: 可选的WHERE子句，用于过滤数据，例如 'WHERE id > 10'

        返回:
            成功返回True，失败返回False
        """
        if not self.connect():
            return False

        try:
            with self.connection.cursor() as cursor:
                # 构建查询语句
                query = f"SELECT {field1}, {field2} FROM {table_name}"
                if where_clause:
                    query += f" {where_clause}"

                print(f"执行查询: {query}")
                cursor.execute(query)
                results = cursor.fetchall()

                if not results:
                    print("没有找到匹配的记录")
                    return True

                # 写入CSV文件
                with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
                    writer = csv.writer(csvfile)
                    # 写入表头
                    writer.writerow([field1, field2])
                    # 写入数据
                    for row in results:
                        writer.writerow([row[field1], row[field2]])

            print(f"成功导出 {len(results)} 条记录到 {output_file}")
            return True
        except pymysql.Error as e:
            print(f"数据库查询错误: {e}")
            return False
        except Exception as e:
            print(f"导出数据错误: {e}")
            return False
        finally:
            self.disconnect()

    def list_tables(self):
        """列出数据库中的所有表"""
        if not self.connect():
            return []

        try:
            with self.connection.cursor() as cursor:
                cursor.execute("SHOW TABLES")
                tables = [table[f'Tables_in_{self.database}'] for table in cursor.fetchall()]
                return tables
        except pymysql.Error as e:
            print(f"查询表错误: {e}")
            return []
        finally:
            self.disconnect()

    def describe_table(self, table_name):
        """查看表结构"""
        if not self.connect():
            return []

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(f"DESCRIBE {table_name}")
                return cursor.fetchall()
        except pymysql.Error as e:
            print(f"查询表结构错误: {e}")
            return []
        finally:
            self.disconnect()

if __name__ == '__main__':
    # 创建导出器实例
    exporter = DataExporter()

    # 列出数据库中的所有表（可选）
    # print("数据库中的表:")
    # tables = exporter.list_tables()
    # for i, table in enumerate(tables, 1):
    #     print(f"{i}. {table}")

    # 示例：导出dca_strategy_state表中的strategy_name和initial_capital字段
    # 取消下面的注释并修改表名和字段名以使用
    
    # 查看表结构（可选）
    # table_name = 'dca_strategy_state'
    # print(f"\n表 {table_name} 的结构:")
    # structure = exporter.describe_table(table_name)
    # for field in structure:
    #     print(f"字段名: {field['Field']}, 类型: {field['Type']}")

    # 导出数据示例
    table_name = 'sorted_history'
    field1 = 'ts'
    field2 = 'open'
    output_file = 'strategy_data.csv'
    # where_clause = 'WHERE initial_capital > 1000'  # 可选的过滤条件
    # 
    exporter.export_fields(table_name, field1, field2, output_file)

    # 请根据实际需求修改上面的示例代码
    print("\n请修改脚本中的示例代码，指定要导出的表名和字段名。")
    print("您可以先取消注释查看表列表和表结构的代码，以了解数据库中的表和字段。")