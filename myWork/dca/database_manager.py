import pymysql
# 由于 datetime 导入项未使用，将其移除，不添加新的导入代码
import time  # 添加此行


class DatabaseManager:
    def __init__(self, host, user, password, database):
        """初始化数据库连接参数"""
        self.host = host
        self.user = user
        self.password = password
        self.database = database
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
            return True
        except pymysql.Error as e:
            print(f"数据库连接错误: {e}")
            return False

    def disconnect(self):
        """关闭数据库连接"""
        if self.connection:
            self.connection.close()
            self.connection = None

    def create_tables(self):
        """创建必要的数据库表"""
        if not self.connect():
            return False

        try:
            with self.connection.cursor() as cursor:
                # 创建策略状态表
                cursor.execute('''
                CREATE TABLE IF NOT EXISTS dca_strategy_state (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    strategy_name VARCHAR(100) NOT NULL,
                    price_drop_threshold DECIMAL(5,4) NOT NULL,
                    max_time_since_last_trade INT NOT NULL,
                    min_time_since_last_trade INT NOT NULL,
                    take_profit_threshold DECIMAL(5,4) NOT NULL,
                    initial_capital DECIMAL(15,4) NOT NULL,
                    initial_investment_ratio DECIMAL(5,4) NOT NULL,
                    initial_dca_value DECIMAL(5,4) NOT NULL,
                    buy_fee_rate DECIMAL(5,4) NOT NULL,
                    sell_fee_rate DECIMAL(5,4) NOT NULL,
                    cash_balance DECIMAL(15,4) NOT NULL,
                    position DECIMAL(15,8) NOT NULL,
                    avg_price DECIMAL(15,8) NOT NULL,
                    last_trade_time DATETIME,
                    last_trade_price DECIMAL(15,8),
                    peak_value DECIMAL(15,4) NOT NULL,
                    initial_dca_amount DECIMAL(15,4),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                )
                ''')

                # 创建交易记录表
                cursor.execute('''
                CREATE TABLE IF NOT EXISTS dca_trades (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    strategy_id INT NOT NULL,
                    trade_time DATETIME NOT NULL,
                    trade_type ENUM('INITIAL_BUY', 'DCA', 'TAKE_PROFIT') NOT NULL,
                    price DECIMAL(15,8) NOT NULL,
                    position DECIMAL(15,8) NOT NULL,
                    cash DECIMAL(15,4) NOT NULL,
                    portfolio_value DECIMAL(15,4) NOT NULL,
                    fee DECIMAL(15,4) NOT NULL,
                    amount DECIMAL(15,4) NOT NULL,
                    side ENUM('buy', 'sell') NOT NULL,
                    dca_amount DECIMAL(15,4),
                    profit DECIMAL(15,4),
                    FOREIGN KEY (strategy_id) REFERENCES dca_strategy_state(id)
                )
                ''')

            self.connection.commit()
            return True
        except pymysql.Error as e:
            print(f"创建表错误: {e}")
            self.connection.rollback()
            return False
        finally:
            self.disconnect()

    def save_strategy_state(self, strategy_name, strategy_params, portfolio, initial_dca_amount=None):
        """保存策略状态到数据库"""
        if not self.connect():
            return None

        try:
            with self.connection.cursor() as cursor:
                # 检查策略是否已存在
                cursor.execute("SELECT id FROM dca_strategy_state WHERE strategy_name = %s", (strategy_name,))
                result = cursor.fetchone()

                if result is not None:
                    # 更新现有策略
                    strategy_id = result[0]
                    query = '''
                    UPDATE dca_strategy_state SET
                    price_drop_threshold = %s,
                    max_time_since_last_trade = %s,
                    min_time_since_last_trade = %s,
                    take_profit_threshold = %s,
                    initial_capital = %s,
                    initial_investment_ratio = %s,
                    initial_dca_value = %s,
                    buy_fee_rate = %s,
                    sell_fee_rate = %s,
                    cash_balance = %s,
                    position = %s,
                    avg_price = %s,
                    last_trade_time = %s,
                    last_trade_price = %s,
                    peak_value = %s,
                    initial_dca_amount = %s
                    WHERE id = %s
                    '''
                    cursor.execute(query, (
                        strategy_params['price_drop_threshold'],
                        strategy_params['max_time_since_last_trade'],
                        strategy_params['min_time_since_last_trade'],
                        strategy_params['take_profit_threshold'],
                        strategy_params['initial_capital'],
                        strategy_params['initial_investment_ratio'],
                        strategy_params['initial_dca_value'],
                        strategy_params['buy_fee_rate'],
                        strategy_params['sell_fee_rate'],
                        portfolio['cash'],
                        portfolio['position'],
                        portfolio['avg_price'],
                        portfolio['last_trade_time'],
                        portfolio['last_trade_price'],
                        portfolio['peak_value'],
                        initial_dca_amount,
                        strategy_id
                    ))
                else:
                    # 插入新策略
                    query = '''
                    INSERT INTO dca_strategy_state 
                    (strategy_name, price_drop_threshold, max_time_since_last_trade, 
                     min_time_since_last_trade, take_profit_threshold, initial_capital, 
                     initial_investment_ratio, initial_dca_value, buy_fee_rate, sell_fee_rate,
                     cash_balance, position, avg_price, last_trade_time, last_trade_price, 
                     peak_value, initial_dca_amount)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    '''
                    cursor.execute(query, (
                        strategy_name,
                        strategy_params['price_drop_threshold'],
                        strategy_params['max_time_since_last_trade'],
                        strategy_params['min_time_since_last_trade'],
                        strategy_params['take_profit_threshold'],
                        strategy_params['initial_capital'],
                        strategy_params['initial_investment_ratio'],
                        strategy_params['initial_dca_value'],
                        strategy_params['buy_fee_rate'],
                        strategy_params['sell_fee_rate'],
                        portfolio['cash'],
                        portfolio['position'],
                        portfolio['avg_price'],
                        portfolio['last_trade_time'],
                        portfolio['last_trade_price'],
                        portfolio['peak_value'],
                        initial_dca_amount
                    ))
                    strategy_id = cursor.lastrowid
                self.connection.commit()
                return strategy_id
        except pymysql.Error as e:
            print(f"保存策略状态错误: {e}")
            self.connection.rollback()
            return None
        finally:
            self.disconnect()

    def save_trade_record(self, strategy_id, trade_info):
        """保存交易记录到数据库"""
        if not self.connect():
            return False

        try:
            with self.connection.cursor() as cursor:
                query = '''
                INSERT INTO dca_trades 
                (strategy_id, trade_time, trade_type, price, position, cash, 
                 portfolio_value, fee, amount, side, dca_amount, profit)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                '''

                cursor.execute(query, (
                    strategy_id,
                    trade_info['time'],
                    trade_info['type'],
                    trade_info['price'],
                    trade_info['position'],
                    trade_info['cash'],
                    trade_info['portfolio_value'],
                    trade_info['fee'],
                    trade_info['amount'],
                    trade_info['side'],
                    trade_info.get('dca_amount', None),
                    trade_info.get('profit', None)
                ))

                self.connection.commit()
                return True
        except pymysql.Error as e:
            print(f"保存交易记录错误: {e}")
            self.connection.rollback()
            return False
        finally:
            self.disconnect()

    def load_strategy_state(self, strategy_name):
        """从数据库加载最新的策略状态"""
        if not self.connect():
            return None

        try:
            with self.connection.cursor() as cursor:
                query = '''
                SELECT * FROM dca_strategy_state 
                WHERE strategy_name = %s 
                ORDER BY id DESC 
                LIMIT 1
                '''

                cursor.execute(query, (strategy_name,))
                result = cursor.fetchone()

                if result:
                    # 获取该策略的所有交易记录
                    cursor.execute('''
                    SELECT * FROM dca_trades 
                    WHERE strategy_id = %s 
                    ORDER BY trade_time ASC
                    ''', (result['id'],))

                    trades = cursor.fetchall()

                    # 转换日期时间格式
                    if result['last_trade_time']:
                        result['last_trade_time'] = result['last_trade_time'].isoformat()

                    return {
                        'strategy_id': result['id'],
                        'strategy_params': {
                            'price_drop_threshold': float(result['price_drop_threshold']),
                            'max_time_since_last_trade': result['max_time_since_last_trade'],
                            'min_time_since_last_trade': result['min_time_since_last_trade'],
                            'take_profit_threshold': float(result['take_profit_threshold']),
                            'initial_capital': float(result['initial_capital']),
                            'initial_investment_ratio': float(result['initial_investment_ratio']),
                            'initial_dca_value': float(result['initial_dca_value']),
                            'buy_fee_rate': float(result['buy_fee_rate']),
                            'sell_fee_rate': float(result['sell_fee_rate'])
                        },
                        'portfolio': {
                            'cash': float(result['cash_balance']),
                            'position': float(result['position']),
                            'avg_price': float(result['avg_price']),
                            'last_trade_time': result['last_trade_time'],
                            'last_trade_price': float(result['last_trade_price']) if result[
                                'last_trade_price'] else None,
                            'peak_value': float(result['peak_value'])
                        },
                        'initial_dca_amount': float(result['initial_dca_amount']) if result[
                            'initial_dca_amount'] else None,
                        'trades': trades
                    }
                return None
        except pymysql.Error as e:
            print(f"加载策略状态错误: {e}")
            return None
        finally:
            self.disconnect()

    def record_trade(self, inst_id, trade_info, order_id, status):
        """记录交易到 trade_records 表"""
        if not self.connect():
            return False

        try:
            with self.connection.cursor() as cursor:
                # 准备交易记录数据
                query = '''
                INSERT INTO trade_records 
                (ordId, instId, side, px, sz, cTime, result)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                '''

                # 获取当前时间戳（毫秒）
                current_timestamp = int(time.time() * 1000)

                cursor.execute(query, (
                    order_id,
                    inst_id,
                    trade_info['side'],
                    trade_info.get('price'),
                    trade_info.get('amount'),
                    current_timestamp,
                    f"status: {status}"
                ))

                self.connection.commit()
                return True
        except pymysql.Error as e:
            print(f"记录交易错误: {e}")
            self.connection.rollback()
            return False
        finally:
            self.disconnect()

    def update_order_status(self, order_id, status, result=None):
        """更新订单状态到 trade_records 表"""
        if not self.connect():
            return False

        try:
            with self.connection.cursor() as cursor:
                # 准备更新数据
                query = '''
                UPDATE trade_records 
                SET result = %s, uTime = %s
                WHERE ordId = %s
                '''

                # 获取当前时间戳（毫秒）
                current_timestamp = int(time.time() * 1000)
                result_text = result if result else f"status updated to {status}"

                cursor.execute(query, (
                    result_text,
                    current_timestamp,
                    order_id
                ))

                self.connection.commit()
                return cursor.rowcount > 0  # 返回是否有记录被更新
        except pymysql.Error as e:
            print(f"更新订单状态错误: {e}")
            self.connection.rollback()
            return False
        finally:
            self.disconnect()
