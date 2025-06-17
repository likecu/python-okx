import pymysql

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

                # 创建交易日志表
                cursor.execute('''
                CREATE TABLE IF NOT EXISTS trade_logs (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    inst_id VARCHAR(50) NOT NULL,
                    trade_time DATETIME NOT NULL,
                    trade_type ENUM('INITIAL_BUY', 'DCA', 'TAKE_PROFIT') NOT NULL,
                    price DECIMAL(15,8) NOT NULL,
                    position DECIMAL(15,8) NOT NULL,
                    fee DECIMAL(15,4) NOT NULL,
                    order_id VARCHAR(100),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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

    # 其他方法保持不变