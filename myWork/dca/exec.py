import os
import time
from datetime import datetime

import pymysql
from dotenv import load_dotenv
# from okx.Trade import TradeAPI

import sys
from pathlib import Path

# Add project root to Python path
sys.path.append(str(Path(__file__).parent.parent.parent))
from myWork.another.all import get_realtime_price
from myWork.dca.database_manager import DatabaseManager
from myWork.dca.dca_strategy import DcaExeStrategy
from myWork.dca.trade import TradingExecutor

# 初始化API客户端
load_dotenv()
api_key = os.getenv("OKX_API_KEY")
api_secret_key = os.getenv("OKX_API_SECRET")
passphrase = os.getenv("OKX_API_PASSPHRASE")
ENV_FLAG = os.getenv("OKX_ENV_FLAG")

# API实例
# trade_api = TradeAPI(api_key, api_secret_key, passphrase, use_server_time=False, flag=ENV_FLAG)
# market_api = MarketData.MarketAPI(flag=ENV_FLAG)
# public_api = PublicData.PublicAPI(flag=ENV_FLAG)

load_dotenv()
MYSQL_CONN = os.getenv("MYSQL_CONN")
MYSQL_PASS = os.getenv("MYSQL_PASS")


def main():
    """主函数，程序入口点"""
    # 配置数据库连接
    db_manager = DatabaseManager(
        host=MYSQL_CONN,
        user="root",
        password=MYSQL_PASS,
        database="trading_db"
    )

    db_manager.create_tables()

    # 初始化交易执行器
    executor = TradingExecutor(db_manager)

    # 初始化策略，传入数据库管理器和策略名称
    strategy = DcaExeStrategy(
        price_drop_threshold=0.03,  # 价格下跌3%触发DCA
        take_profit_threshold=0.02,  # 利润达到2%触发止盈
        max_time_since_last_trade=48,
        min_time_since_last_trade=24,
        initial_capital=100,  # 初始资金100,000 USDT
        initial_investment_ratio=0.05,  # 初始投资使用50%的资金
        initial_dca_value=0.065,  # 首次DCA使用剩余资金的10%
        database_manager=db_manager,  # 传入数据库管理器
        buy_fee_rate=0.001,
        sell_fee_rate=0.001,
        strategy_name="BTC_USDT_DCA-89"  # 策略名称
    )

    # 尝试从数据库加载策略状态
    # 如果是首次运行，将使用默认参数
    # 如果已有保存的状态，将恢复到上次的状态
    strategy.load_state()

    # 交易对
    inst_id = "BTC-USDT"

    # 示例：手动执行一次交易决策
    current_time = datetime.now()
    price_data = get_realtime_price(inst_id)
    current_price = price_data['bid_px']  # 使用买一价
    print("进行初始化")
    # 执行策略逻辑
    trade_decision = strategy.execute_logic(current_time, current_price)

    # 如果有交易决策，执行交易
    if trade_decision:
        print(f"策略生成交易决策: {trade_decision['type']} {trade_decision['side']}")
        order_id = executor.execute_trade(inst_id, trade_decision)
        if order_id:
            print(f"交易执行成功，订单ID: {order_id}")
            # 保存交易日志
            if db_manager.connect():
                try:
                    with db_manager.connection.cursor() as cursor:
                        query = '''
                        INSERT INTO trade_logs (inst_id, trade_time, trade_type, price, position, fee, order_id)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        '''
                        cursor.execute(query, (
                            inst_id,
                            current_time,
                            trade_decision['type'],
                            current_price,
                            trade_decision['position'],
                            trade_decision['fee'],
                            order_id
                        ))
                    db_manager.connection.commit()
                except pymysql.Error as e:
                    print(f"保存交易日志错误: {e}")
                    db_manager.connection.rollback()
                finally:
                    db_manager.disconnect()
        else:
            print("交易执行失败")

    print("开始循环")

    while True:
        try:
            current_time = datetime.now()
            current_price = get_realtime_price(inst_id)['bid_px']
            strategy.load_state()  # 每次循环都加载最新状态
            trade_decision = strategy.execute_logic(current_time, current_price)
            if trade_decision:
                order_id = executor.execute_trade(inst_id, trade_decision)
                if order_id:
                    # 保存交易日志
                    if db_manager.connect():
                        try:
                            with db_manager.connection.cursor() as cursor:
                                query = '''
                                INSERT INTO trade_logs (inst_id, trade_time, trade_type, price, position, fee, order_id)
                                VALUES (%s, %s, %s, %s, %s, %s, %s)
                                '''
                                cursor.execute(query, (
                                    inst_id,
                                    current_time,
                                    trade_decision['type'],
                                    current_price,
                                    trade_decision['position'],
                                    trade_decision['fee'],
                                    order_id
                                ))
                            db_manager.connection.commit()
                        except pymysql.Error as e:
                            print(f"保存交易日志错误: {e}")
                            db_manager.connection.rollback()
                        finally:
                            db_manager.disconnect()
            time.sleep(5)  # 每5秒检查一次
        except Exception as e:
            print(f"循环中出现错误: {e}")
            continue


if __name__ == "__main__":
    main()
