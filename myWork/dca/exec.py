import os
import time
from datetime import datetime

from dotenv import load_dotenv
from okx import MarketData, PublicData
from okx.Trade import TradeAPI

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
trade_api = TradeAPI(api_key, api_secret_key, passphrase, use_server_time=False, flag=ENV_FLAG)
market_api = MarketData.MarketAPI(flag=ENV_FLAG)
public_api = PublicData.PublicAPI(flag=ENV_FLAG)

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
        initial_capital=100000,  # 初始资金100,000 USDT
        initial_investment_ratio=0.5,  # 初始投资使用50%的资金
        initial_dca_value=0.1,  # 首次DCA使用剩余资金的10%
        database_manager=db_manager,  # 传入数据库管理器
        strategy_name="BTC_USDT_DCA"  # 策略名称
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

    # 执行策略逻辑
    trade_decision = strategy.execute_logic(current_time, current_price)

    # 如果有交易决策，执行交易
    if trade_decision:
        print(f"策略生成交易决策: {trade_decision['type']} {trade_decision['side']}")
        order_id = executor.execute_trade(inst_id, trade_decision)
        if order_id:
            print(f"交易执行成功，订单ID: {order_id}")
        else:
            print("交易执行失败")

    while True:
        current_time = datetime.now()
        current_price = get_realtime_price(inst_id)['bid_px']
        trade_decision = strategy.execute_logic(current_time, current_price)
        if trade_decision:
            executor.execute_trade(inst_id, trade_decision)
        time.sleep(60)  # 每分钟检查一次


if __name__ == "__main__":
    main()
