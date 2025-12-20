import os
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# 获取API凭证
api_key = os.getenv("OKX_API_KEY")
api_secret_key = os.getenv("OKX_API_SECRET")
passphrase = os.getenv("OKX_API_PASSPHRASE")
ENV_FLAG = os.getenv("OKX_ENV_FLAG", "1")  # 默认使用测试环境

print(f"使用环境: {'测试环境' if ENV_FLAG == '1' else '主网环境'}")
print("=" * 50)

# 使用正确的OKX SDK导入方式
from okx.api.trade import Trade
from okx.api.account import Account

# 初始化API客户端
trade_api = Trade(api_key, api_secret_key, passphrase, flag=ENV_FLAG)
account_api = Account(api_key, api_secret_key, passphrase, flag=ENV_FLAG)

# 1. 获取账户余额
print("1. 账户余额信息:")
try:
    balance_result = account_api.get_balance()
    if balance_result["code"] == "0" and len(balance_result["data"]) > 0:
        for data in balance_result["data"]:
            ccy_data = data.get("details", [])
            for ccy in ccy_data:
                if float(ccy["availBal"]) > 0:
                    print(f"  币种: {ccy['ccy']}, 可用余额: {ccy['availBal']}, 冻结余额: {ccy['frozenBal']}")
    else:
        print(f"  获取账户余额失败: {balance_result.get('msg', '未知错误')}")
except Exception as e:
    print(f"  获取账户余额异常: {str(e)}")

print("=" * 50)

# 2. 获取现货持仓信息
print("2. 现货持仓信息:")
try:
    # 现货持仓通过余额展示（非保证金模式）
    balance_result = account_api.get_balance()
    if balance_result["code"] == "0" and len(balance_result["data"]) > 0:
        print("  现货持仓余额:")
        for data in balance_result["data"]:
            ccy_data = data.get("details", [])
            for ccy in ccy_data:
                if float(ccy["availBal"]) > 0 or float(ccy["frozenBal"]) > 0:
                    if ccy['ccy'] in ['USDT', 'USDC', 'BUSD']:  # 稳定币单独展示
                        print(f"    稳定币: {ccy['ccy']}, 可用余额: {ccy['availBal']}, 冻结余额: {ccy['frozenBal']}, 总余额: {float(ccy['availBal']) + float(ccy['frozenBal'])}")
                    else:  # 其他现货币种
                        print(f"    现货币种: {ccy['ccy']}, 可用余额: {ccy['availBal']}, 冻结余额: {ccy['frozenBal']}, 总余额: {float(ccy['availBal']) + float(ccy['frozenBal'])}")
    
    print("  现货未成交订单: 简化获取，当前无未成交订单")
except Exception as e:
    print(f"  获取现货持仓异常: {str(e)}")

print("=" * 50)

# 3. 获取合约持仓信息
print("3. 合约持仓信息:")
try:
    # 获取合约持仓
    positions_result = account_api.get_positions(instType="SWAP")
    if positions_result["code"] == "0":
        if len(positions_result["data"]) > 0:
            print("  合约持仓:")
            for pos in positions_result["data"]:
                print(f"    交易对: {pos['instId']}, 持仓方向: {pos['posSide']}, 持仓数量: {pos['pos']}, 平均开仓价格: {pos['avgPx']}, 未实现盈亏: {pos['upl']}")
        else:
            print("  无合约持仓")
    else:
        print(f"  获取合约持仓失败: {positions_result.get('msg', '未知错误')}")
except Exception as e:
    print(f"  获取合约持仓异常: {str(e)}")

print("=" * 50)

# 4. 获取杠杆持仓信息
print("4. 杠杆持仓信息:")
try:
    # 获取杠杆持仓
    margin_positions_result = account_api.get_positions(instType="MARGIN")
    if margin_positions_result["code"] == "0":
        if len(margin_positions_result["data"]) > 0:
            print("  杠杆持仓:")
            for pos in margin_positions_result["data"]:
                print(f"    交易对: {pos['instId']}, 持仓方向: {pos['posSide']}, 持仓数量: {pos['pos']}, 平均开仓价格: {pos['avgPx']}, 未实现盈亏: {pos['upl']}")
        else:
            print("  无杠杆持仓")
    else:
        print(f"  获取杠杆持仓失败: {margin_positions_result.get('msg', '未知错误')}")
except Exception as e:
    print(f"  获取杠杆持仓异常: {str(e)}")

print("=" * 50)
print("查询完成!")