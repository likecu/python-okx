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

# 使用正确的OKX SDK导入方式
from okx.api.trade import Trade
from okx.api.account import Account

# 初始化API客户端
trade_api = Trade(api_key, api_secret_key, passphrase, flag=ENV_FLAG)
account_api = Account(api_key, api_secret_key, passphrase, flag=ENV_FLAG)

# 设置交易参数
instId = "BTC-USDT"
side = "buy"  # 买入
tdMode = "cross"  # 全仓杠杆
ordType = "market"  # 市价单
amount = 100  # 买入金额（USDT）
leverage = 2  # 杠杆倍数

# 1. 设置杠杆倍数
print(f"\n1. 设置 {instId} 的杠杆倍数为 {leverage}x")
try:
    # 设置杠杆倍数
    leverage_result = account_api.set_leverage(
        instId=instId,
        lever=leverage,
        mgnMode="cross",  # 全仓
        posSide="net"  # 净持仓
    )
    
    if leverage_result["code"] == "0":
        print(f"   杠杆设置成功: {leverage_result['data']}")
    else:
        print(f"   杠杆设置失败: {leverage_result.get('msg', '未知错误')}")
except Exception as e:
    print(f"   设置杠杆异常: {str(e)}")

# 2. 执行杠杆买入
print(f"\n2. 执行 {amount} USDT 的 {instId} 市价买入，杠杆 {leverage}x")
try:
    # 执行市价买入订单
    order_result = trade_api.set_order(
        instId=instId,
        tdMode=tdMode,
        side=side,
        ordType=ordType,
        sz=str(amount),  # 买入金额
        ccy="USDT"
    )
    
    if order_result["code"] == "0" and len(order_result.get("data", [])) > 0:
        order_data = order_result["data"][0]
        print(f"   订单提交成功!")
        print(f"   订单ID: {order_data['ordId']}")
        print(f"   交易对: {order_data['instId']}")
        print(f"   方向: {order_data['side']}")
        print(f"   类型: {order_data['ordType']}")
        print(f"   状态: {order_data['state']}")
        print(f"   杠杆倍数: {order_data.get('lever', leverage)}")
    else:
        error_msg = order_result.get("msg", "未知错误")
        print(f"   订单提交失败: {error_msg}")
        if "data" in order_result and order_result["data"]:
            print(f"   详细错误: {order_result['data']}")
except Exception as e:
    print(f"   执行买入异常: {str(e)}")

# 3. 查询当前杠杆持仓
print(f"\n3. 查询 {instId} 的当前杠杆持仓")
try:
    positions_result = account_api.get_positions(
        instType="MARGIN",  # 杠杆交易
        instId=instId
    )
    
    if positions_result["code"] == "0":
        if len(positions_result["data"]) > 0:
            print(f"   当前杠杆持仓:")
            for pos in positions_result["data"]:
                print(f"   - 交易对: {pos['instId']}")
                print(f"   - 持仓方向: {pos['posSide']}")
                print(f"   - 持仓数量: {pos['pos']} {instId.split('-')[0]}")
                print(f"   - 平均开仓价格: {pos['avgPx']} USDT")
                print(f"   - 未实现盈亏: {pos['upl']} USDT")
                print(f"   - 杠杆倍数: {pos.get('lever', leverage)}x")
        else:
            print(f"   当前无 {instId} 杠杆持仓")
    else:
        print(f"   查询持仓失败: {positions_result.get('msg', '未知错误')}")
except Exception as e:
    print(f"   查询持仓异常: {str(e)}")

# 4. 查询账户余额
print(f"\n4. 查询当前账户余额")
try:
    balance_result = account_api.get_balance()
    if balance_result["code"] == "0" and len(balance_result["data"]) > 0:
        print(f"   账户余额:")
        for data in balance_result["data"]:
            ccy_data = data.get("details", [])
            for ccy in ccy_data:
                if float(ccy["availBal"]) > 0 or float(ccy["frozenBal"]) > 0:
                    print(f"   - {ccy['ccy']}: 可用 {ccy['availBal']}, 冻结 {ccy['frozenBal']}, 总余额 {float(ccy['availBal']) + float(ccy['frozenBal'])}")
except Exception as e:
    print(f"   查询余额异常: {str(e)}")

print(f"\n操作完成!")