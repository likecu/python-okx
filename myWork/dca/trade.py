import time
from typing import Dict, Optional

from myWork.another.all import trade_api, get_instrument_info, get_realtime_price


def _get_precision(value: float) -> int:
    """获取数值的小数位数精度"""
    value_str = str(value)
    # 处理科学计数法表示的数字
    if 'e' in value_str.lower():
        # 转换为标准格式
        value_str = format(value, 'f')
    if '.' in value_str:
        decimal_part = value_str.split('.')[1]
        # 移除末尾的零
        decimal_part = decimal_part.rstrip('0')
        return len(decimal_part)
    return 0


def format_number(value: float, precision: int) -> str:
    """根据精度格式化数字"""
    # 使用格式化字符串确保小数位数正确
    return f"{value:.{precision}f}"


class TradingExecutor:
    """交易执行器，负责执行交易决策并与API交互"""

    def __init__(self, db_manager):
        self.db_manager = db_manager

    def execute_trade(self, inst_id: str, trade_info: Dict) -> Optional[str]:
        """执行交易并返回订单ID"""
        # 获取产品信息
        instrument_info = get_instrument_info(inst_id)
        if not instrument_info:
            print(f"无法获取{inst_id}的产品信息，交易取消")
            return None

        # 获取最新价格
        price_data = get_realtime_price(inst_id)
        if not price_data:
            print(f"无法获取{inst_id}的最新价格，交易取消")
            return None

        # 根据交易类型确定使用买价还是卖价
        if trade_info['side'] == 'buy':
            price = price_data['ask_px']
        else:  # sell
            price = price_data['bid_px']

        # 确保价格符合精度要求
        tick_sz = float(instrument_info.get('tickSz', '0.01'))
        adjusted_px = round(price, _get_precision(tick_sz))

        # 确定交易数量
        if trade_info['side'] == 'buy':
            # 买入时，根据金额计算数量
            min_sz = float(instrument_info.get('minSz', '0.001'))
            sz = trade_info['amount'] / adjusted_px

            # 确保数量符合最小下单量要求
            if sz < min_sz:
                print(f"计算的下单量{sz}小于最小下单量{min_sz}，交易取消")
                return None

            # 调整数量精度
            sz_precision = _get_precision(min_sz)
            # 先四舍五入到指定精度
            rounded_sz = round(sz, sz_precision)
            # 然后格式化为字符串
            final_sz = format_number(rounded_sz, sz_precision)
        else:  # sell
            # 卖出时，使用当前持仓量
            final_sz = trade_info['sz']

        # 构造交易参数
        trade_params = {
            "instId": inst_id,
            "tdMode": "cash",
            "side": trade_info['side'],
            "ccy": "USDT",
            "ordType": "limit",
            "sz": final_sz,  # 数量精度
            "px": adjusted_px  # 价格精度
        }

        # 记录初始订单
        # self.db_manager.record_trade(inst_id, trade_info, status='pending')

        # 执行下单
        max_retries = 3
        order_id = None

        for attempt in range(max_retries):
            try:
                print(f"[{attempt + 1}/{max_retries}] 提交订单: {trade_params}")
                result = trade_api.set_order(**trade_params)

                if result["code"] == "0" and len(result.get("data", [])) > 0:
                    order_id = result["data"][0]["ordId"]
                    print(f"订单提交成功，订单ID: {order_id}")

                    # 更新订单状态
                    self.db_manager.update_order_status(order_id, 'filled')
                    self.db_manager.record_trade(inst_id, trade_info, order_id, 'filled')
                    break
                else:
                    error = result.get("data", [{}])[0]
                    error_msg = error.get("sMsg", "未知错误")
                    error_code = error.get("sCode", "未知代码")
                    print(f"订单失败 (代码: {error_code}): {error_msg}")

                    # 处理特定错误
                    if error_code == "51137" and "buy orders" in error_msg:
                        new_px = float(error_msg.split("is ")[1].split(". ")[0])
                        print(f"触发价格限制，使用强制限价: {new_px}")
                        trade_params["px"] = f"{new_px:.8f}"
                    else:
                        self.db_manager.update_order_status(order_id, 'rejected', error_msg)
                        break
            except Exception as e:
                print(f"下单异常: {str(e)}")
                if attempt == max_retries - 1:
                    self.db_manager.update_order_status(order_id, 'rejected', str(e))

            time.sleep(1)  # API调用间隔

        return order_id
