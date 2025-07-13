import csv
import time
from typing import List, Dict, Optional
import os
from dotenv import load_dotenv
from okx.api.trade import Trade
from okx.api.market import Market
from okx.api.public import Public

# 初始化API客户端
load_dotenv()
api_key = os.getenv("OKX_API_KEY")
api_secret_key = os.getenv("OKX_API_SECRET")
passphrase = os.getenv("OKX_API_PASSPHRASE")
ENV_FLAG = os.getenv("OKX_ENV_FLAG")

# API实例
trade_api = Trade(api_key, api_secret_key, passphrase, flag=ENV_FLAG)
market_api = Market(flag=ENV_FLAG)
public_api = Public(flag=ENV_FLAG)

# 缓存产品信息，避免重复查询
instrument_cache = {}


def get_instrument_info(inst_id: str) -> Optional[Dict]:
    """获取产品基础信息，包括最小下单量等参数"""
    global instrument_cache

    # 优先使用缓存
    if inst_id in instrument_cache:
        return instrument_cache[inst_id]

    try:
        # 根据instId推断instType
        if "-SWAP" in inst_id:
            inst_type = "SWAP"
        elif "-FUTURES" in inst_id:
            inst_type = "FUTURES"
        elif "-OPTION" in inst_id:
            inst_type = "OPTION"
        elif "-MARGIN" in inst_id:
            inst_type = "MARGIN"
        else:
            inst_type = "SPOT"

        # 查询产品信息
        result = public_api.get_instruments(instType=inst_type, instId=inst_id)
        if result["code"] == "0" and len(result["data"]) > 0:
            info = result["data"][0]
            instrument_cache[inst_id] = info
            return info
        else:
            print(f"获取{inst_id}产品信息失败: {result.get('msg', '无错误信息')}")
            return None
    except Exception as e:
        print(f"查询产品信息异常: {str(e)}")
        return None


from functools import lru_cache
from datetime import datetime, timedelta


def get_realtime_price(inst_id: str) -> Dict[str, float]:
    """获取实时行情数据(带2分钟缓存)"""
    result = market_api.get_ticker(instId=inst_id)
    if result["code"] == "0" and len(result["data"]) > 0:
        data = result["data"][0]
        return {
            "ask_px": float(data["askPx"]),
            "bid_px": float(data["bidPx"])
        }

