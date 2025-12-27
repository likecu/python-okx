#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
测试OKX API是否支持15秒级别数据
"""

import requests
import json
import time

def test_okx_15s_api():
    """测试OKX API对15秒级别数据的支持"""
    
    # 基础URL
    base_url = "https://www.okx.com"
    
    # 测试参数
    inst_id = "BTC-USDT"
    bar = "15s"  # 15秒级别
    limit = 10   # 只获取10条数据用于测试
    
    # 构建API请求
    url = "%s/api/v5/market/history-candles" % base_url
    params = {
        "instId": inst_id,
        "bar": bar,
        "limit": str(limit)
    }
    
    print("测试OKX API是否支持15秒级别数据...")
    print("请求参数: %s" % json.dumps(params, indent=2))
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        print("\nAPI响应:")
        print("状态码: %s" % data.get("code"))
        print("消息: %s" % data.get("msg"))
        print("数据条数: %d" % len(data.get("data", [])))
        
        if data.get("code") == "0":
            print("✓ API成功返回15秒级别数据")
            
            # 显示第一条数据作为示例
            if data.get("data"):
                first_record = data["data"][0]
                print("\n示例数据:")
                print("时间戳: %s" % first_record[0])
                print("开盘价: %s" % first_record[1])
                print("最高价: %s" % first_record[2])
                print("最低价: %s" % first_record[3])
                print("收盘价: %s" % first_record[4])
                print("成交量: %s" % first_record[5])
                
                # 转换时间戳为可读格式
                ts = int(first_record[0]) // 1000
                import datetime
                readable_time = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
                print("可读时间: %s" % readable_time)
                
            return True
        else:
            print("✗ API返回错误")
            return False
            
    except Exception as e:
        print("✗ 请求失败: %s" % str(e))
        return False

if __name__ == "__main__":
    test_okx_15s_api()