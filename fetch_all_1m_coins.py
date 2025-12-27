#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
批量下载1分钟级别多币种历史数据工具
注意：OKX API不支持15秒级别数据，1分钟是最接近15秒的可用时间级别
"""

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

from download_1m_data import download_1m_single_currency, load_config


def fetch_all_1m_coins(save_path='./'):
    """批量爬取所有启用的币种1分钟级别历史数据

    参数:
        save_path: 数据保存路径

    返回:
        dict: 爬取结果字典，包含每个币种的爬取状态
    """
    config = load_config()
    db_config = config['database_config'].copy()
    db_config['table_name'] = 'sorted_history_1m'  # 使用1分钟级别表
    api_config = config['api_config']
    table_name = db_config['table_name']

    enabled_pairs = [p for p in config['trading_pairs'] if p.get('enabled', True)]

    print("=" * 60)
    print("批量爬取所有币种的1分钟级别历史数据")
    print("注意：OKX API不支持15秒级别，1分钟是最接近的时间级别")
    print("=" * 60)
    print("启用的交易对数量: %d" % len(enabled_pairs))
    print("保存路径: %s" % save_path)
    print("=" * 60)

    results = {}
    success_count = 0
    fail_count = 0

    for i, currency_config in enumerate(enabled_pairs, 1):
        currency = currency_config['symbol']
        coin_name = currency_config['name']

        print("\n[%d/%d] 开始爬取 %s (%s) 的1分钟级别数据..." % (i, len(enabled_pairs), coin_name, currency))

        try:
            success = download_1m_single_currency(
                currency_config=currency_config,
                db_config=db_config,
                api_config=api_config,
                table_name=table_name,
                save_path=save_path
            )

            if success:
                results[currency] = {
                    'status': 'success',
                    'name': coin_name
                }
                success_count += 1
                print("✓ %s (%s) 1分钟级别数据爬取完成" % (coin_name, currency))
            else:
                results[currency] = {
                    'status': 'failed',
                    'name': coin_name
                }
                fail_count += 1
                print("✗ %s (%s) 1分钟级别数据爬取失败" % (coin_name, currency))

        except Exception as e:
            print("✗ %s (%s) 1分钟级别数据爬取出错: %s" % (coin_name, currency, str(e)))
            results[currency] = {
                'status': 'error',
                'name': coin_name,
                'error': str(e)
            }
            fail_count += 1

    print("\n" + "=" * 60)
    print("1分钟级别批量爬取完成！")
    print("成功: %d, 失败: %d" % (success_count, fail_count))
    print("=" * 60)

    return results


def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description='批量爬取所有币种的1分钟级别历史数据')
    parser.add_argument('--save_path', type=str, default='./', help='数据保存路径')

    args = parser.parse_args()

    fetch_all_1m_coins(save_path=args.save_path)


if __name__ == "__main__":
    main()