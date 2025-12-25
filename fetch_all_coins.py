#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import json
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'myWork'))

from download_data import download_single_currency, load_config


def fetch_all_coins(save_path='./'):
    """批量爬取所有启用的币种历史数据

    参数:
        save_path: 数据保存路径

    返回:
        dict: 爬取结果字典，包含每个币种的爬取状态
    """
    config = load_config()
    db_config = config['database_config']
    api_config = config['api_config']
    table_name = db_config['table_name']

    enabled_pairs = [p for p in config['trading_pairs'] if p.get('enabled', True)]

    print("=" * 60)
    print("批量爬取所有币种的历史数据")
    print("=" * 60)
    print(f"启用的交易对数量: {len(enabled_pairs)}")
    print(f"保存路径: {save_path}")
    print("=" * 60)

    results = {}
    success_count = 0
    fail_count = 0

    for i, currency_config in enumerate(enabled_pairs, 1):
        currency = currency_config['symbol']
        coin_name = currency_config['name']

        print(f"\n[{i}/{len(enabled_pairs)}] 开始爬取 {coin_name} ({currency})...")

        try:
            success = download_single_currency(
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
                print(f"✓ {coin_name} ({currency}) 爬取完成")
            else:
                results[currency] = {
                    'status': 'failed',
                    'name': coin_name
                }
                fail_count += 1
                print(f"✗ {coin_name} ({currency}) 爬取失败")

        except Exception as e:
            print(f"✗ {coin_name} ({currency}) 爬取出错: {str(e)}")
            results[currency] = {
                'status': 'error',
                'name': coin_name,
                'error': str(e)
            }
            fail_count += 1

    print("\n" + "=" * 60)
    print("批量爬取完成！")
    print(f"成功: {success_count}, 失败: {fail_count}")
    print("=" * 60)

    return results


def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description='批量爬取所有币种的历史数据')
    parser.add_argument('--save_path', type=str, default='./', help='数据保存路径')

    args = parser.parse_args()

    fetch_all_coins(save_path=args.save_path)


if __name__ == "__main__":
    main()
