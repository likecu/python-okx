#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import json
import sys
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'myWork'))

from process.回测 import backtest_currency


def load_config():
    """加载配置文件

    返回:
        dict: 配置字典
    """
    config_path = os.path.join(os.path.dirname(__file__), 'config/trading_pairs.json')
    with open(config_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def backtest_all_coins(short_window=5, long_window=20,
                       initial_balance=10000, buy_ratio=0.5, sell_ratio=0.5,
                       buy_fee_rate=0.001, sell_fee_rate=0.001,
                       save_results=True, save_dir='./results'):
    """批量回测所有启用的币种

    参数:
        short_window: 短期均线窗口
        long_window: 长期均线窗口
        initial_balance: 初始资金
        buy_ratio: 买入比例
        sell_ratio: 卖出比例
        buy_fee_rate: 买入手续费率
        sell_fee_rate: 卖出手续费率
        save_results: 是否保存回测结果
        save_dir: 结果保存目录

    返回:
        dict: 回测结果字典，包含每个币种的回测状态
    """
    config = load_config()
    enabled_pairs = [p for p in config['trading_pairs'] if p.get('enabled', True)]
    table_name = config['database_config']['table_name']

    print("=" * 60)
    print("批量回测所有币种")
    print("=" * 60)
    print(f"启用的交易对数量: {len(enabled_pairs)}")
    print(f"策略参数: short_window={short_window}, long_window={long_window}")
    print(f"交易参数: buy_ratio={buy_ratio}, sell_ratio={sell_ratio}")
    print("=" * 60)

    results = {}
    success_count = 0
    fail_count = 0
    all_performances = []

    for i, currency_config in enumerate(enabled_pairs, 1):
        currency = currency_config['symbol']
        coin_name = currency_config['name']

        print(f"\n[{i}/{len(enabled_pairs)}] 开始回测 {coin_name} ({currency})...")

        try:
            backtest_result, performance = backtest_currency(
                currency=currency,
                table_name=table_name,
                short_window=short_window,
                long_window=long_window,
                initial_balance=initial_balance,
                buy_ratio=buy_ratio,
                sell_ratio=sell_ratio,
                buy_fee_rate=buy_fee_rate,
                sell_fee_rate=sell_fee_rate
            )

            if backtest_result is not None and performance is not None:
                results[currency] = {
                    'status': 'success',
                    'name': coin_name,
                    'backtest_result': backtest_result,
                    'performance': performance
                }
                all_performances.append({
                    'currency': currency,
                    'name': coin_name,
                    'total_return': performance['total_return'],
                    'win_rate': performance['win_rate'],
                    'max_drawdown': performance['max_drawdown'],
                    'avg_return': performance['avg_return'],
                    'num_trades': performance['num_trades']
                })
                success_count += 1
                print(f"✓ {coin_name} ({currency}) 回测完成")
            else:
                results[currency] = {
                    'status': 'skipped',
                    'name': coin_name,
                    'reason': 'No data available'
                }
                fail_count += 1
                print(f"○ {coin_name} ({currency}) 跳过（无数据）")

        except Exception as e:
            print(f"✗ {coin_name} ({currency}) 回测失败: {str(e)}")
            results[currency] = {
                'status': 'failed',
                'name': coin_name,
                'error': str(e)
            }
            fail_count += 1

    print("\n" + "=" * 60)
    print("批量回测完成！")
    print(f"成功: {success_count}, 失败: {fail_count}")
    print("=" * 60)

    if all_performances:
        print("\n" + "=" * 60)
        print("回测结果汇总")
        print("=" * 60)

        df_summary = pd.DataFrame(all_performances)
        df_summary = df_summary.sort_values('total_return', ascending=False)

        print("\n按总收益率排序：")
        print(df_summary.to_string(index=False))

        if save_results:
            os.makedirs(save_dir, exist_ok=True)
            summary_file = os.path.join(save_dir, 'backtest_summary.csv')
            df_summary.to_csv(summary_file, index=False, float_format='%.4f')
            print(f"\n回测结果已保存到: {summary_file}")

    return results


def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description='批量回测所有币种')
    parser.add_argument('--short_window', type=int, default=5, help='短期均线窗口')
    parser.add_argument('--long_window', type=int, default=20, help='长期均线窗口')
    parser.add_argument('--initial_balance', type=float, default=10000, help='初始资金')
    parser.add_argument('--buy_ratio', type=float, default=0.5, help='买入比例')
    parser.add_argument('--sell_ratio', type=float, default=0.5, help='卖出比例')
    parser.add_argument('--buy_fee_rate', type=float, default=0.001, help='买入手续费率')
    parser.add_argument('--sell_fee_rate', type=float, default=0.001, help='卖出手续费率')
    parser.add_argument('--no_save', action='store_true', help='不保存回测结果')
    parser.add_argument('--save_dir', type=str, default='./results', help='结果保存目录')

    args = parser.parse_args()

    backtest_all_coins(
        short_window=args.short_window,
        long_window=args.long_window,
        initial_balance=args.initial_balance,
        buy_ratio=args.buy_ratio,
        sell_ratio=args.sell_ratio,
        buy_fee_rate=args.buy_fee_rate,
        sell_fee_rate=args.sell_fee_rate,
        save_results=not args.no_save,
        save_dir=args.save_dir
    )


if __name__ == "__main__":
    main()
