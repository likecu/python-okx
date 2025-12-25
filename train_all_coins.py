#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import json
import torch

from myWork.model.LSTM import train_model_for_currency, plot_training_history


def load_config():
    """加载配置文件

    返回:
        dict: 配置字典
    """
    config_path = os.path.join(os.path.dirname(__file__), 'config/trading_pairs.json')
    with open(config_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def train_all_coins(lookback=60, forecast=1, split_ratio=0.8,
                    hidden_size=64, num_layers=2, batch_size=64, epochs=50, lr=0.001,
                    save_dir='./models', plot_dir='./plots', device='cuda'):
    """批量训练所有启用的币种

    参数:
        lookback: 用于预测的历史数据长度
        forecast: 预测未来数据点的数量
        split_ratio: 训练集与测试集的划分比例
        hidden_size: LSTM隐藏层单元数
        num_layers: LSTM层数
        batch_size: 批次大小
        epochs: 训练轮数
        lr: 学习率
        save_dir: 模型保存目录
        plot_dir: 图表保存目录
        device: 训练设备

    返回:
        dict: 训练结果字典，包含每个币种的训练状态
    """
    config = load_config()
    enabled_pairs = [p for p in config['trading_pairs'] if p.get('enabled', True)]

    print("=" * 60)
    print("批量训练所有币种的LSTM模型")
    print("=" * 60)
    print(f"启用的交易对数量: {len(enabled_pairs)}")
    print(f"训练参数: lookback={lookback}, forecast={forecast}, epochs={epochs}")
    print(f"模型参数: hidden_size={hidden_size}, num_layers={num_layers}")
    print(f"设备: {device}")
    print("=" * 60)

    results = {}
    success_count = 0
    fail_count = 0

    for i, currency_config in enumerate(enabled_pairs, 1):
        currency = currency_config['symbol']
        coin_name = currency_config['name']

        print(f"\n[{i}/{len(enabled_pairs)}] 开始训练 {coin_name} ({currency})...")

        try:
            model, scaler, history = train_model_for_currency(
                currency=currency,
                table_name=config['database_config']['table_name'],
                lookback=lookback,
                forecast=forecast,
                split_ratio=split_ratio,
                hidden_size=hidden_size,
                num_layers=num_layers,
                batch_size=batch_size,
                epochs=epochs,
                lr=lr,
                save_dir=save_dir,
                device=device
            )

            plot_training_history(history, currency=currency, save_dir=plot_dir)

            results[currency] = {
                'status': 'success',
                'name': coin_name,
                'history': history
            }
            success_count += 1

            print(f"✓ {coin_name} ({currency}) 训练完成")

        except Exception as e:
            print(f"✗ {coin_name} ({currency}) 训练失败: {str(e)}")
            results[currency] = {
                'status': 'failed',
                'name': coin_name,
                'error': str(e)
            }
            fail_count += 1

    print("\n" + "=" * 60)
    print("批量训练完成！")
    print(f"成功: {success_count}, 失败: {fail_count}")
    print("=" * 60)

    return results


def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description='批量训练所有币种的LSTM模型')
    parser.add_argument('--lookback', type=int, default=60, help='用于预测的历史数据长度')
    parser.add_argument('--forecast', type=int, default=1, help='预测未来数据点的数量')
    parser.add_argument('--split_ratio', type=float, default=0.8, help='训练集与测试集的划分比例')
    parser.add_argument('--hidden_size', type=int, default=64, help='LSTM隐藏层单元数')
    parser.add_argument('--num_layers', type=int, default=2, help='LSTM层数')
    parser.add_argument('--batch_size', type=int, default=64, help='批次大小')
    parser.add_argument('--epochs', type=int, default=50, help='训练轮数')
    parser.add_argument('--lr', type=float, default=0.001, help='学习率')
    parser.add_argument('--save_dir', type=str, default='./models', help='模型保存目录')
    parser.add_argument('--plot_dir', type=str, default='./plots', help='图表保存目录')
    parser.add_argument('--device', type=str, default='cuda', help='训练设备（cuda 或 cpu）')

    args = parser.parse_args()

    device = torch.device('cuda' if torch.cuda.is_available() and args.device == 'cuda' else 'cpu')

    train_all_coins(
        lookback=args.lookback,
        forecast=args.forecast,
        split_ratio=args.split_ratio,
        hidden_size=args.hidden_size,
        num_layers=args.num_layers,
        batch_size=args.batch_size,
        epochs=args.epochs,
        lr=args.lr,
        save_dir=args.save_dir,
        plot_dir=args.plot_dir,
        device=device
    )


if __name__ == "__main__":
    main()
