import numpy as np
import pandas as pd
import torch

from myWork.model.prepare_data import calculate_rsi


def backtest_strategy(
        df,
        initial_balance=10000,
        buy_ratio=0.5,
        sell_ratio=0.5,
        buy_fee_rate=0.001,
        sell_fee_rate=0.001
):
    """支持比例交易的回测策略

    参数:
        df: 包含信号和价格数据的DataFrame
        initial_balance: 初始资金
        buy_ratio: 买入资金占总资金的比例（0 < ratio ≤ 1）
        sell_ratio: 卖出资金占总资金的比例（0 < ratio ≤ 1）
        buy_fee_rate: 买入手续费率
        sell_fee_rate: 卖出手续费率

    返回:
        dict: 回测结果字典
    """
    balance = initial_balance
    holdings = 0.0
    history = []

    for idx, row in df.iterrows():
        signal, close_price = row['signal'], row['c']
        total_value = balance + holdings * close_price

        if signal == 1:
            planned_invest = total_value * buy_ratio
            if planned_invest > balance:
                planned_invest = balance
            available_invest = planned_invest * (1 - buy_fee_rate)
            amount = available_invest / close_price

            history.append({
                'time': row['ts'],
                'type': 'buy',
                'price': close_price,
                'ratio': buy_ratio,
                'planned_invest': planned_invest,
                'actual_invest': available_invest,
                'amount': amount,
                'balance_after': balance - planned_invest,
                'holdings_after': holdings + amount
            })

            holdings += amount
            balance -= planned_invest

        elif signal == -1 and holdings > 0:
            planned_sell_value = total_value * sell_ratio
            if planned_sell_value > holdings * close_price:
                planned_sell_value = holdings * close_price
            planned_sell = planned_sell_value / close_price
            total_proceeds = planned_sell * close_price
            available_proceeds = total_proceeds * (1 - sell_fee_rate)

            history.append({
                'time': row['ts'],
                'type': 'sell',
                'price': close_price,
                'ratio': sell_ratio,
                'planned_sell_value': planned_sell_value,
                'planned_sell': planned_sell,
                'actual_proceeds': available_proceeds,
                'amount': planned_sell,
                'balance_after': balance + available_proceeds,
                'holdings_after': holdings - planned_sell
            })

            balance += available_proceeds
            holdings -= planned_sell

    if holdings > 0:
        final_price = df['c'].iloc[-1]
        total_proceeds = holdings * final_price
        available_proceeds = total_proceeds * (1 - sell_fee_rate)
        balance += available_proceeds
        holdings = 0

    return {
        'initial_balance': initial_balance,
        'final_balance': balance,
        'final_holdings': holdings,
        'return': (balance - initial_balance) / initial_balance if initial_balance != 0 else 0,
        'trade_history': history,
        'buy_ratio': buy_ratio,
        'sell_ratio': sell_ratio,
        'buy_fee_rate': buy_fee_rate,
        'sell_fee_rate': sell_fee_rate
    }


def evaluate_performance(backtest_result):
    """评估含比例交易的策略绩效

    参数:
        backtest_result: 回测结果字典

    返回:
        dict: 绩效评估结果
    """
    history = backtest_result['trade_history']
    returns = []

    buy_trades = [t for t in history if t['type'] == 'buy']
    sell_trades = [t for t in history if t['type'] == 'sell']

    for buy, sell in zip(buy_trades, sell_trades):
        buy_price = buy['price']
        sell_price = sell['price']
        if buy_price != 0:
            returns.append((sell_price - buy_price) / buy_price)

    win_rate = sum(r > 0 for r in returns) / len(returns) if returns else 0
    return {
        'total_return': backtest_result['return'],
        'win_rate': win_rate,
        'max_drawdown': calculate_max_drawdown(history),
        'avg_return': np.mean(returns) if returns else 0,
        'num_trades': len(returns),
        'buy_ratio': backtest_result['buy_ratio'],
        'sell_ratio': backtest_result['sell_ratio']
    }


def calculate_max_drawdown(history):
    """适配比例交易的最大回撤计算

    参数:
        history: 交易历史列表

    返回:
        float: 最大回撤率
    """
    if not history:
        return 0.0

    equity = [history[0]['balance_after'] if history[0]['type'] == 'buy' else history[0]['balance']]

    for trade in history:
        if trade['type'] == 'buy':
            current_value = trade['balance_after'] + trade['holdings_after'] * trade['price']
        elif trade['type'] == 'sell':
            current_value = trade['balance_after'] + trade['holdings_after'] * trade['price']
        else:
            current_value = equity[-1]
        equity.append(current_value)

    peak, max_drawdown = equity[0], 0.0
    for e in equity:
        if e > peak:
            peak = e
        if peak != 0:
            drawdown = (peak - e) / peak
            max_drawdown = max(max_drawdown, drawdown)
    return max_drawdown


def calculate_ma_signals(df, short_window=5, long_window=20):
    """计算双均线信号（保留原始逻辑）

    参数:
        df: K线数据DataFrame
        short_window: 短期均线窗口
        long_window: 长期均线窗口

    返回:
        pd.DataFrame: 添加了信号列的DataFrame
    """
    df['ma_short'] = df['c'].rolling(short_window).mean()
    df['ma_long'] = df['c'].rolling(long_window).mean()
    df['signal'] = np.where(df['ma_short'] > df['ma_long'], 1, np.where(df['ma_short'] < df['ma_long'], -1, 0))
    return df.dropna(subset=['ma_short', 'ma_long']).reset_index(drop=True)


def backtest_currency(currency='BTC-USDT', table_name='sorted_history_15m',
                    short_window=5, long_window=20,
                    initial_balance=10000, buy_ratio=0.5, sell_ratio=0.5,
                    buy_fee_rate=0.001, sell_fee_rate=0.001):
    """对指定币种进行回测

    参数:
        currency: 币种标识（如 BTC-USDT）
        table_name: 数据表名
        short_window: 短期均线窗口
        long_window: 长期均线窗口
        initial_balance: 初始资金
        buy_ratio: 买入比例
        sell_ratio: 卖出比例
        buy_fee_rate: 买入手续费率
        sell_fee_rate: 卖出手续费率

    返回:
        tuple: (backtest_result, performance)
    """
    from myWork.process.read import load_kline_from_db

    print(f"\n{'='*60}")
    print(f"开始回测 {currency}")
    print(f"{'='*60}")

    kline_df = load_kline_from_db(currency=currency, table_name=table_name)

    if kline_df.empty:
        print(f"警告: 数据库中没有 {currency} 的数据，跳过回测")
        return None, None

    signal_df = calculate_ma_signals(kline_df, short_window=short_window, long_window=long_window)

    backtest_result = backtest_strategy(
        signal_df,
        initial_balance=initial_balance,
        buy_ratio=buy_ratio,
        sell_ratio=sell_ratio,
        buy_fee_rate=buy_fee_rate,
        sell_fee_rate=sell_fee_rate
    )

    performance = evaluate_performance(backtest_result)

    print(f"策略参数：买入比例{performance['buy_ratio']*100}%，卖出比例{performance['sell_ratio']*100}%")
    print(f"最终资产：{backtest_result['final_balance'] + backtest_result['final_holdings'] * kline_df['c'].iloc[-1]:.2f} USDT")
    print(f"总收益率：{performance['total_return']*100:.2f}%")
    print(f"胜率：{performance['win_rate']*100:.2f}%，最大回撤：{performance['max_drawdown']*100:.2f}%")

    return backtest_result, performance
