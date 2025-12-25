#!/usr/bin/env python
# -*- coding: utf-8 -*-

import threading
import time
import datetime
import pandas as pd
import pymysql
from dca_strategy import DcaExeStrategy


class DcaBacktester:
    """DCA Strategy Backtester"""
    
    def __init__(self, host, user, password, database, data_table="sorted_history_15m"):
        """Initialize backtester"""
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.data_table = data_table
        self.results = []
        self.lock = threading.Lock()
    
    def fetch_historical_data(self, limit=None):
        """Fetch historical data from database"""
        conn = pymysql.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database,
            cursorclass=pymysql.cursors.DictCursor
        )
        
        try:
            with conn.cursor() as cursor:
                query = f"SELECT * FROM {self.data_table} ORDER BY ts ASC"
                if limit:
                    query += f" LIMIT {limit}"
                
                cursor.execute(query)
                data = cursor.fetchall()
                
            # 转换为DataFrame
            df = pd.DataFrame(data)
            df['ts'] = pd.to_datetime(df['ts'])
            
            # 转换数值字段为float类型
            numeric_columns = ['open', 'high', 'low', 'close', 'volume', 'vol_ccy_quote']
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = df[col].astype(float)
            
            return df
        finally:
            conn.close()
    
    def backtest_strategy(self, params, data):
        """Backtest a single DCA strategy"""
        strategy_name = f"DCA_{params['price_drop_threshold']}_{params['take_profit_threshold']}_{params['initial_investment_ratio']}"
        
        # Initialize strategy
        strategy = DcaExeStrategy(
            price_drop_threshold=params['price_drop_threshold'],
            take_profit_threshold=params['take_profit_threshold'],
            max_time_since_last_trade=params['max_time_since_last_trade'],
            min_time_since_last_trade=params['min_time_since_last_trade'],
            initial_capital=params['initial_capital'],
            initial_investment_ratio=params['initial_investment_ratio'],
            initial_dca_value=params['initial_dca_value'],
            buy_fee_rate=params['buy_fee_rate'],
            sell_fee_rate=params['sell_fee_rate'],
            strategy_name=strategy_name
        )
        
        # Run backtest
        start_time = time.time()
        
        for _, row in data.iterrows():
            strategy.execute_logic(row['ts'], row['close'])
        
        end_time = time.time()
        
        # Calculate final results
        final_value = strategy.portfolio['cash'] + strategy.portfolio['position'] * data.iloc[-1]['close']
        total_return = (final_value - params['initial_capital']) / params['initial_capital']
        max_drawdown = (params['initial_capital'] - strategy.portfolio['peak_value']) / params['initial_capital'] if strategy.portfolio['peak_value'] < params['initial_capital'] else 0
        
        # Collect results
        result = {
            'strategy_name': strategy_name,
            'params': params.copy(),
            'initial_capital': params['initial_capital'],
            'final_value': final_value,
            'total_return': total_return,
            'max_drawdown': max_drawdown,
            'trade_count': len(strategy.trades),
            'run_time': end_time - start_time
        }
        
        with self.lock:
            self.results.append(result)
        
        print(f"Strategy {strategy_name} backtest completed: Return {total_return:.2%}, Max Drawdown {max_drawdown:.2%}, Trades {len(strategy.trades)}, Time {end_time - start_time:.2f}s")
    
    def run_backtest(self, param_list, data, num_threads=2):
        """Run multi-threaded backtest"""
        threads = []
        
        for params in param_list:
            thread = threading.Thread(target=self.backtest_strategy, args=(params, data))
            threads.append(thread)
            thread.start()
            
            # 控制线程数量
            if len(threads) >= num_threads:
                for t in threads:
                    t.join()
                threads = []
        
        # 等待剩余线程完成
        for t in threads:
            t.join()
    
    def get_best_strategy(self):
        """Get the best performing strategy"""
        if not self.results:
            return None
        
        # 按总收益率排序
        sorted_results = sorted(self.results, key=lambda x: x['total_return'], reverse=True)
        return sorted_results[0]
    
    def print_results(self):
        """Print all backtest results"""
        if not self.results:
            print("No backtest results")
            return
        
        print("\n=== Backtest Results ===")
        print(f"Total strategies tested: {len(self.results)}")
        
        # Sort by total return
        sorted_results = sorted(self.results, key=lambda x: x['total_return'], reverse=True)
        
        for i, result in enumerate(sorted_results[:10]):
            print(f"\nRank {i+1}: {result['strategy_name']}")
            print(f"  Return: {result['total_return']:.2%}")
            print(f"  Max Drawdown: {result['max_drawdown']:.2%}")
            print(f"  Final Value: {result['final_value']:.2f}")
            print(f"  Trade Count: {result['trade_count']}")
            print(f"  Params: {result['params']}")
        
        best = self.get_best_strategy()
        print(f"\n=== Best Strategy ===")
        print(f"Strategy Name: {best['strategy_name']}")
        print(f"Return: {best['total_return']:.2%}")
        print(f"Max Drawdown: {best['max_drawdown']:.2%}")
        print(f"Final Value: {best['final_value']:.2f}")
        print(f"Trade Count: {best['trade_count']}")
        print(f"Params: {best['params']}")


def main():
    """Main function"""
    # Database configuration
    DB_CONFIG = {
        'host': 'localhost',
        'user': 'root',
        'password': '!A33b3e561fec',
        'database': 'okx_data'
    }
    
    # Create backtester
    backtester = DcaBacktester(
        host=DB_CONFIG['host'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password'],
        database=DB_CONFIG['database']
    )
    
    # Get historical data
    print("Fetching historical data...")
    data = backtester.fetch_historical_data()
    print(f"Fetched {len(data)} historical data points, time range: {data['ts'].min()} to {data['ts'].max()}")
    
    # Define parameter combinations
    param_combinations = [
        {
            'price_drop_threshold': 0.02,
            'take_profit_threshold': 0.01,
            'max_time_since_last_trade': 48,
            'min_time_since_last_trade': 24,
            'initial_capital': 100000,
            'initial_investment_ratio': 0.5,
            'initial_dca_value': 0.1,
            'buy_fee_rate': 0.001,
            'sell_fee_rate': 0.001
        },
        {
            'price_drop_threshold': 0.03,
            'take_profit_threshold': 0.02,
            'max_time_since_last_trade': 48,
            'min_time_since_last_trade': 24,
            'initial_capital': 100000,
            'initial_investment_ratio': 0.5,
            'initial_dca_value': 0.1,
            'buy_fee_rate': 0.001,
            'sell_fee_rate': 0.001
        },
        {
            'price_drop_threshold': 0.05,
            'take_profit_threshold': 0.03,
            'max_time_since_last_trade': 48,
            'min_time_since_last_trade': 24,
            'initial_capital': 100000,
            'initial_investment_ratio': 0.5,
            'initial_dca_value': 0.1,
            'buy_fee_rate': 0.001,
            'sell_fee_rate': 0.001
        },
        {
            'price_drop_threshold': 0.02,
            'take_profit_threshold': 0.02,
            'max_time_since_last_trade': 48,
            'min_time_since_last_trade': 24,
            'initial_capital': 100000,
            'initial_investment_ratio': 0.3,
            'initial_dca_value': 0.1,
            'buy_fee_rate': 0.001,
            'sell_fee_rate': 0.001
        },
        {
            'price_drop_threshold': 0.03,
            'take_profit_threshold': 0.03,
            'max_time_since_last_trade': 48,
            'min_time_since_last_trade': 24,
            'initial_capital': 100000,
            'initial_investment_ratio': 0.7,
            'initial_dca_value': 0.1,
            'buy_fee_rate': 0.001,
            'sell_fee_rate': 0.001
        }
    ]
    
    # Run backtest
    print("\nStarting backtest...")
    start_time = time.time()
    
    backtester.run_backtest(param_combinations, data, num_threads=2)
    
    end_time = time.time()
    print(f"\nBacktest completed, total time: {end_time - start_time:.2f} seconds")
    
    # Print results
    backtester.print_results()


if __name__ == "__main__":
    main()