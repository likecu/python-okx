#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import time
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing
from datetime import datetime
import sys
from myWork.dca.dca_strategy import DcaExeStrategy


def process_single_param_combination_with_logging(btc_data, params):
    """处理单个DCA参数组合的回测，带详细日志"""
    
    # 提取参数
    price_drop_threshold, max_time_since_last_trade, min_time_since_last_trade, take_profit_threshold = params
    
    # 创建日志文件
    log_file = f"dca_retest_{params['price_drop_threshold']}_{params['max_time_since_last_trade']}_{params['min_time_since_last_trade']}_{params['take_profit_threshold']}.log"
    
    with open(log_file, 'w') as log:
        log.write(f"=== Starting backtest for params: {params} ===\n")
        log.write(f"Timestamp: {datetime.now()}\n")
        log.write(f"Data points: {len(btc_data)}\n")
        log.write(f"Price range: {btc_data['ts'].min()} to {btc_data['ts'].max()}\n")
        log.write(f"Initial capital: 10000\n")
        log.write(f"Parameters:\n")
        log.write(f"  - price_drop_threshold: {price_drop_threshold}\n")
        log.write(f"  - max_time_since_last_trade: {max_time_since_last_trade} days\n")
        log.write(f"  - min_time_since_last_trade: {min_time_since_last_trade} days\n")
        log.write(f"  - take_profit_threshold: {take_profit_threshold}\n")
        log.write("\n")
    
    try:
        # 初始化DCA策略
        dca_strategy = DcaExeStrategy(
            price_drop_threshold=price_drop_threshold,
            max_time_since_last_trade=max_time_since_last_trade,
            min_time_since_last_trade=min_time_since_last_trade,
            take_profit_threshold=take_profit_threshold,
            initial_capital=10000,
            initial_investment_ratio=0.5,
            initial_dca_value=0.1,
            buy_fee_rate=0.001,
            sell_fee_rate=0.001,
            currency="BTC-USDT"
        )
        
        with open(log_file, 'a') as log:
            log.write(f"Strategy initialized successfully\n")
            log.write(f"Initial portfolio:\n")
            log.write(f"  - Cash: {dca_strategy.portfolio['cash']}\n")
            log.write(f"  - Position: {dca_strategy.portfolio['position']}\n")
            log.write(f"  - Avg Price: {dca_strategy.portfolio['avg_price']}\n")
            log.write("\n")
        
        # 执行回测
        trade_count = 0
        for index, row in btc_data.iterrows():
            current_time = row['ts']
            current_price = row['close']
            
            # 每1000条记录记录一次进度
            if index % 1000 == 0:
                with open(log_file, 'a') as log:
                    log.write(f"Progress: {index}/{len(btc_data)} - Processing: {current_time}, Price: {current_price}\n")
            
            # 执行策略逻辑
            trade_decision = dca_strategy.execute_logic(current_time, current_price, "BTC-USDT")
            
            if trade_decision:
                trade_count += 1
                with open(log_file, 'a') as log:
                    log.write(f"Trade {trade_count}: {trade_decision['type']} at {current_time}, Price: {current_price}\n")
                    log.write(f"  Portfolio after trade:\n")
                    log.write(f"    - Cash: {dca_strategy.portfolio['cash']}\n")
                    log.write(f"    - Position: {dca_strategy.portfolio['position']}\n")
                    log.write(f"    - Avg Price: {dca_strategy.portfolio['avg_price']}\n")
                    log.write("\n")
        
        # 计算最终结果
        last_price = btc_data['close'].iloc[-1]
        final_portfolio_value = dca_strategy.portfolio['cash'] + dca_strategy.portfolio['position'] * last_price
        total_return = (final_portfolio_value - 10000) / 10000 * 100
        
        with open(log_file, 'a') as log:
            log.write(f"\n=== Backtest completed ===\n")
            log.write(f"Final Price: {last_price}\n")
            log.write(f"Final Portfolio Value: {final_portfolio_value}\n")
            log.write(f"Total Return: {total_return:.2f}%\n")
            log.write(f"Trade Count: {trade_count}\n")
            log.write(f"Final Portfolio:\n")
            log.write(f"  - Cash: {dca_strategy.portfolio['cash']}\n")
            log.write(f"  - Position: {dca_strategy.portfolio['position']}\n")
            log.write(f"  - Avg Price: {dca_strategy.portfolio['avg_price']}\n")
            log.write(f"\nTimestamp: {datetime.now()}\n")
        
        # 返回结果
        return {
            'price_drop_threshold': price_drop_threshold,
            'max_time_since_last_trade': max_time_since_last_trade,
            'min_time_since_last_trade': min_time_since_last_trade,
            'take_profit_threshold': take_profit_threshold,
            'total_return': total_return,
            'final_portfolio_value': final_portfolio_value,
            'trade_count': trade_count,
            'success': True,
            'error': None
        }
        
    except Exception as e:
        with open(log_file, 'a') as log:
            log.write(f"\n=== ERROR occurred ===\n")
            log.write(f"Timestamp: {datetime.now()}\n")
            log.write(f"Error Type: {type(e).__name__}\n")
            log.write(f"Error Message: {str(e)}\n")
            log.write(f"Traceback:\n{traceback.format_exc()}\n")
            log.write(f"\nTimestamp: {datetime.now()}\n")
        
        return {
            'price_drop_threshold': price_drop_threshold,
            'max_time_since_last_trade': max_time_since_last_trade,
            'min_time_since_last_trade': min_time_since_last_trade,
            'take_profit_threshold': take_profit_threshold,
            'total_return': None,
            'final_portfolio_value': None,
            'trade_count': None,
            'success': False,
            'error': str(e)
        }


def get_btc_data_from_mysql(data_type="15m"):
    """从MySQL数据库获取BTC历史数据"""
    from sqlalchemy import create_engine
    import os
    from dotenv import load_dotenv
    
    # 加载环境变量
    load_dotenv()
    
    # 获取数据库配置
    mysql_conn = os.getenv("MYSQL_CONN")
    mysql_pass = os.getenv("MYSQL_PASS")
    mysql_user = os.getenv("MYSQL_USER")
    mysql_db = os.getenv("MYSQL_DB")
    
    try:
        # 使用SQLAlchemy创建数据库引擎
        engine = create_engine(f"mysql+pymysql://{mysql_user}:{mysql_pass}@{mysql_conn}:3306/{mysql_db}")
        
        # 根据数据类型选择表名
        table_name = "sorted_history_15m" if data_type == "15m" else "sorted_history"
        
        # 查询BTC数据，按时间排序
        sql = """
        SELECT ts, close, high, low, open, volume 
        FROM %s 
        WHERE currency = 'BTC-USDT' 
        ORDER BY ts ASC
        """ % table_name
        
        df = pd.read_sql(sql, engine)
        
        # 转换时间格式
        df['ts'] = pd.to_datetime(df['ts'])
        
        # 转换数值字段为float类型
        numeric_columns = ['open', 'high', 'low', 'close', 'volume']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = df[col].astype(float)
        
        print(f"Successfully fetched {len(df)} BTC historical data points")
        print(f"Time range: {df['ts'].min()} to {df['ts'].max()}")
        
        return df
        
    except Exception as e:
        print(f"Failed to get data from database: {e}")
        return None


def main():
    """主函数"""
    # 1. 获取BTC历史数据
    print("Fetching BTC historical data...")
    btc_data = get_btc_data_from_mysql(data_type="15m")
    
    if btc_data is None or len(btc_data) == 0:
        print("Failed to get valid BTC historical data, exiting program")
        sys.exit(1)
    
    print(f"Successfully fetched {len(btc_data)} BTC historical data records")
    
    # 2. 定义失败的参数组合（从之前的结果文件中提取）
    failed_params = [
        (0.03, 15, 1, 0.05),
        (0.03, 15, 1, 0.1),
        (0.03, 15, 1, 0.15),
        (0.03, 15, 3, 0.05),
        (0.03, 15, 3, 0.1),
        (0.03, 15, 3, 0.15),
        (0.03, 15, 5, 0.05),
        (0.03, 15, 5, 0.1),
        (0.03, 15, 5, 0.15),
        (0.03, 20, 1, 0.05),
        (0.03, 20, 1, 0.1),
        (0.03, 20, 1, 0.15),
        (0.03, 20, 3, 0.05),
        (0.03, 20, 3, 0.1),
        (0.03, 20, 3, 0.15),
        (0.03, 25, 1, 0.05),
        (0.03, 25, 1, 0.1),
        (0.03, 25, 1, 0.15),
        (0.03, 25, 3, 0.05),
        (0.03, 25, 3, 0.15),
        (0.03, 25, 5, 0.05),
        (0.03, 25, 5, 0.1),
        (0.03, 25, 5, 0.15),
        (0.03, 30, 1, 0.05),
        (0.03, 30, 1, 0.1),
        (0.03, 30, 1, 0.15),
        (0.03, 30, 3, 0.05),
        (0.03, 30, 3, 0.1),
        (0.03, 30, 3, 0.15),
        (0.03, 30, 5, 0.05),
        (0.03, 30, 5, 0.1),
        (0.03, 30, 5, 0.15),
        (0.03, 30, 5, 0.15),
        (0.05, 15, 1, 0.05),
        (0.05, 15, 1, 0.1),
        (0.05, 15, 1, 0.15),
        (0.05, 15, 3, 0.05),
        (0.05, 15, 3, 0.1),
        (0.05, 15, 3, 0.15),
        (0.05, 15, 5, 0.05),
        (0.05, 15, 5, 0.1),
        (0.05, 15, 5, 0.15),
        (0.05, 20, 1, 0.05),
        (0.05, 20, 1, 0.1),
        (0.05, 20, 1, 0.15),
        (0.05, 20, 3, 0.05),
        (0.05, 20, 3, 0.1),
        (0.05, 20, 3, 0.15),
        (0.05, 20, 5, 0.05),
        (0.05, 20, 5, 0.1),
        (0.05, 20, 5, 0.15),
        (0.05, 25, 1, 0.05),
        (0.05, 25, 1, 0.1),
        (0.05, 25, 1, 0.15),
        (0.05, 25, 3, 0.05),
        (0.05, 25, 3, 0.1),
        (0.05, 25, 3, 0.15),
        (0.05, 25, 5, 0.05),
        (0.05, 25, 5, 0.1),
        (0.05, 25, 5, 0.15),
        (0.05, 30, 1, 0.05),
        (0.05, 30, 1, 0.1),
        (0.05, 30, 1, 0.15),
        (0.05, 30, 3, 0.05),
        (0.05, 30, 3, 0.1),
        (0.05, 30, 3, 0.15),
        (0.05, 30, 5, 0.05),
        (0.05, 30, 5, 0.1),
        (0.05, 30, 5, 0.15),
        (0.05, 30, 5, 0.15),
        (0.07, 15, 1, 0.05),
        (0.07, 15, 1, 0.1),
        (0.07, 15, 1, 0.15),
        (0.07, 15, 3, 0.05),
        (0.07, 15, 3, 0.1),
        (0.07, 15, 3, 0.15),
        (0.07, 15, 5, 0.05),
        (0.07, 15, 5, 0.1),
        (0.07, 15, 5, 0.15),
        (0.07, 20, 1, 0.05),
        (0.07, 20, 1, 0.1),
        (0.07, 20, 1, 0.15),
        (0.07, 20, 3, 0.05),
        (0.07, 20, 3, 0.1),
        (0.07, 20, 3, 0.15),
        (0.07, 20, 5, 0.05),
        (0.07, 20, 5, 0.1),
        (0.07, 20, 5, 0.15),
        (0.07, 25, 1, 0.05),
        (0.07, 25, 1, 0.1),
        (0.07, 25, 1, 0.15),
        (0.07, 25, 3, 0.05),
        (0.07, 25, 3, 0.1),
        (0.07, 25, 3, 0.15),
        (0.07, 25, 5, 0.05),
        (0.07, 25, 5, 0.1),
        (0.07, 25, 5, 0.15),
        (0.07, 30, 1, 0.05),
        (0.07, 30, 1, 0.1),
        (0.07, 30, 1, 0.15),
        (0.07, 30, 3, 0.05),
        (0.07, 30, 3, 0.1),
        (0.07, 30, 3, 0.15),
        (0.07, 30, 5, 0.05),
        (0.07, 30, 5, 0.1),
        (0.07, 30, 5, 0.15),
        (0.07, 30, 5, 0.15),
        (0.1, 15, 1, 0.05),
        (0.1, 15, 1, 0.1),
        (0.1, 15, 1, 0.15),
        (0.1, 15, 3, 0.05),
        (0.1, 15, 3, 0.1),
        (0.1, 15, 3, 0.15),
        (0.1, 15, 5, 0.05),
        (0.1, 15, 5, 0.1),
        (0.1, 15, 5, 0.15),
        (0.1, 20, 1, 0.05),
        (0.1, 20, 1, 0.1),
        (0.1, 20, 1, 0.15),
        (0.1, 20, 3, 0.05),
        (0.1, 20, 3, 0.1),
        (0.1, 20, 3, 0.15),
        (0.1, 20, 5, 0.05),
        (0.1, 20, 5, 0.1),
        (0.1, 20, 5, 0.15),
        (0.1, 20, 5, 0.15),
        (0.1, 25, 1, 0.05),
        (0.1, 25, 1, 0.1),
        (0.1, 25, 1, 0.15),
        (0.1, 25, 3, 0.05),
        (0.1, 25, 3, 0.1),
        (0.1, 25, 3, 0.15),
        (0.1, 25, 5, 0.05),
        (0.1, 25, 5, 0.1),
        (0.1, 25, 5, 0.15),
        (0.1, 25, 5, 0.15),
        (0.1, 30, 1, 0.05),
        (0.1, 30, 1, 0.1),
        (0.1, 30, 1, 0.15),
        (0.1, 30, 3, 0.05),
        (0.1, 30, 3, 0.1),
        (0.1, 30, 3, 0.15),
        (0.1, 30, 5, 0.05),
        (0.1, 30, 5, 0.1),
        (0.1, 30, 5, 0.15),
        (0.1, 30, 5, 0.15),
        (0.1, 30, 5, 0.15),
        (0.15, 15, 1, 0.05),
        (0.15, 15, 1, 0.1),
        (0.15, 15, 1, 0.15),
        (0.15, 15, 3, 0.05),
        (0.15, 15, 3, 0.1),
        (0.15, 15, 3, 0.15),
        (0.15, 15, 5, 0.05),
        (0.15, 15, 5, 0.1),
        (0.15, 15, 5, 0.15),
        (0.15, 15, 5, 0.15),
        (0.15, 20, 1, 0.05),
        (0.15, 20, 1, 0.1),
        (0.15, 20, 1, 0.15),
        (0.15, 20, 3, 0.05),
        (0.15, 20, 3, 0.1),
        (0.15, 20, 3, 0.15),
        (0.15, 20, 5, 0.05),
        (0.15, 20, 5, 0.1),
        (0.15, 20, 5, 0.15),
        (0.15, 20, 5, 0.15),
        (0.15, 25, 1, 0.05),
        (0.15, 25, 1, 0.1),
        (0.15, 25, 1, 0.15),
        (0.15, 25, 3, 0.05),
        (0.15, 25, 3, 0.1),
        (0.15, 25, 3, 0.15),
        (0.15, 25, 5, 0.05),
        (0.15, 25, 5, 0.1),
        (0.15, 25, 5, 0.15),
        (0.15, 25, 5, 0.15),
        (0.15, 30, 1, 0.05),
        (0.15, 30, 1, 0.1),
        (0.15, 30, 1, 0.15),
        (0.15, 30, 3, 0.05),
        (0.15, 30, 3, 0.1),
        (0.15, 30, 5, 0.05),
        (0.15, 30, 5, 0.1),
        (0.15, 30, 5, 0.15),
        (0.15, 30, 5, 0.15),
        (0.15, 30, 5, 0.15),
        (0.15, 30, 5, 0.15),
        (0.2, 15, 1, 0.05),
        (0.2, 15, 1, 0.1),
        (0.2, 15, 1, 0.15),
        (0.2, 15, 3, 0.05),
        (0.2, 15, 3, 0.1),
        (0.2, 15, 3, 0.15),
        (0.2, 15, 5, 0.05),
        (0.2, 15, 5, 0.1),
        (0.2, 15, 5, 0.15),
        (0.2, 15, 5, 0.15),
        (0.2, 20, 1, 0.05),
        (0.2, 20, 1, 0.1),
        (0.2, 20, 1, 0.15),
        (0.2, 20, 3, 0.05),
        (0.2, 20, 3, 0.1),
        (0.2, 20, 3, 0.15),
        (0.2, 20, 5, 0.05),
        (0.2, 20, 5, 0.1),
        (0.2, 20, 5, 0.15),
        (0.2, 20, 5, 0.15),
        (0.2, 25, 1, 0.05),
        (0.2, 25, 1, 0.1),
        (0.2, 25, 1, 0.15),
        (0.2, 25, 3, 0.05),
        (0.2, 25, 3, 0.1),
        (0.2, 25, 3, 0.15),
        (0.2, 25, 5, 0.05),
        (0.2, 25, 5, 0.1),
        (0.2, 25, 5, 0.15),
        (0.2, 25, 5, 0.15),
        (0.2, 30, 1, 0.05),
        (0.2, 30, 1, 0.1),
        (0.2, 30, 1, 0.15),
        (0.2, 30, 3, 0.05),
        (0.2, 30, 3, 0.1),
        (0.2, 30, 5, 0.05),
        (0.2, 30, 5, 0.1),
        (0.2, 30, 5, 0.15),
        (0.2, 30, 5, 0.15),
        (0.2, 30, 5, 0.15),
    ]
    
    # 转换为参数字典格式
    param_combinations = []
    for params in failed_params:
        param_dict = {
            'price_drop_threshold': params[0],
            'max_time_since_last_trade': params[1],
            'min_time_since_last_trade': params[2],
            'take_profit_threshold': params[3]
        }
        param_combinations.append(param_dict)
    
    # 3. 执行回测
    print(f"Starting retest for {len(param_combinations)} failed parameter combinations...")
    print(f"Using 2 threads for parallel processing")
    start_time = time.time()
    
    results = []
    with ProcessPoolExecutor(max_workers=2) as executor:
        future_to_params = {
            executor.submit(
                process_single_param_combination_with_logging,
                btc_data.copy(),
                params
            ): params
            for params in param_combinations
        }
        
        # 处理完成的任务
        for future in as_completed(future_to_params):
            params = future_to_params[future]
            try:
                result = future.result()
                if result:
                    results.append(result)
                    elapsed = time.time() - start_time
                    print(f"Completed {params[0]}/{params[1]}/{params[2]}/{params[3]} - Return: {result.get('total_return', 'N/A')}%, Time: {elapsed:.1f}s")
            except Exception as e:
                elapsed = time.time() - start_time
                print(f"Error in {params[0]}/{params[1]}/{params[2]}/{params[3]} - {str(e)}, Time: {elapsed:.1f}s")
    
    total_time = time.time() - start_time
    print(f"\nRetest completed! Total time: {total_time:.1f}s")
    print(f"Successfully tested: {len(results)}/{len(param_combinations)} combinations")
    
    # 4. 保存结果
    if results:
        results_df = pd.DataFrame(results)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        results_file = f"dca_retest_results_{timestamp}.csv"
        results_df.to_csv(results_file, index=False)
        print(f"Results saved to: {results_file}")
        
        # 统计成功和失败
        success_count = len([r for r in results if r.get('success', False)])
        error_count = len([r for r in results if not r.get('success', True)])
        
        print(f"\n=== Summary ===")
        print(f"Success: {success_count}")
        print(f"Errors: {error_count}")
        
        # 打印失败的参数组合
        failed_results = [r for r in results if not r.get('success', True)]
        if failed_results:
            print(f"\nFailed parameter combinations:")
            for r in failed_results[:10]:
                params = r
                print(f"  {params['price_drop_threshold']}/{params['max_time_since_last_trade']}/{params['min_time_since_last_trade']}/{params['take_profit_threshold']} - Error: {params.get('error', 'Unknown')}")
        
        # 打印成功的参数组合
        success_results = [r for r in results if r.get('success', True)]
        if success_results:
            print(f"\nSuccessful parameter combinations:")
            for r in success_results[:5]:
                params = r
                print(f"  {params['price_drop_threshold']}/{params['max_time_since_last_trade']}/{params['min_time_since_last_trade']}/{params['take_profit_threshold']} - Return: {params.get('total_return', 'N/A')}%")
    else:
        print("No valid results obtained")


if __name__ == "__main__":
    main()