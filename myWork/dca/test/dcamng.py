import datetime
import os
import sys
from itertools import product
from multiprocessing import get_context
from pathlib import Path

# Correct project root path (4 levels up to reach python-okx1 directory)
sys.path.append(str(Path(__file__).parent.parent.parent.parent))
from dotenv import load_dotenv
import numpy as np
import pandas as pd
from myWork.dca.test.mysql_read import MySQLDataReader
from myWork.dca.test.save import run_strategy_df


def generate_range(min_val, max_val, step):
    """生成从min到max的等间隔数值列表"""
    return list(np.arange(min_val, max_val + step, step))

def handle_worker_error(e):
    print(f"Worker进程异常: {str(e)}")

def worker(db_config, df, start_time, end_time):
    """工作函数，每个进程将从数据库获取参数并处理"""
    # 每个worker创建自己的数据库连接
    try:
        worker_reader = MySQLDataReader(**db_config)
        worker_reader.connect()
        while True:
            # 获取未执行的参数
            try:
                param_id, params = worker_reader.get_unexecuted_parameter()
                if not param_id:
                    break  # 没有更多参数，退出循环

                # 处理参数
                try:
                    # 移除currency参数避免策略初始化错误
                    params_clean = params.copy()
                    params_clean.pop('currency', None)
                    result = run_strategy_df(params_clean, db_config, start_time, end_time, df)
                    worker_reader.update_parameter_status(param_id, 'completed', result)
                except Exception as e:
                    error_msg = f"处理参数 {param_id} 时出错: {str(e)}"
                    worker_reader.update_parameter_status(param_id, 'failed', error_msg)
            except Exception as e:
                break
    except Exception as e:
        pass
    finally:
        if 'worker_reader' in locals():
            worker_reader.disconnect()

def parameter_range_training(db_config, start_time, end_time, base_strategy_config, n_jobs=1):
    """
    从数据库获取参数并执行训练，每次处理一行参数
    
    参数:
    db_config - 数据库连接配置
    start_time, end_time - 回测时间范围
    n_jobs - 并行处理数
    """
    # 创建数据库连接
    reader = MySQLDataReader(**db_config)
    reader.connect()
    
    # 获取历史数据
    # 使用基础配置中的货币对参数
    df = reader.get_sorted_history_data(start_time, end_time, base_strategy_config['currency'])
    
    # 使用多进程处理参数
    # 根据操作系统选择合适的多进程启动方式
    if sys.platform.startswith('win'):
        context = get_context('spawn')  # Windows系统使用spawn
    else:
        context = get_context('fork')   # Unix/Linux系统使用fork
    with context.Pool(processes=n_jobs) as pool:
        # 启动n_jobs个worker进程
        for i in range(n_jobs):
            pool.apply_async(worker, args=(db_config, df, start_time, end_time), error_callback=handle_worker_error)
        
        # 等待所有worker完成
        pool.close()
        pool.join()
    
    reader.disconnect()
    
    # 训练完成后分析结果
    # analyze_training_results(db_config)
    # print("所有参数训练完成!已生成结果报告")

def generate_and_insert_parameters(db_config, base_config, param_ranges):
    """生成参数组合并插入数据库，只执行一次"""
    reader = MySQLDataReader(**db_config)
    reader.connect()
    
    # 创建参数表
    reader.create_parameter_table()
    
    # 生成所有参数组合
    param_names = list(param_ranges.keys())
    param_values = list(param_ranges.values())
    param_combinations = list(product(*param_values))
    
    # 创建所有配置组合并插入数据库
    all_configs = []
    for params in param_combinations:
        config = base_config.copy()
        for name, value in zip(param_names, params):
            config[name] = value
        all_configs.append(config)
    
    # 插入参数到数据库
    reader.insert_parameters(all_configs)
    print(f"=== 参数生成完成，共{len(all_configs)}个组合 ===")
    try:
        reader.insert_parameters(all_configs)
        print(f"成功插入{len(all_configs)}个参数组合到数据库")
    except Exception as e:
        print(f"参数插入失败: {str(e)}")
    
    reader.disconnect()



load_dotenv()
MYSQL_CONN = os.getenv("MYSQL_CONN")
MYSQL_PASS = os.getenv("MYSQL_PASS")


def main():
    # 配置数据库连接信息
    db_config = {
        'host': MYSQL_CONN,
        'user': 'root',
        'password': MYSQL_PASS,
        'database': 'trading_db',
        'port': 3306
    }

    # 基础策略配置
    base_strategy_config = {
        'currency': 'BTC-USDT',  # 添加货币对参数
        'price_drop_threshold': 0.02,
        'max_time_since_last_trade': 96,
        'min_time_since_last_trade': 24,
        'take_profit_threshold': 0.01,
        'initial_capital': 100000,
        'initial_investment_ratio': 0.1,
        'initial_dca_value': 0.035
    }

    # 定义参数范围
    parameter_ranges = {
        'price_drop_threshold': generate_range(0.01, 0.05, 0.005),
        'max_time_since_last_trade': generate_range(24, 120, 24),
        'min_time_since_last_trade': generate_range(6, 48, 6),
        'take_profit_threshold': generate_range(0.005, 0.03, 0.005),
        'initial_investment_ratio': generate_range(0.05, 0.3, 0.05),
        'initial_dca_value': generate_range(0.02, 0.2, 0.005)
    }

    # 强制重新生成参数（用于测试）
    print("=== 开始生成并插入参数 ===")
    # 移除currency参数后再生成策略参数
    param_base = base_strategy_config.copy()
    if 'currency' in param_base:
        del param_base['currency']
    # generate_and_insert_parameters(db_config, param_base, parameter_ranges)

    # 配置数据时间范围
    end_time = datetime.datetime(2025, 6, 8)
    start_time = end_time - pd.Timedelta(days=120)

    # 执行参数训练
    parameter_range_training(db_config, start_time, end_time, base_strategy_config, n_jobs=1)


if __name__ == "__main__":
    main()
