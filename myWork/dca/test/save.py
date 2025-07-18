from datetime import datetime

import pymysql

from myWork.dca.test.mysql_read import MySQLDataReader
from myWork.dca.test.stg import DCAStrategy


def save_strategy_performance(db_config, performance, strategy_config, start_time, end_time, currency, debug=False):
    """将策略回测结果和配置参数保存到MySQL数据库"""
    # 定义需要验证的字段列表
    required_performance_fields = [
        'total_return', 'annualized_return', 'sharpe_ratio', 'max_drawdown',
        'trade_count', 'dca_count', 'take_profit_count', 'win_rate',
        'final_portfolio_value'
    ]

    required_strategy_fields = [
        'price_drop_threshold', 'max_time_since_last_trade', 'min_time_since_last_trade',
        'take_profit_threshold', 'initial_capital', 'initial_investment_ratio', 'initial_dca_value'
    ]

    try:
        # 验证performance字典中的字段
        for field in required_performance_fields:
            if field not in performance:
                raise ValueError(f"performance缺少必要的字段: {field}")
            if performance[field] is None:
                raise ValueError(f"performance字段 '{field}' 的值为None")

        # 验证strategy_config字典中的字段
        for field in required_strategy_fields:
            if field not in strategy_config:
                raise ValueError(f"strategy_config缺少必要的字段: {field}")
            if strategy_config[field] is None:
                raise ValueError(f"strategy_config字段 '{field}' 的值为None")

        # 检查时间参数
        if not isinstance(start_time, (str, datetime)):
            raise TypeError(f"start_time类型错误，期望str或datetime，得到{type(start_time)}")
        if not isinstance(end_time, (str, datetime)):
            raise TypeError(f"end_time类型错误，期望str或datetime，得到{type(end_time)}")

        # 检查currency参数
        if not isinstance(currency, str):
            raise TypeError(f"currency类型错误，期望str，得到{type(currency)}")

        connection = pymysql.connect(**db_config)
        with connection.cursor() as cursor:
            sql = """
            INSERT INTO strategy_performance 
            (currency, total_return, annualized_return, sharpe_ratio, max_drawdown, 
             trade_count, dca_count, take_profit_count, win_rate, final_portfolio_value,
             price_drop_threshold, max_time_since_last_trade, min_time_since_last_trade,
             take_profit_threshold, initial_capital, initial_investment_ratio, initial_dca_value,
             total_fees,
             start_time, end_time)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            # 准备所有参数值
            params = (
                currency,
                performance['total_return'],
                performance['annualized_return'],
                performance['sharpe_ratio'],
                performance['max_drawdown'],
                performance['trade_count'],
                performance['dca_count'],
                performance['take_profit_count'],
                performance['win_rate'],
                performance['final_portfolio_value'],
                strategy_config['price_drop_threshold'],
                strategy_config['max_time_since_last_trade'],
                strategy_config['min_time_since_last_trade'],
                strategy_config['take_profit_threshold'],
                strategy_config['initial_capital'],
                strategy_config['initial_investment_ratio'],
                strategy_config['initial_dca_value'],
                performance.get('total_fees', 0),
                start_time,
                end_time
            )

            # 仅在debug模式下打印参数信息
            if debug:
                print("\n准备插入的参数:")
                print(f"currency: {currency}")
                for field in required_performance_fields:
                    print(f"performance['{field}']: {performance[field]} ({type(performance[field])})")
                for field in required_strategy_fields:
                    print(f"strategy_config['{field}']: {strategy_config[field]} ({type(strategy_config[field])})")
                print(f"total_fees: {performance.get('total_fees', 0)}")
                print(f"start_time: {start_time} ({type(start_time)})")
                print(f"end_time: {end_time} ({type(end_time)})")

            cursor.execute(sql, params)
        connection.commit()

        # 仅在debug模式下打印成功信息
        if debug:
            print(f"\n{currency} 数据已成功提交到数据库")

    except ValueError as ve:
        # 仅在debug模式下打印错误信息
        if debug:
            print(f"\n数据验证错误: {ve}")
        raise
    except TypeError as te:
        # 仅在debug模式下打印错误信息
        if debug:
            print(f"\n类型错误: {te}")
        raise
    except pymysql.Error as e:
        # 仅在debug模式下打印数据库错误信息
        if debug:
            print(f"\nMySQL错误 ({e.args[0]}): {e.args[1]}")
            # 获取错误的SQL语句
            print(f"错误的SQL: {cursor.mogrify(sql, params).decode('utf-8')}")
        raise
    except Exception as e:
        # 仅在debug模式下打印其他错误信息
        if debug:
            print(f"\n保存{currency}数据到数据库时出错: {e}")
            import traceback
            traceback.print_exc()
        raise
    finally:
        if connection and connection.open:
            connection.close()


def run_strategy_df(config, db_config, start_time, end_time, df):
    """运行策略并返回性能指标"""
    try:

        if df.empty:
            reader = MySQLDataReader(**db_config)
            reader.connect()
            df = reader.get_sorted_history_data(start_time, end_time, config.get('currency', 'UNKNOWN'))
            reader.disconnect()

        df1 = df.copy(deep=True)  # 确保所有层级数据独立

        strategy = DCAStrategy(**config)
        performance = strategy.backtest(df1)

        # 保存到数据库，从配置中获取币种
        save_strategy_performance(
            db_config,
            performance,
            config,
            start_time,
            end_time,
            config.get('currency', 'UNKNOWN')  # 从配置中获取币种，如果没有则使用默认值
        )

        return {
            'config': config,
            'performance': performance
        }
    except Exception as e:
        print(f"运行策略时发生错误: {e}")
        return None
