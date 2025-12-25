import pickle

import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler

from myWork.process.read import load_kline_from_db, load_kline_from_file


def calculate_rsi(data, period=14):
    """计算相对强弱指数（RSI）

    参数:
        data: 价格序列
        period: RSI周期，默认14

    返回:
        pd.Series: RSI值序列
    """
    deltas = data.diff()
    up = deltas.clip(lower=0)
    down = -deltas.clip(upper=0)
    avg_gain = up.rolling(window=period).mean()
    avg_loss = down.rolling(window=period).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi


def prepare_training_data_from_file(file_path: str, lookback: int = 60, forecast: int = 1, split_ratio: float = 0.8) -> tuple:
    """从文件准备训练数据

    参数:
        file_path: 历史K线数据文件路径
        lookback: 用于预测的历史数据长度
        forecast: 预测未来数据点的数量
        split_ratio: 训练集与测试集的划分比例

    返回:
        tuple: 包含以下元素的元组
            - X_train: 训练特征数据，形状为 (样本数, 时间步长, 特征数)
            - X_test: 测试特征数据，形状为 (样本数, 时间步长, 特征数)
            - y_train: 训练目标数据，形状为 (样本数, 预测步长)
            - y_test: 测试目标数据，形状为 (样本数, 预测步长)
            - scaler: 标准化器实例，用于后续数据还原
            - df_processed: 处理后的完整DataFrame
    """
    df = load_kline_from_file(file_path)
    return _prepare_training_data(df, lookback, forecast, split_ratio)


def prepare_training_data_from_db(currency: str = 'BTC-USDT', table_name: str = 'sorted_history_15m',
                                  lookback: int = 60, forecast: int = 1, split_ratio: float = 0.8) -> tuple:
    """从数据库准备训练数据

    参数:
        currency: 币种标识（如 BTC-USDT）
        table_name: 数据表名
        lookback: 用于预测的历史数据长度
        forecast: 预测未来数据点的数量
        split_ratio: 训练集与测试集的划分比例

    返回:
        tuple: 包含以下元素的元组
            - X_train: 训练特征数据，形状为 (样本数, 时间步长, 特征数)
            - X_test: 测试特征数据，形状为 (样本数, 时间步长, 特征数)
            - y_train: 训练目标数据，形状为 (样本数, 预测步长)
            - y_test: 测试目标数据，形状为 (样本数, 预测步长)
            - scaler: 标准化器实例，用于后续数据还原
            - df_processed: 处理后的完整DataFrame
    """
    df = load_kline_from_db(currency=currency, table_name=table_name)
    return _prepare_training_data(df, lookback, forecast, split_ratio)


def _prepare_training_data(df: pd.DataFrame, lookback: int = 60, forecast: int = 1, split_ratio: float = 0.8) -> tuple:
    """内部函数：处理K线数据并准备用于模型训练的序列数据

    参数:
        df: K线数据DataFrame
        lookback: 用于预测的历史数据长度
        forecast: 预测未来数据点的数量
        split_ratio: 训练集与测试集的划分比例

    返回:
        tuple: 包含以下元素的元组
            - X_train: 训练特征数据
            - X_test: 测试特征数据
            - y_train: 训练目标数据
            - y_test: 测试目标数据
            - scaler: 标准化器实例
            - df_processed: 处理后的完整DataFrame
    """
    if df.empty:
        raise ValueError("输入数据为空")

    df['ma5'] = df['c'].rolling(5).mean()
    df['ma10'] = df['c'].rolling(10).mean()
    df['rsi'] = calculate_rsi(df['c'], 14)

    df.dropna(inplace=True)

    columns_to_scale = ['o', 'h', 'l', 'c', 'vol', 'ma5', 'ma10', 'rsi']
    scaler = StandardScaler()
    df_scaled = df.copy()
    df_scaled[columns_to_scale] = scaler.fit_transform(df[columns_to_scale])

    X, y = [], []
    for i in range(len(df_scaled) - lookback - forecast + 1):
        X.append(df_scaled.iloc[i:i + lookback][columns_to_scale].values)
        y.append(df_scaled['c'].iloc[i + lookback:i + lookback + forecast].values)
    X, y = np.array(X), np.array(y)

    split_idx = int(len(X) * split_ratio)
    X_train, X_test = X[:split_idx], X[split_idx:]
    y_train, y_test = y[:split_idx], y[split_idx:]

    return X_train, X_test, y_train, y_test, scaler, df_scaled


def save_training_data(X_train, X_test, y_train, y_test, scaler, df_processed, currency='BTC-USDT', save_dir='./data'):
    """保存训练数据到文件

    参数:
        X_train: 训练特征数据
        X_test: 测试特征数据
        y_train: 训练目标数据
        y_test: 测试目标数据
        scaler: 标准化器实例
        df_processed: 处理后的DataFrame
        currency: 币种标识，用于文件命名
        save_dir: 保存目录
    """
    import os
    os.makedirs(save_dir, exist_ok=True)

    coin_name = currency.replace('-', '_')

    np.save(os.path.join(save_dir, f"X_train_{coin_name}.npy"), X_train)
    np.save(os.path.join(save_dir, f"X_test_{coin_name}.npy"), X_test)
    np.save(os.path.join(save_dir, f"y_train_{coin_name}.npy"), y_train)
    np.save(os.path.join(save_dir, f"y_test_{coin_name}.npy"), y_test)

    with open(os.path.join(save_dir, f"scaler_{coin_name}.pkl"), "wb") as f:
        pickle.dump(scaler, f)

    df_processed.to_parquet(os.path.join(save_dir, f"df_processed_{coin_name}.parquet"))

    print(f"训练数据已保存到 {save_dir} (币种: {currency})")


def load_training_data(currency='BTC-USDT', save_dir='./data'):
    """从文件加载训练数据

    参数:
        currency: 币种标识
        save_dir: 数据保存目录

    返回:
        tuple: X_train, X_test, y_train, y_test, scaler, df_processed
    """
    import os
    coin_name = currency.replace('-', '_')

    X_train = np.load(os.path.join(save_dir, f"X_train_{coin_name}.npy"))
    X_test = np.load(os.path.join(save_dir, f"X_test_{coin_name}.npy"))
    y_train = np.load(os.path.join(save_dir, f"y_train_{coin_name}.npy"))
    y_test = np.load(os.path.join(save_dir, f"y_test_{coin_name}.npy"))

    with open(os.path.join(save_dir, f"scaler_{coin_name}.pkl"), "rb") as f:
        scaler = pickle.load(f)

    df_processed = pd.read_parquet(os.path.join(save_dir, f"df_processed_{coin_name}.parquet"))

    print(f"训练数据已从 {save_dir} 加载 (币种: {currency})")
    return X_train, X_test, y_train, y_test, scaler, df_processed


if __name__ == "__main__":
    X_train, X_test, y_train, y_test, scaler, df_processed = prepare_training_data_from_db(
        currency='BTC-USDT',
        lookback=60,
        forecast=1,
        split_ratio=0.8
    )

    print("训练集X形状:", X_train.shape)
    print("测试集X形状:", X_test.shape)
    print("训练集y形状:", y_train.shape)
    print("测试集y形状:", y_test.shape)

    save_training_data(X_train, X_test, y_train, y_test, scaler, df_processed, currency='BTC-USDT')
