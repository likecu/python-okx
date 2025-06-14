import numpy
import numpy as np
import pandas as pd
import torch

from myWork.model.LSTM import LSTMModel
from myWork.model.prepare_data import prepare_training_data, calculate_rsi


def predict_with_model(model, X, scaler, device='cuda'):
    model.eval()
    with torch.no_grad():
        X_tensor = torch.FloatTensor(X).to(device)
        predictions = model(X_tensor).cpu().numpy()

    # 假设特征数为8（与数据预处理时一致）
    temp_array = np.zeros((predictions.shape[0], 8))  # 直接指定特征数
    temp_array[:, 3] = predictions.flatten()  # 'c'在第4列（索引3）
    predictions = scaler.inverse_transform(temp_array)[:, 3].reshape(-1, 1)
    return predictions


def calculate_ma_signals_lstm(df, lookback=60, short_window=5, long_window=20, device='cuda'):
    """
    结合LSTM模型预测结果和双均线策略生成交易信号

    参数:
        df: 包含原始K线数据的DataFrame
        lookback: 模型输入的历史序列长度
        short_window: 短期均线窗口大小
        long_window: 长期均线窗口大小
        device: 模型运行设备
    """
    # 获取模型和标准化器
    a,model, scaler,c = get_model()  # 假设此函数返回模型和scaler

    # 复制原始数据
    df_processed = df.copy()

    # 准备模型预测所需的特征（与训练时一致）
    columns_to_scale = ['o', 'h', 'l', 'c', 'vol', 'ma5', 'ma10', 'rsi']

    # 添加技术指标
    if 'ma5' not in df_processed.columns:
        df_processed['ma5'] = df_processed['c'].rolling(5).mean()
    if 'ma10' not in df_processed.columns:
        df_processed['ma10'] = df_processed['c'].rolling(10).mean()
    if 'rsi' not in df_processed.columns:
        df_processed['rsi'] = calculate_rsi(df_processed['c'], 14)

    # 移除包含NaN的行
    df_processed = df_processed.dropna(subset=columns_to_scale)

    # 数据标准化
    df_scaled = df_processed.copy()
    df_scaled[columns_to_scale] = scaler.transform(df_processed[columns_to_scale])

    # 创建模型输入序列（内存优化版）
    def create_sequences(data, lookback):
        sequences = []
        for i in range(len(data) - lookback + 1):
            sequences.append(data.iloc[i:i + lookback].values)
        return np.array(sequences)

    X = create_sequences(df_scaled[columns_to_scale], lookback)

    # 使用分批预测减少内存占用
    def predict_in_batches(model, X, device, batch_size=512):
        model.eval()
        predictions = []

        for i in range(0, len(X), batch_size):
            batch = X[i:i + batch_size]
            with torch.no_grad():
                batch_tensor = torch.FloatTensor(batch).to(device)
                batch_preds = model(batch_tensor).cpu().numpy()
                predictions.append(batch_preds)
            # 释放GPU内存
            del batch_tensor
            torch.cuda.empty_cache()

        return np.concatenate(predictions, axis=0)

    # 执行预测
    predictions = predict_in_batches(model, X, device)

    # 反标准化预测结果（优化内存）
    def inverse_transform_predictions(predictions, scaler, feature_index=3):
        """高效反标准化预测结果，避免创建大型临时数组"""
        # 创建仅包含预测值的数组
        pred_array = np.zeros((len(predictions), scaler.n_features_in_))
        pred_array[:, feature_index] = predictions.flatten()

        # 反标准化
        inverse_transformed = scaler.inverse_transform(pred_array)
        return inverse_transformed[:, feature_index]

    predicted_prices = inverse_transform_predictions(predictions, scaler, 3)

    # 创建预测价格的DataFrame
    pred_start_idx = lookback - 1
    df_pred = pd.DataFrame({
        'ts': df_processed['ts'].iloc[pred_start_idx:].values,
        'predicted_c': predicted_prices
    })

    # 计算预测价格的双均线
    df_pred['ma_short'] = df_pred['predicted_c'].rolling(short_window).mean()
    df_pred['ma_long'] = df_pred['predicted_c'].rolling(long_window).mean()

    # 生成交易信号
    df_pred['signal'] = np.where(
        df_pred['ma_short'] > df_pred['ma_long'], 1,
        np.where(df_pred['ma_short'] < df_pred['ma_long'], -1, 0)
    )

    # 合并原始数据和预测信号
    df_merged = pd.merge(
        df_processed,
        df_pred[['ts', 'predicted_c', 'ma_short', 'ma_long', 'signal']],
        on='ts',
        how='left'
    )

    # 移除没有预测信号的行
    df_merged = df_merged.dropna(subset=['signal']).reset_index(drop=True)

    return df_merged

def print_img():
    X_test_new, model, scaler, y_test_new = get_model()

    # 对最新测试数据进行预测
    y_pred_new = predict_with_model(model, X_test_new, scaler)

    # 反标准化真实值（仅用于评估）
    temp_true = numpy.zeros((y_test_new.shape[0], 8))
    temp_true[:, 3] = y_test_new.flatten()
    y_true_new = scaler.inverse_transform(temp_true)[:, 3].reshape(-1, 1)

    import numpy as np
    import matplotlib.pyplot as plt

    # 计算评估指标
    mse = np.mean((y_pred_new - y_true_new) ** 2)
    mae = np.mean(np.abs(y_pred_new - y_true_new))
    mape = np.mean(np.abs((y_pred_new - y_true_new) / y_true_new)) * 100

    print(f"New Test MSE: {mse:.4f}")
    print(f"New Test MAE: {mae:.4f}")
    print(f"New Test MAPE: {mape:.2f}%")

    # 绘制预测结果
    plt.figure(figsize=(12, 6))
    plt.plot(y_true_new, label='Actual Price', alpha=0.7)
    plt.plot(y_pred_new, label='Predicted Price', alpha=0.7, linestyle='--')
    plt.title('Latest Data Price Prediction')
    plt.xlabel('Time Steps')
    plt.ylabel('Price')
    plt.legend()
    plt.grid(True)
    plt.savefig('latest_prediction.png', dpi=300)


def get_model():
    # 假设最新数据文件路径为CONFIG["SAVE_PATH"] + CONFIG["FINAL_FILE"]
    file_path = "../sorted_history_71.csv"  # 替换为实际路径
    # 调用数据预处理函数（无需划分训练集，仅需生成测试数据）
    # 设置split_ratio=1.0，使所有数据作为测试集（或根据需求调整）
    X_test_new, _, y_test_new, _, scaler, df_processed = prepare_training_data(
        file_path, lookback=60, forecast=1, split_ratio=1.0
    )
    model = LSTMModel(
        input_size=X_test_new.shape[2],  # 特征数，由数据预处理结果决定
        hidden_size=16,  # 与训练时一致
        num_layers=2,
        output_size=1  # 预测步数（forecast=1）
    ).to(torch.device('cuda' if torch.cuda.is_available() else 'cpu'))
    # 加载训练好的权重
    model.load_state_dict(torch.load('best_lstm_model.pth'))
    model.eval()  # 设置为评估模式
    return X_test_new, model, scaler, y_test_new


if __name__ == "__main__":
    print_img()





