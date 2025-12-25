import pickle

import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader, TensorDataset
import numpy as np
import matplotlib.pyplot as plt
from sklearn.preprocessing import StandardScaler
import pandas as pd

from myWork.model.prepare_data import prepare_training_data_from_db, load_training_data


class LSTMModel(nn.Module):
    """LSTM价格预测模型"""

    def __init__(self, input_size, hidden_size, num_layers, output_size, dropout=0.2):
        """初始化LSTM模型

        参数:
            input_size: 输入特征维度
            hidden_size: LSTM隐藏层单元数
            num_layers: LSTM层数
            output_size: 输出维度
            dropout: Dropout比率
        """
        super(LSTMModel, self).__init__()
        self.hidden_size = hidden_size
        self.num_layers = num_layers

        self.lstm = nn.LSTM(
            input_size=input_size,
            hidden_size=hidden_size,
            num_layers=num_layers,
            batch_first=True,
            dropout=dropout if num_layers > 1 else 0
        )

        self.fc = nn.Sequential(
            nn.Linear(hidden_size, 50),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(50, output_size)
        )

    def forward(self, x):
        """前向传播

        参数:
            x: 输入张量

        返回:
            torch.Tensor: 输出张量
        """
        h0 = torch.zeros(self.num_layers, x.size(0), self.hidden_size).to(x.device)
        c0 = torch.zeros(self.num_layers, x.size(0), self.hidden_size).to(x.device)

        out, _ = self.lstm(x, (h0, c0))

        out = self.fc(out[:, -1, :])
        return out


def train_lstm_model(X_train, y_train, X_test, y_test,
                     input_size, hidden_size=64, num_layers=2, output_size=1,
                     batch_size=64, epochs=50, lr=0.001, device='cuda'):
    """训练LSTM模型并返回训练好的模型和训练历史

    参数:
        X_train: 训练特征数据
        y_train: 训练目标数据
        X_test: 测试特征数据
        y_test: 测试目标数据
        input_size: 输入特征维度
        hidden_size: LSTM隐藏层单元数
        num_layers: LSTM层数
        output_size: 输出维度
        batch_size: 批次大小
        epochs: 训练轮数
        lr: 学习率
        device: 训练设备（'cuda' 或 'cpu'）

    返回:
        tuple: (model, history) 模型和训练历史
    """
    X_train_tensor = torch.FloatTensor(X_train).to(device)
    y_train_tensor = torch.FloatTensor(y_train).to(device)
    X_test_tensor = torch.FloatTensor(X_test).to(device)
    y_test_tensor = torch.FloatTensor(y_test).to(device)

    train_dataset = TensorDataset(X_train_tensor, y_train_tensor)
    train_loader = DataLoader(train_dataset, batch_size=batch_size, shuffle=True)

    model = LSTMModel(input_size, hidden_size, num_layers, output_size).to(device)

    criterion = nn.MSELoss()
    optimizer = optim.Adam(model.parameters(), lr=lr)
    scheduler = optim.lr_scheduler.ReduceLROnPlateau(optimizer, 'min', patience=5, factor=0.5)

    train_losses = []
    val_losses = []
    best_val_loss = float('inf')

    for epoch in range(epochs):
        print("開始訓練")
        model.train()
        train_loss = 0
        for X_batch, y_batch in train_loader:
            outputs = model(X_batch)
            loss = criterion(outputs, y_batch)

            optimizer.zero_grad()
            loss.backward()
            optimizer.step()

            train_loss += loss.item() * X_batch.size(0)

        model.eval()
        with torch.no_grad():
            val_outputs = model(X_test_tensor)
            val_loss = criterion(val_outputs, y_test_tensor).item()

        train_loss = train_loss / len(train_loader.dataset)
        train_losses.append(train_loss)
        val_losses.append(val_loss)

        scheduler.step(val_loss)

        if val_loss < best_val_loss:
            best_val_loss = val_loss

        print(f'Epoch [{epoch + 1}/{epochs}], Train Loss: {train_loss:.6f}, Val Loss: {val_loss:.6f}')

    return model, {'train_loss': train_losses, 'val_loss': val_losses}


def train_model_for_currency(currency='BTC-USDT', table_name='sorted_history_15m',
                            lookback=60, forecast=1, split_ratio=0.8,
                            hidden_size=64, num_layers=2, batch_size=64, epochs=50, lr=0.001,
                            save_dir='./models', device='cuda'):
    """为指定币种训练LSTM模型

    参数:
        currency: 币种标识（如 BTC-USDT）
        table_name: 数据表名
        lookback: 用于预测的历史数据长度
        forecast: 预测未来数据点的数量
        split_ratio: 训练集与测试集的划分比例
        hidden_size: LSTM隐藏层单元数
        num_layers: LSTM层数
        batch_size: 批次大小
        epochs: 训练轮数
        lr: 学习率
        save_dir: 模型保存目录
        device: 训练设备

    返回:
        tuple: (model, scaler, history)
    """
    import os
    os.makedirs(save_dir, exist_ok=True)

    print(f"\n{'='*60}")
    print(f"开始训练 {currency} 的LSTM模型")
    print(f"{'='*60}")

    X_train, X_test, y_train, y_test, scaler, df_processed = prepare_training_data_from_db(
        currency=currency,
        table_name=table_name,
        lookback=lookback,
        forecast=forecast,
        split_ratio=split_ratio
    )

    input_size = X_train.shape[2]

    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    print(f'Using device: {device}')

    model, history = train_lstm_model(
        X_train, y_train, X_test, y_test,
        input_size=input_size,
        hidden_size=hidden_size,
        num_layers=num_layers,
        output_size=forecast,
        batch_size=batch_size,
        epochs=epochs,
        lr=lr,
        device=device
    )

    coin_name = currency.replace('-', '_')
    model_path = os.path.join(save_dir, f"best_lstm_model_{coin_name}.pth")
    torch.save(model.state_dict(), model_path)
    print(f"模型已保存到: {model_path}")

    scaler_path = os.path.join(save_dir, f"scaler_{coin_name}.pkl")
    with open(scaler_path, "wb") as f:
        pickle.dump(scaler, f)
    print(f"标准化器已保存到: {scaler_path}")

    return model, scaler, history


def predict_with_model(model, X, scaler, device='cuda'):
    """使用训练好的模型进行预测并反标准化结果

    参数:
        model: 训练好的LSTM模型
        X: 输入特征数据
        scaler: 标准化器实例
        device: 计算设备

    返回:
        np.ndarray: 反标准化后的预测值
    """
    model.eval()
    with torch.no_grad():
        X_tensor = torch.FloatTensor(X).to(device)
        predictions = model(X_tensor).cpu().numpy()

    temp_array = np.zeros((predictions.shape[0], scaler.n_features_in_))
    temp_array[:, 3] = predictions.flatten()
    temp_array = scaler.inverse_transform(temp_array)
    predictions = temp_array[:, 3].reshape(-1, 1)

    return predictions


def plot_training_history(history, currency='BTC-USDT', save_dir='./plots'):
    """绘制训练历史

    参数:
        history: 训练历史字典
        currency: 币种标识
        save_dir: 图片保存目录
    """
    import os
    os.makedirs(save_dir, exist_ok=True)

    plt.figure(figsize=(10, 5))
    plt.plot(history['train_loss'], label='Train Loss')
    plt.plot(history['val_loss'], label='Validation Loss')
    plt.title(f'{currency} Training and Validation Loss')
    plt.xlabel('Epoch')
    plt.ylabel('Loss')
    plt.legend()
    plt.grid(True)

    coin_name = currency.replace('-', '_')
    plt.savefig(os.path.join(save_dir, f"history_{coin_name}.png"), dpi=300)
    plt.close()


def plot_predictions(y_true, y_pred, currency='BTC-USDT', title='Price Prediction', save_dir='./plots'):
    """绘制预测结果与实际值对比图

    参数:
        y_true: 真实值
        y_pred: 预测值
        currency: 币种标识
        title: 图表标题
        save_dir: 图片保存目录
    """
    import os
    os.makedirs(save_dir, exist_ok=True)

    plt.figure(figsize=(12, 6))
    plt.plot(y_true, label='Actual Price')
    plt.plot(y_pred, label='Predicted Price')
    plt.title(f'{currency} {title}')
    plt.xlabel('Time')
    plt.ylabel('Price')
    plt.legend()
    plt.grid(True)

    coin_name = currency.replace('-', '_')
    plt.savefig(os.path.join(save_dir, f"prediction_{coin_name}.png"), dpi=300)
    plt.close()


def load_model_for_currency(currency='BTC-USDT', input_size=8, hidden_size=64, num_layers=2,
                          output_size=1, model_dir='./models', device='cuda'):
    """加载指定币种的训练好的模型

    参数:
        currency: 币种标识
        input_size: 输入特征维度
        hidden_size: LSTM隐藏层单元数
        num_layers: LSTM层数
        output_size: 输出维度
        model_dir: 模型目录
        device: 计算设备

    返回:
        tuple: (model, scaler)
    """
    import os
    coin_name = currency.replace('-', '_')

    model_path = os.path.join(model_dir, f"best_lstm_model_{coin_name}.pth")
    scaler_path = os.path.join(model_dir, f"scaler_{coin_name}.pkl")

    model = LSTMModel(input_size, hidden_size, num_layers, output_size).to(device)
    model.load_state_dict(torch.load(model_path))
    model.eval()

    with open(scaler_path, "rb") as f:
        scaler = pickle.load(f)

    print(f"已加载 {currency} 的模型和标准化器")
    return model, scaler


if __name__ == "__main__":
    model, scaler, history = train_model_for_currency(
        currency='BTC-USDT',
        lookback=60,
        forecast=1,
        split_ratio=0.8,
        hidden_size=16,
        num_layers=2,
        batch_size=16,
        epochs=0,
        lr=0.001,
        save_dir='./models'
    )

    plot_training_history(history, currency='BTC-USDT')
