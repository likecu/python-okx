# 多币种数据爬取和训练使用说明

## 概述

本项目已升级为支持多币种（BTC、ETH、BNB、OKB等）的量化交易系统。所有币种的数据都存储在同一个数据库表中，通过 `currency` 字段区分。

## 快速开始

### 1. 数据库迁移

首先需要执行数据库迁移脚本，修改表结构以支持多币种：

```bash
mysql -u root -p < database/migrate_multi_coin.sql
```

### 2. 配置交易对

编辑 `config/trading_pairs.json` 文件，启用或禁用需要交易的币种：

```json
{
  "trading_pairs": [
    {
      "symbol": "BTC-USDT",
      "name": "Bitcoin",
      "enabled": true,
      "time_range_days": 730,
      "bar": "15m"
    },
    {
      "symbol": "ETH-USDT",
      "name": "Ethereum",
      "enabled": true,
      "time_range_days": 730,
      "bar": "15m"
    }
  ]
}
```

## 使用方法

### 数据爬取

#### 批量爬取所有币种的历史数据

```bash
python fetch_all_coins.py
```

#### 爬取单个币种的最新数据

```bash
python fetch_latest_price.py
```

### 模型训练

#### 批量训练所有币种的LSTM模型

```bash
python train_all_coins.py --epochs 50 --hidden_size 64
```

参数说明：
- `--lookback`: 用于预测的历史数据长度（默认60）
- `--forecast`: 预测未来数据点的数量（默认1）
- `--hidden_size`: LSTM隐藏层单元数（默认64）
- `--num_layers`: LSTM层数（默认2）
- `--batch_size`: 批次大小（默认64）
- `--epochs`: 训练轮数（默认50）
- `--lr`: 学习率（默认0.001）
- `--device`: 训练设备（cuda 或 cpu）

### 策略回测

#### 批量回测所有币种

```bash
python backtest_all_coins.py --short_window 5 --long_window 20
```

参数说明：
- `--short_window`: 短期均线窗口（默认5）
- `--long_window`: 长期均线窗口（默认20）
- `--initial_balance`: 初始资金（默认10000）
- `--buy_ratio`: 买入比例（默认0.5）
- `--sell_ratio`: 卖出比例（默认0.5）

## 文件结构

```
python-okx1/
├── config/
│   └── trading_pairs.json          # 多币种配置文件
├── database/
│   └── migrate_multi_coin.sql      # 数据库迁移脚本
├── myWork/
│   ├── model/
│   │   ├── LSTM.py                # LSTM模型（支持多币种）
│   │   └── prepare_data.py        # 数据预处理（支持多币种）
│   └── process/
│       ├── read.py                # 数据读取（支持按币种过滤）
│       └── 回测.py                # 回测模块（支持多币种）
├── fetch_latest_price.py            # 爬取最新数据（支持多币种）
├── myWork/download_data.py         # 下载历史数据（支持多币种）
├── train_all_coins.py             # 批量训练所有币种
├── fetch_all_coins.py             # 批量爬取所有币种
└── backtest_all_coins.py          # 批量回测所有币种
```

## 数据库表结构

### sorted_history_15m 表

主键已修改为复合主键 `(ts, currency)`，支持多币种数据存储。

| 字段 | 类型 | 说明 |
|------|------|------|
| ts | DATETIME | 时间戳（主键） |
| open | DECIMAL(30,15) | 开盘价 |
| high | DECIMAL(30,15) | 最高价 |
| low | DECIMAL(30,15) | 最低价 |
| close | DECIMAL(30,15) | 收盘价 |
| volume | DECIMAL(30,15) | 交易量 |
| vol_ccy | VARCHAR(50) | 交易币种 |
| vol_ccy_quote | DECIMAL(30,15) | 报价币种交易量 |
| confirm | VARCHAR(10) | K线确认状态 |
| currency | VARCHAR(20) | 货币对（主键） |

## API 使用示例

### 从数据库加载指定币种的数据

```python
from myWork.process.read import load_kline_from_db

# 加载BTC数据
df_btc = load_kline_from_db(currency='BTC-USDT')

# 加载ETH数据
df_eth = load_kline_from_db(currency='ETH-USDT')

# 按时间范围加载数据
from datetime import datetime
df = load_kline_from_db_by_time_range(
    currency='BTC-USDT',
    start_time='2023-01-01',
    end_time='2023-12-31'
)
```

### 为指定币种准备训练数据

```python
from myWork.model.prepare_data import prepare_training_data_from_db

X_train, X_test, y_train, y_test, scaler, df = prepare_training_data_from_db(
    currency='ETH-USDT',
    lookback=60,
    forecast=1,
    split_ratio=0.8
)
```

### 训练指定币种的模型

```python
from myWork.model.LSTM import train_model_for_currency

model, scaler, history = train_model_for_currency(
    currency='ETH-USDT',
    hidden_size=64,
    num_layers=2,
    epochs=50
)
```

### 加载指定币种的模型

```python
from myWork.model.LSTM import load_model_for_currency

model, scaler = load_model_for_currency(
    currency='ETH-USDT',
    input_size=8,
    hidden_size=64,
    num_layers=2,
    output_size=1
)
```

### 对指定币种进行回测

```python
from myWork.process.回测 import backtest_currency

backtest_result, performance = backtest_currency(
    currency='ETH-USDT',
    short_window=5,
    long_window=20,
    initial_balance=10000,
    buy_ratio=0.5,
    sell_ratio=0.5
)
```

## 支持的币种

当前配置文件中包含以下币种（可在 `config/trading_pairs.json` 中修改）：

- BTC-USDT (Bitcoin)
- ETH-USDT (Ethereum)
- BNB-USDT (Binance Coin)
- OKB-USDT (OKEx Blockchain)
- SOL-USDT (Solana)
- XRP-USDT (Ripple)
- ADA-USDT (Cardano)
- DOGE-USDT (Dogecoin)
- AVAX-USDT (Avalanche)
- DOT-USDT (Polkadot)

## 注意事项

1. **数据库迁移**：首次使用多币种功能前，必须执行数据库迁移脚本
2. **配置文件**：所有币种的配置都在 `config/trading_pairs.json` 中管理
3. **模型文件命名**：每个币种的模型文件命名为 `best_lstm_model_{币种}.pth`
4. **标准化器命名**：每个币种的标准化器文件命名为 `scaler_{币种}.pkl`
5. **数据存储**：所有币种的数据存储在同一个数据库表中，通过 `currency` 字段区分

## 故障排查

### 问题：数据库连接失败

检查 `config/trading_pairs.json` 中的数据库配置是否正确。

### 问题：某个币种没有数据

可能原因：
1. 该币种未在OKX交易所上市
2. API请求失败
3. 数据库表结构未正确迁移

### 问题：训练失败

检查：
1. 数据库中是否有足够的历史数据
2. GPU内存是否足够（如果使用CUDA）
3. 模型参数是否合理
