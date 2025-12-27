# Python OKX 项目

这是一个基于OKX交易所API的加密货币交易策略项目，包含数据爬取、DCA（定投）策略、AI分析等功能。

## 项目结构

```
python-okx1/
├── myWork/                    # 主要工作目录
│   ├── ai/                    # AI分析模块
│   ├── dca/                   # DCA定投策略模块
│   ├── model/                 # 机器学习模型
│   └── process/               # 数据处理流程
├── config/                    # 配置文件
├── database/                  # 数据库迁移脚本
└── fetch_latest_price.py      # 爬取最新价格数据
```

## 主要功能

### 1. 爬取最新价格数据

**文件**: `fetch_latest_price.py`

**功能说明**:
- 从OKX API获取BTC-USDT的15分钟K线数据
- 自动检测数据库中最新的时间戳，只获取新数据
- 支持增量更新，避免重复数据
- 数据保存到MySQL数据库的 `sorted_history_15m` 表

**使用方法**:

本地执行:
```bash
python fetch_latest_price.py
```

远程服务器执行:
```bash
cd /root/python-okx/python-okx
bash -c "source /root/okx-env-3.8/bin/activate && python fetch_latest_price.py"
```

**配置参数**:
- API_URL: https://www.okx.com
- INST_ID: BTC-USDT (交易对)
- BAR: 15m (时间周期)
- LIMIT: 100 (每次请求的数据条数)

**数据库配置**:
- host: localhost
- user: root
- database: okx_data
- 表名: sorted_history_15m

### 2. DCA定投策略

**目录**: `myWork/dca/`

**主要文件**:
- `dca_strategy.py` - DCA策略实现
- `dca_backtest.py` - DCA回测功能
- `database_manager.py` - 数据库管理
- `trade.py` - 交易执行

### 3. AI分析

**目录**: `myWork/ai/`

**主要文件**:
- `ai_analysis.py` - AI分析主程序
- `CoinGeckoAPI.py` - CoinGecko API接口
- `technical_indicators.py` - 技术指标计算

### 4. 机器学习模型

**目录**: `myWork/model/`

**主要文件**:
- `LSTM.py` - LSTM模型实现
- `prepare_data.py` - 数据准备

## 数据库表结构

### sorted_history_15m 表

存储15分钟K线数据:

| 字段 | 类型 | 说明 |
|------|------|------|
| ts | DATETIME | 时间戳 (主键) |
| open | DECIMAL(30,15) | 开盘价 |
| high | DECIMAL(30,15) | 最高价 |
| low | DECIMAL(30,15) | 最低价 |
| close | DECIMAL(30,15) | 收盘价 |
| volume | DECIMAL(30,15) | 成交量 |
| vol_ccy | VARCHAR(50) | 成交货币 |
| vol_ccy_quote | DECIMAL(30,15) | 成交额 |
| confirm | VARCHAR(10) | 确认状态 |
| currency | VARCHAR(20) | 交易对 |

### dca_strategy_state 表

存储DCA策略状态

### dca_trades 表

存储DCA交易记录

### api_keys 表

存储API密钥管理信息：

| 字段 | 类型 | 说明 |
|------|------|------|
| id | INT | 主键 |
| key_type | VARCHAR(50) | 密钥类型（如gemini、openai、okx等） |
| key_name | VARCHAR(100) | 密钥名称 |
| key_value | VARCHAR(255) | 密钥值 |
| priority | INT | 优先级（数字越小优先级越高） |
| is_enabled | TINYINT(1) | 是否启用（1=启用，0=禁用） |
| rpm_limit | INT | 每分钟请求数限制 |
| tpm_limit | INT | 每分钟令牌数限制 |
| rpd_limit | INT | 每天请求数限制 |
| daily_usage | INT | 今日使用次数 |
| description | TEXT | 密钥描述 |

## API密钥管理

### 数据库表创建

执行SQL脚本创建API密钥管理表：

```bash
# 连接到远程数据库后执行
mysql -h 43.163.118.163 -u root -p okx_data < database/create_api_keys_table.sql
mysql -h 43.163.118.163 -u root -p okx_data < database/init_api_keys.sql
```

### 环境变量配置

在运行AI分析功能前，需要配置以下环境变量：

```bash
# Gemini API密钥
export GEMINI_API_KEY_1="your-gemini-api-key-1"
export GEMINI_API_KEY_2="your-gemini-api-key-2"

# OpenAI API密钥（可选）
export OPENAI_API_KEY="your-openai-api-key"
```

### 密钥加载优先级

1. 优先从数据库加载启用的API密钥
2. 如果数据库连接失败或无可用密钥，从环境变量加载
3. 如果环境变量也未配置，使用内置的备用密钥（仅用于开发测试）

## 远程服务器信息

- **IP地址**: YOUR_SERVER_IP (请替换为实际IP)
- **工作目录**: /root/python-okx/python-okx
- **Python虚拟环境**: /root/okx-env-3.8
- **数据库密码**: YOUR_DB_PASSWORD (请替换为实际密码)
- **SSH密钥**: /Volumes/600g/app1/okx_api.pem (请妥善保管密钥文件)

## GitHub仓库

- **地址**: https://github.com/likecu/python-okx
- **当前分支**: dev_2505

## 依赖安装

```bash
pip install -r requirements.txt
```

## 注意事项

1. 远程服务器使用SSH密钥登录: `/Volumes/600g/app1/okx_api.pem` (请妥善保管密钥文件)
2. 执行Python脚本前需要激活虚拟环境
3. 数据库操作使用pymysql库
4. API请求有频率限制，注意控制请求速率
