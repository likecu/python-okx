-- 多币种数据库迁移脚本
-- 修改表结构以支持多币种数据

USE okx_data;

-- 1. 修改 sorted_history_15m 表的主键为复合主键 (ts, currency)
-- 删除旧的主键
ALTER TABLE sorted_history_15m DROP PRIMARY KEY;

-- 添加新的复合主键
ALTER TABLE sorted_history_15m ADD PRIMARY KEY (ts, currency);

-- 确保 currency 字段有索引
ALTER TABLE sorted_history_15m ADD INDEX idx_currency (currency);

-- 2. 为 dca_strategy_state 表添加 currency 字段
ALTER TABLE dca_strategy_state ADD COLUMN currency VARCHAR(20) DEFAULT 'BTC-USDT' AFTER strategy_name;

-- 为 dca_strategy_state 表添加 currency 索引
ALTER TABLE dca_strategy_state ADD INDEX idx_currency (currency);

-- 3. 为 dca_trades 表添加 currency 字段
ALTER TABLE dca_trades ADD COLUMN currency VARCHAR(20) DEFAULT 'BTC-USDT' AFTER strategy_id;

-- 为 dca_trades 表添加 currency 索引
ALTER TABLE dca_trades ADD INDEX idx_currency (currency);

-- 4. 为 trade_records 表添加 currency 字段（如果存在）
-- ALTER TABLE trade_records ADD COLUMN currency VARCHAR(20) DEFAULT 'BTC-USDT' AFTER instId;

-- 验证修改
SELECT 'Migration completed successfully!' AS status;
