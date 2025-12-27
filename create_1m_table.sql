-- 1分钟级别行情数据表创建脚本
-- 用于存储OKX交易所1分钟级别的多币种行情数据
-- 注意：OKX API不支持15秒级别数据，1分钟是最接近的时间级别

USE okx_data;

-- 创建1分钟级别历史数据表
CREATE TABLE IF NOT EXISTS sorted_history_1m (
    ts DATETIME NOT NULL COMMENT '时间戳',
    open DECIMAL(30,15) COMMENT '开盘价',
    high DECIMAL(30,15) COMMENT '最高价',
    low DECIMAL(30,15) COMMENT '最低价',
    close DECIMAL(30,15) COMMENT '收盘价',
    volume DECIMAL(30,15) COMMENT '成交量',
    vol_ccy VARCHAR(50) COMMENT '成交量货币单位',
    vol_ccy_quote DECIMAL(30,15) COMMENT '报价货币成交量',
    confirm VARCHAR(10) COMMENT '确认字段',
    currency VARCHAR(20) NOT NULL COMMENT '交易对货币',
    
    -- 复合主键：(时间戳, 货币)
    PRIMARY KEY (ts, currency),
    
    -- 货币索引，用于多币种查询
    INDEX idx_currency (currency),
    
    -- 时间戳索引，用于时间范围查询
    INDEX idx_ts (ts)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci 
  COMMENT='OKX交易所1分钟级别多币种行情数据表（最接近15秒的可用时间级别）';

-- 创建表完成提示
SELECT 'Table sorted_history_1m created successfully!' AS status;

-- 显示表结构验证
DESCRIBE sorted_history_1m;

-- 显示索引信息
SHOW INDEX FROM sorted_history_1m;