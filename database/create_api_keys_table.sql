-- API密钥管理表创建脚本
-- 用于统一管理各种API密钥，包括Gemini、OpenAI、OKX等

USE okx_data;

-- 创建API密钥管理表
CREATE TABLE IF NOT EXISTS api_keys (
    id INT PRIMARY KEY AUTO_INCREMENT,
    key_type VARCHAR(50) NOT NULL COMMENT '密钥类型，如gemini, openai, okx等',
    key_name VARCHAR(100) NOT NULL COMMENT '密钥名称，用于标识',
    key_value VARCHAR(255) NOT NULL COMMENT '密钥值',
    priority INT NOT NULL DEFAULT 0 COMMENT '优先级，数字越小优先级越高',
    is_enabled TINYINT(1) NOT NULL DEFAULT 1 COMMENT '是否启用，1=启用，0=禁用',
    rpm_limit INT DEFAULT NULL COMMENT '每分钟请求数限制',
    tpm_limit INT DEFAULT NULL COMMENT '每分钟令牌数限制',
    rpd_limit INT DEFAULT NULL COMMENT '每天请求数限制',
    daily_usage INT DEFAULT 0 COMMENT '今日使用次数',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    last_used_at TIMESTAMP NULL DEFAULT NULL COMMENT '最后使用时间',
    description TEXT COMMENT '密钥描述',
    UNIQUE KEY uk_key_type_name (key_type, key_name),
    INDEX idx_key_type (key_type),
    INDEX idx_priority (priority),
    INDEX idx_enabled (is_enabled),
    INDEX idx_last_used (last_used_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='API密钥管理表';

-- 验证表创建结果
SELECT 'API keys table created successfully!' AS status;
