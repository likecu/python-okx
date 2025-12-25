-- Gemini AI 模型配置表迁移脚本
-- 用于管理 Gemini API 模型配置和优先级

USE okx_data;

-- 创建 Gemini 模型配置表
CREATE TABLE IF NOT EXISTS gemini_models (
    id INT PRIMARY KEY AUTO_INCREMENT,
    model_name VARCHAR(100) NOT NULL UNIQUE COMMENT '模型名称',
    model_type ENUM('text_only', 'image_supported', 'document_supported') NOT NULL DEFAULT 'text_only' COMMENT '模型类型',
    priority INT NOT NULL DEFAULT 0 COMMENT '优先级，数字越小优先级越高',
    is_enabled TINYINT(1) NOT NULL DEFAULT 1 COMMENT '是否启用，1=启用，0=禁用',
    rpm_limit INT DEFAULT NULL COMMENT '每分钟请求数限制',
    tpm_limit INT DEFAULT NULL COMMENT '每分钟令牌数限制',
    rpd_limit INT DEFAULT NULL COMMENT '每天请求数限制',
    description TEXT COMMENT '模型描述',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_priority (priority),
    INDEX idx_enabled (is_enabled),
    INDEX idx_type (model_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Gemini AI 模型配置表';

-- 插入默认模型配置
INSERT INTO gemini_models (model_name, model_type, priority, is_enabled, rpm_limit, tpm_limit, rpd_limit, description) VALUES
('gemini-2.5-flash', 'text_only', 1, 1, 5, 250000, 20, 'Gemini 2.5 Flash 模型，快速响应，适合文本生成'),
('gemini-2.5-flash-lite', 'text_only', 2, 1, 10, 100000, 40, 'Gemini 2.5 Flash Lite 模型，轻量级，适合简单文本任务'),
('gemini-1.5-flash', 'image_supported', 3, 1, 5, 250000, 20, 'Gemini 1.5 Flash 模型，支持图像理解'),
('gemini-1.5-pro', 'image_supported', 4, 1, 5, 10000, 10, 'Gemini 1.5 Pro 模型，高质量图像理解'),
('gemini-2.5-pro', 'text_only', 5, 1, 3, 100000, 10, 'Gemini 2.5 Pro 模型，专业级文本生成'),
('gemma-3-27b-it', 'text_only', 6, 1, 5, 100000, 15, 'Gemma 3 27B 模型，高质量文本生成'),
('gemma-3-12b-it', 'text_only', 7, 1, 10, 50000, 20, 'Gemma 3 12B 模型，平衡性能和速度'),
('gemma-3-2b-it', 'text_only', 8, 1, 20, 100000, 40, 'Gemma 3 2B 模型，快速响应'),
('gemma-3-9b-it', 'text_only', 9, 1, 15, 75000, 30, 'Gemma 3 9B 模型，中等规模'),
('gemini-2-27b-it', 'text_only', 10, 1, 10, 50000, 20, 'Gemma 2 27B 模型'),
('gemini-2-9b-it', 'text_only', 11, 1, 20, 100000, 40, 'Gemma 2 9B 模型'),
('gemini-1.1-7b-it', 'text_only', 12, 1, 20, 100000, 40, 'Gemma 1.1 7B 模型'),
('gemini-1-7b-it', 'text_only', 13, 1, 20, 100000, 40, 'Gemma 1 7B 模型'),
('gemini-2-2b-it', 'text_only', 14, 1, 50, 200000, 100, 'Gemma 2 2B 模型，轻量级'),
('gemini-1.1-2b-it', 'text_only', 15, 1, 50, 200000, 100, 'Gemma 1.1 2B 模型'),
('gemini-1-2b-it', 'text_only', 16, 1, 50, 200000, 100, 'Gemma 1 2B 模型'),
('gemini-nano', 'text_only', 17, 1, 50, 200000, 100, 'Gemini Nano 模型，移动端优化'),
('gemini-ultra', 'image_supported', 18, 1, 5, 10000, 10, 'Gemini Ultra 模型，最高质量'),
('gemini-experimental', 'text_only', 19, 1, 2, 5000, 5, 'Gemini 实验性模型')
ON DUPLICATE KEY UPDATE 
    priority = VALUES(priority),
    is_enabled = VALUES(is_enabled),
    rpm_limit = VALUES(rpm_limit),
    tpm_limit = VALUES(tpm_limit),
    rpd_limit = VALUES(rpd_limit),
    description = VALUES(description),
    updated_at = CURRENT_TIMESTAMP;

-- 验证插入结果
SELECT 'Gemini models table created and populated successfully!' AS status;
SELECT COUNT(*) AS total_models FROM gemini_models;
SELECT model_name, model_type, priority, is_enabled FROM gemini_models ORDER BY priority ASC;
