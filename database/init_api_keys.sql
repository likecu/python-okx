-- API密钥初始化脚本
-- 用于插入初始API密钥数据，包括现有Gemini密钥

USE okx_data;

-- 插入Gemini API密钥
INSERT INTO api_keys (key_type, key_name, key_value, priority, is_enabled, rpm_limit, tpm_limit, rpd_limit, description)
VALUES 
    ('gemini', 'gemini-key-1', 'AIzaSyATTaWd3cGkhFoFbtBQUCL4ez5r1vVhJxI', 1, 1, 5, 250000, 20, 'Gemini API密钥1，主要密钥'),
    ('gemini', 'gemini-key-2', 'AIzaSyBHZwljuV3ojIl7abOcemAeKX6LNZuzhOw', 2, 1, 5, 250000, 20, 'Gemini API密钥2，备用密钥')
ON DUPLICATE KEY UPDATE
    key_value = VALUES(key_value),
    priority = VALUES(priority),
    is_enabled = VALUES(is_enabled),
    rpm_limit = VALUES(rpm_limit),
    tpm_limit = VALUES(tpm_limit),
    rpd_limit = VALUES(rpd_limit),
    description = VALUES(description),
    updated_at = CURRENT_TIMESTAMP;

-- 可选：插入OpenAI密钥（从.env文件获取）
INSERT INTO api_keys (key_type, key_name, key_value, priority, is_enabled, description)
VALUES 
    ('openai', 'openai-key-1', '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', 1, 1, 'OpenAI API密钥')
ON DUPLICATE KEY UPDATE
    key_value = VALUES(key_value),
    priority = VALUES(priority),
    is_enabled = VALUES(is_enabled),
    description = VALUES(description),
    updated_at = CURRENT_TIMESTAMP;

-- 验证插入结果
SELECT 'API keys initialized successfully!' AS status;
SELECT key_type, key_name, priority, is_enabled FROM api_keys ORDER BY key_type, priority ASC;
