#!/usr/bin/env python3.8
import sys
import os

# 添加项目根目录到Python路径
sys.path.insert(0, '/Users/aaa/PycharmProjects/python-okx1')

from myWork.dca.database_manager import DatabaseManager
from myWork.ai.ai_analysis import GeminiAIAnalyzer

print("测试数据库Gemini集成...")

# 使用远程数据库配置（请使用环境变量配置）
db_manager = DatabaseManager(
    host=os.environ.get('DB_HOST', 'YOUR_REMOTE_HOST'),
    user='root',
    password=os.environ.get('DB_PASSWORD', 'YOUR_DB_PASSWORD'),
    database='okx_data'
)

print("创建数据库管理器成功！")

print("创建 Gemini AI 分析器（带数据库管理）...")
analyzer = GeminiAIAnalyzer(db_manager=db_manager)

print(f"\n当前使用模型: {analyzer.model_name}")
print(f"可用模型数量: {len(analyzer.model_manager.get_available_models())}")

print(f"\n当前使用的API密钥: {analyzer.api_key[:10]}...")
print(f"可用API密钥数量: {len(analyzer.model_manager.api_keys)}")

print("\n测试简单提问...")
result = analyzer.ask_question("请用一句话介绍一下你自己")

if result and result.get("success"):
    print(f"\n✓ 测试成功！")
    print(f"模型: {result['model']}")
    print(f"回答: {result['response'][:100]}...")
else:
    print(f"\n✗ 测试失败")
    print(f"结果: {result}")

print("\n测试完成！")
