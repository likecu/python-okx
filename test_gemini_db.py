#!/usr/bin/env python3.8
import sys
sys.path.insert(0, '/root/python-okx/python-okx')

from myWork.dca.database_manager import DatabaseManager
from myWork.ai.ai_analysis import GeminiAIAnalyzer

print("创建数据库管理器...")
db_manager = DatabaseManager(host='127.0.0.1', user='root', password='!A33b3e561fec', database='okx_data')

print("创建 Gemini AI 分析器（带数据库管理）...")
analyzer = GeminiAIAnalyzer(db_manager=db_manager)

print(f"\n当前使用模型: {analyzer.model_name}")
print(f"可用模型数量: {len(analyzer.model_manager.get_available_models())}")

print("\n测试简单提问...")
result = analyzer.ask_question("请用一句话介绍一下你自己")

if result and result.get("success"):
    print(f"\n✓ 测试成功！")
    print(f"模型: {result['model']}")
    print(f"回答: {result['response']}")
else:
    print(f"\n✗ 测试失败")
    print(f"结果: {result}")
