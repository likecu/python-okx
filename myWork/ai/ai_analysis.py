import os
from datetime import datetime
import json
import random
from typing import Dict, Any
import requests

try:
    from .gemini_config import GEMINI_API_KEYS
except ImportError:
    from gemini_config import GEMINI_API_KEYS


class GeminiAIAnalyzer:
    def __init__(self):
        """
        初始化 Gemini AI 分析器
        """
        self.current_key_index = random.randint(0, len(GEMINI_API_KEYS) - 1)
        self.api_key = GEMINI_API_KEYS[self.current_key_index]
        
        self.model_priority = [
            "gemini-2.5-flash",
            "gemini-2.5-flash-lite",
            "gemini-1.5-flash",
            "gemini-1.5-pro",
            "gemini-2.5-pro",
            "gemini-ultra",
            "gemini-experimental"
        ]
        
        self.model_name = self.select_best_model()
        print(f"使用模型: {self.model_name}")
    
    def select_best_model(self):
        """
        根据优先级选择最优模型
        :return: 最优模型名称
        """
        for model_name in self.model_priority:
            return model_name
        return "gemini-2.5-flash"
    
    def ask_question(self, question):
        """
        直接向 Gemini 提问
        :param question: 提问内容
        :return: 提问结果对象
        """
        print(f"开始提问: {question}")
        print(f"当前使用模型: {self.model_name}")
        
        max_attempts = 3
        current_attempt = 0
        
        while current_attempt < max_attempts:
            try:
                url = f"https://generativelanguage.googleapis.com/v1beta/models/{self.model_name}:generateContent?key={self.api_key}"
                
                headers = {
                    "Content-Type": "application/json"
                }
                
                data = {
                    "contents": [
                        {
                            "parts": [
                                {"text": question}
                            ]
                        }
                    ],
                    "generationConfig": {
                        "temperature": 0.7,
                        "topK": 40,
                        "topP": 0.95,
                        "maxOutputTokens": 8192
                    }
                }
                
                response = requests.post(url, headers=headers, json=data, timeout=60)
                
                if response.status_code == 200:
                    result = response.json()
                    if "candidates" in result and len(result["candidates"]) > 0:
                        text = result["candidates"][0]["content"]["parts"][0]["text"]
                        return {
                            "success": True,
                            "response": text,
                            "model": self.model_name
                        }
                    else:
                        print(f"API 返回格式异常: {result}")
                        return None
                else:
                    error_msg = response.text
                    print(f"API 请求失败: {response.status_code}, {error_msg}")
                    
                    if "quota exceeded" in error_msg.lower() or "429" in error_msg or response.status_code == 429:
                        raise Exception("quota exceeded")
                    else:
                        return None
                
            except Exception as e:
                error_msg = str(e)
                print(f"调用 Gemini API 时发生错误: {error_msg}")
                
                if "quota exceeded" in error_msg.lower() or "429" in error_msg:
                    print(f"模型 {self.model_name} 配额已用完，尝试切换模型或 API 密钥...")
                    current_attempt += 1
                    
                    if len(GEMINI_API_KEYS) > 1:
                        self.current_key_index = (self.current_key_index + 1) % len(GEMINI_API_KEYS)
                        self.api_key = GEMINI_API_KEYS[self.current_key_index]
                        print(f"已切换到新 API 密钥: {self.api_key[:10]}...")
                    
                    if self.model_name in self.model_priority:
                        self.model_priority.remove(self.model_name)
                    
                    new_model = self.select_best_model()
                    if new_model != self.model_name:
                        self.model_name = new_model
                        print(f"已切换到新模型: {self.model_name}")
                    else:
                        print("没有可用的替代模型")
                        break
                else:
                    return None
        
        print(f"尝试了 {current_attempt} 次后仍无法完成提问")
        return None
    
    def generate_analysis(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        生成 AI 分析结果
        :param data: 包含价格数据、技术指标等的数据字典
        :return: 分析结果字典
        """
        system_prompt = """
        你是一位精通加密货币市场的量化分析师。请基于以下数据，对 BTC 未来 7 天的价格走势进行全面分析：

        1. 技术面分析：MA5/MA20/MA50/MA200 趋势、RSI 指标、MACD 指标
        2. 链上数据：交易所余额变化、巨鲸活动、活跃地址数
        3. 市场情绪：恐惧与贪婪指数
        4. 宏观经济：美元指数、美联储政策预期

        请提供结构化分析，包括：
        - 未来 7 天价格区间预测（精确到 $100）
        - 关键支撑位和阻力位
        - 上涨/下跌概率（百分比）
        - 主要驱动因素（至少 3 条）
        - 重大风险提示（至少 3 条）
        - 明确的交易建议

        请以 JSON 格式返回结果，包含以下字段：
        "price_range", "support_level", "resistance_level", 
        "bullish_probability", "bearish_probability", 
        "driving_factors", "risks", "trading_advice", "analysis_date"
        """
        
        user_prompt = f"""
        当前 BTC 数据：

        1. 价格数据：
           - 当前价格：${data['price_data']['price']:.2f}
           - 24h 变化：{data['price_data']['change_24h']:.2f}%
           - 市值：${data['price_data']['market_cap']:.0f}
           - 24h 交易量：${data['price_data']['volume_24h']:.0f}

        2. 技术指标：
           - MA5：${data['technical_indicators']['MA5']:.2f}
           - MA20：${data['technical_indicators']['MA20']:.2f}
           - MA50：${data['technical_indicators']['MA50']:.2f}
           - MA200：${data['technical_indicators']['MA200']:.2f}
           - RSI：{data['technical_indicators']['RSI']:.2f}
           - MACD：{data['technical_indicators']['MACD']:.2f}

        请基于以上数据，按照指定格式进行分析。
        """
        
        full_prompt = f"{system_prompt}\n\n{user_prompt}"
        
        result = self.ask_question(full_prompt)
        
        if result and result.get("success"):
            try:
                message_content = result["response"].replace("```json", "").replace("```", "")
                analysis_data = json.loads(message_content)
                analysis_data["analysis_date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                return analysis_data
            except json.JSONDecodeError as e:
                print("JSON 解析错误:", e)
                return self._get_error_response(str(e))
        else:
            return self._get_error_response("API 调用失败")
    
    def _get_error_response(self, error_msg: str) -> Dict[str, Any]:
        """
        返回错误响应
        :param error_msg: 错误消息
        :return: 错误响应字典
        """
        return {
            "error": error_msg,
            "price_range": "无法生成预测",
            "support_level": "无法生成预测",
            "resistance_level": "无法生成预测",
            "bullish_probability": "0%",
            "bearish_probability": "0%",
            "driving_factors": ["AI分析失败", "请检查API密钥", "请检查网络连接"],
            "risks": ["AI分析失败风险", "数据获取失败风险", "模型响应异常风险"],
            "trading_advice": "谨慎操作，等待系统恢复正常",
            "analysis_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }


AIAnalyzer = GeminiAIAnalyzer
